package worker

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AdminObservability collects lightweight admin service metrics.
type AdminObservability struct {
	mu      sync.RWMutex
	started time.Time
	http    map[string]*adminMethodMetric
	grpc    map[string]*adminMethodMetric
	jobs    adminJobRunMetric
}

type adminMethodMetric struct {
	calls     int64
	errors    int64
	totalMs   int64
	maxMs     int64
	lastCode  string
	updatedAt time.Time
}

type adminJobRunMetric struct {
	running   int64
	completed int64
	failed    int64
	totalMs   int64
	maxMs     int64
	lastCode  string
	updatedAt time.Time
}

// AdminObservabilitySnapshot is a serializable view of admin metrics.
type AdminObservabilitySnapshot struct {
	StartedAt time.Time                               `json:"startedAt"`
	UptimeSec int64                                   `json:"uptimeSec"`
	HTTP      map[string]AdminObservabilityMethodStat `json:"http"`
	GRPC      map[string]AdminObservabilityMethodStat `json:"grpc"`
	Jobs      AdminObservabilityJobStat               `json:"jobs"`
}

// AdminObservabilityMethodStat describes aggregate metrics for an HTTP/gRPC method.
type AdminObservabilityMethodStat struct {
	Calls       int64     `json:"calls"`
	Errors      int64     `json:"errors"`
	TotalMs     int64     `json:"totalMs"`
	MaxMs       int64     `json:"maxMs"`
	AvgMs       float64   `json:"avgMs"`
	LastCode    string    `json:"lastCode"`
	LastUpdated time.Time `json:"lastUpdated"`
}

// AdminObservabilityJobStat describes aggregate job-runner outcomes.
type AdminObservabilityJobStat struct {
	Running     int64     `json:"running"`
	Completed   int64     `json:"completed"`
	Failed      int64     `json:"failed"`
	TotalMs     int64     `json:"totalMs"`
	MaxMs       int64     `json:"maxMs"`
	AvgMs       float64   `json:"avgMs"`
	LastCode    string    `json:"lastCode"`
	LastUpdated time.Time `json:"lastUpdated"`
}

// NewAdminObservability creates an in-memory admin metrics collector.
func NewAdminObservability() *AdminObservability {
	return &AdminObservability{
		started: time.Now().UTC(),
		http:    make(map[string]*adminMethodMetric),
		grpc:    make(map[string]*adminMethodMetric),
	}
}

// Middleware records per-route HTTP latency and status metrics.
func (collector *AdminObservability) Middleware(next http.Handler) http.Handler {
	if collector == nil || next == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &adminResponseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(recorder, r)
		collector.RecordHTTP(r.Method, r.URL.Path, recorder.statusCode, time.Since(startedAt))
	})
}

// UnaryServerInterceptor records gRPC latency and status metrics.
func (collector *AdminObservability) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		startedAt := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		collector.RecordGRPC(info.FullMethod, code, time.Since(startedAt))

		return resp, err
	}
}

// RecordHTTP records HTTP route metrics.
func (collector *AdminObservability) RecordHTTP(method, route string, statusCode int, duration time.Duration) {
	if collector == nil {
		return
	}

	key := method + " " + route
	collector.recordMethodMetric(collector.http, key, statusCode >= http.StatusBadRequest, http.StatusText(statusCode), duration)
}

// RecordGRPC records gRPC method metrics.
func (collector *AdminObservability) RecordGRPC(method string, code codes.Code, duration time.Duration) {
	if collector == nil {
		return
	}

	collector.recordMethodMetric(
		collector.grpc,
		method,
		code != codes.OK,
		code.String(),
		duration,
	)
}

// RecordJobQueued increments running gauge when a job run starts.
func (collector *AdminObservability) RecordJobQueued() {
	if collector == nil {
		return
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	collector.jobs.running++
	collector.jobs.updatedAt = time.Now().UTC()
}

// RecordJobOutcome records a final job status and duration.
func (collector *AdminObservability) RecordJobOutcome(statusCode string, duration time.Duration) {
	if collector == nil {
		return
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()

	if collector.jobs.running > 0 {
		collector.jobs.running--
	}

	statusCode = normalizeMetricCode(statusCode)
	switch statusCode {
	case "completed":
		collector.jobs.completed++
	default:
		collector.jobs.failed++
	}

	ms := durationToMillis(duration)

	collector.jobs.totalMs += ms
	if ms > collector.jobs.maxMs {
		collector.jobs.maxMs = ms
	}

	collector.jobs.lastCode = statusCode
	collector.jobs.updatedAt = time.Now().UTC()
}

// Snapshot returns a consistent view of collected metrics.
func (collector *AdminObservability) Snapshot() AdminObservabilitySnapshot {
	if collector == nil {
		return AdminObservabilitySnapshot{}
	}

	collector.mu.RLock()
	defer collector.mu.RUnlock()

	now := time.Now().UTC()
	snapshot := AdminObservabilitySnapshot{
		StartedAt: collector.started,
		UptimeSec: int64(now.Sub(collector.started).Seconds()),
		HTTP:      make(map[string]AdminObservabilityMethodStat, len(collector.http)),
		GRPC:      make(map[string]AdminObservabilityMethodStat, len(collector.grpc)),
		Jobs:      toJobSnapshot(collector.jobs),
	}

	for key, metric := range collector.http {
		snapshot.HTTP[key] = toMethodSnapshot(metric)
	}

	for key, metric := range collector.grpc {
		snapshot.GRPC[key] = toMethodSnapshot(metric)
	}

	return snapshot
}

func (collector *AdminObservability) recordMethodMetric(
	target map[string]*adminMethodMetric,
	key string,
	isError bool,
	lastCode string,
	duration time.Duration,
) {
	collector.mu.Lock()
	defer collector.mu.Unlock()

	metric, ok := target[key]
	if !ok {
		metric = &adminMethodMetric{}
		target[key] = metric
	}

	metric.calls++
	if isError {
		metric.errors++
	}

	ms := durationToMillis(duration)

	metric.totalMs += ms
	if ms > metric.maxMs {
		metric.maxMs = ms
	}

	metric.lastCode = normalizeMetricCode(lastCode)
	metric.updatedAt = time.Now().UTC()
}

func toMethodSnapshot(metric *adminMethodMetric) AdminObservabilityMethodStat {
	if metric == nil {
		return AdminObservabilityMethodStat{}
	}

	avgMs := float64(0)
	if metric.calls > 0 {
		avgMs = float64(metric.totalMs) / float64(metric.calls)
	}

	return AdminObservabilityMethodStat{
		Calls:       metric.calls,
		Errors:      metric.errors,
		TotalMs:     metric.totalMs,
		MaxMs:       metric.maxMs,
		AvgMs:       avgMs,
		LastCode:    metric.lastCode,
		LastUpdated: metric.updatedAt,
	}
}

func toJobSnapshot(metric adminJobRunMetric) AdminObservabilityJobStat {
	total := metric.completed + metric.failed

	avgMs := float64(0)
	if total > 0 {
		avgMs = float64(metric.totalMs) / float64(total)
	}

	return AdminObservabilityJobStat{
		Running:     metric.running,
		Completed:   metric.completed,
		Failed:      metric.failed,
		TotalMs:     metric.totalMs,
		MaxMs:       metric.maxMs,
		AvgMs:       avgMs,
		LastCode:    metric.lastCode,
		LastUpdated: metric.updatedAt,
	}
}

func durationToMillis(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}

	return duration.Milliseconds()
}

func normalizeMetricCode(code string) string {
	code = strings.TrimSpace(strings.ToLower(code))
	if code == "" {
		return "unknown"
	}

	return code
}

type adminResponseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (recorder *adminResponseRecorder) WriteHeader(statusCode int) {
	recorder.statusCode = statusCode
	recorder.ResponseWriter.WriteHeader(statusCode)
}
