package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	sectconv "github.com/hyp3rd/sectools/pkg/converters"
	sectools "github.com/hyp3rd/sectools/pkg/io"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	adminDefaultGRPCAddr  = "127.0.0.1:50052"
	adminDefaultHTTPAddr  = "127.0.0.1:8081"
	adminDefaultReadTime  = 5 * time.Second
	adminDefaultWriteTime = 10 * time.Second
	adminBodyLimitBytes   = 1024 * 1024
	adminMaxInt32         = 2147483647
	adminErrMethodBlocked = "method_not_allowed"
	adminErrMethodMessage = "Method not allowed"
	adminErrInvalidJSON   = "invalid_json"
	adminErrReadBodyMsg   = "failed to read request body"
	adminErrParseBodyMsg  = "failed to parse request body"
	adminErrInvalidBody   = "invalid_body"
	adminErrArtifactDown  = "artifact_unavailable"
	adminErrMissingQueue  = "missing_queue"
	adminErrQueueRequired = "Queue name is required"
	adminErrMissingJob    = "missing_job"
	adminErrJobRequired   = "Job name is required"
	adminErrInvalidJob    = "invalid_job"
	adminErrInvalidJobMsg = "Invalid job name"
	adminRequestTimeout   = 5 * time.Second
	adminRequestIDHeader  = "X-Request-Id"
	adminRequestIDMetaKey = "x-request-id"
	adminApprovalHeader   = "X-Admin-Approval"
	adminHeaderType       = "Content-Type"
	adminPathSeparator    = "/"
	adminQueryLimit       = "limit"
	adminScheduleLag      = 2 * time.Minute
	adminEventsInterval   = 10 * time.Second
	adminEventsMaxCount   = 25
	adminEventsReplaySize = 300
	adminExportFmtJSON    = "json"
	adminExportFmtCSV     = "csv"
	adminExportFmtJSONL   = "jsonl"
	adminParseIntBitSize  = 64
	adminTarballMimeType  = "application/gzip"
	adminErrNoArtifact    = "This job source has no downloadable tarball"
	adminAuditExportMax   = 5000
	adminMetricsFmtJSON   = "json"
	adminMetricsFmtProm   = "prometheus"
	adminRequestIDMaxLen  = 128
	adminASCIIControlMin  = 0x20
	adminASCIIDelete      = 0x7f
)

// AdminGatewayConfig configures the admin HTTP gateway.
type AdminGatewayConfig struct {
	GRPCAddr string
	HTTPAddr string

	// GRPCTLS is optional; when nil, the gateway dials gRPC without TLS.
	GRPCTLS *tls.Config

	// TLSCertFile, TLSKeyFile, and TLSCAFile are required for mTLS.
	TLSCertFile    string
	TLSKeyFile     string
	TLSCAFile      string
	JobTarballDir  string
	AuditExportMax int

	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	Observability *AdminObservability
}

// LoadAdminMTLSConfig loads a server TLS config that enforces mTLS.
func LoadAdminMTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, ewrap.New("admin gateway TLS cert, key, and CA are required")
	}

	allowedRoots := uniqueRoots(
		filepath.Dir(certFile),
		filepath.Dir(keyFile),
		filepath.Dir(caFile),
	)
	if len(allowedRoots) == 0 {
		return nil, ewrap.New("admin gateway TLS roots are required")
	}

	ioClient, err := sectools.NewWithOptions(
		sectools.WithAllowAbsolute(true),
		sectools.WithAllowedRoots(allowedRoots...),
	)
	if err != nil {
		return nil, ewrap.Wrap(err, "init admin gateway io")
	}

	certBytes, err := ioClient.ReadFile(certFile)
	if err != nil {
		return nil, ewrap.Wrap(err, "read admin gateway TLS cert")
	}

	keyBytes, err := ioClient.ReadFile(keyFile)
	if err != nil {
		return nil, ewrap.Wrap(err, "read admin gateway TLS key")
	}

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, ewrap.Wrap(err, "load admin gateway TLS cert")
	}

	caBytes, err := ioClient.ReadFile(caFile)
	if err != nil {
		return nil, ewrap.Wrap(err, "read admin gateway CA")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, ewrap.New("failed to parse admin gateway CA")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}

// NewAdminGatewayServer builds an HTTPS gateway that proxies to AdminService.
func NewAdminGatewayServer(cfg AdminGatewayConfig) (*http.Server, error) {
	grpcAddr, httpAddr := adminGatewayAddresses(cfg)

	tlsConfig, err := LoadAdminMTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
	if err != nil {
		return nil, err
	}

	conn, err := newAdminGatewayConnection(grpcAddr, cfg)
	if err != nil {
		return nil, err
	}

	observability := adminGatewayObservability(cfg.Observability)
	handler := newAdminGatewayHandler(
		workerpb.NewAdminServiceClient(conn),
		observability,
		cfg.JobTarballDir,
		cfg.AuditExportMax,
	)

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      observability.Middleware(newAdminGatewayMux(handler)),
		TLSConfig:    tlsConfig,
		ReadTimeout:  defaultDuration(cfg.ReadTimeout, adminDefaultReadTime),
		WriteTimeout: defaultDuration(cfg.WriteTimeout, adminDefaultWriteTime),
	}
	registerAdminGatewayConnClose(server, conn)

	return server, nil
}

func newAdminGatewayConnection(grpcAddr string, cfg AdminGatewayConfig) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(grpcAddr, adminGatewayDialOptions(cfg)...)
	if err != nil {
		return nil, ewrap.Wrap(err, "dial admin gRPC")
	}

	return conn, nil
}

func adminGatewayObservability(observability *AdminObservability) *AdminObservability {
	if observability != nil {
		return observability
	}

	return NewAdminObservability()
}

func registerAdminGatewayConnClose(server *http.Server, conn *grpc.ClientConn) {
	server.RegisterOnShutdown(func() {
		closeErr := conn.Close()
		if closeErr != nil {
			_ = closeErr
		}
	})
}

func adminGatewayAddresses(cfg AdminGatewayConfig) (grpcAddr, httpAddr string) {
	grpcAddr = cfg.GRPCAddr
	if grpcAddr == "" {
		grpcAddr = adminDefaultGRPCAddr
	}

	httpAddr = cfg.HTTPAddr
	if httpAddr == "" {
		httpAddr = adminDefaultHTTPAddr
	}

	return grpcAddr, httpAddr
}

func adminGatewayDialOptions(cfg AdminGatewayConfig) []grpc.DialOption {
	if cfg.GRPCTLS != nil {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(cfg.GRPCTLS)),
		}
	}

	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func newAdminGatewayHandler(
	client workerpb.AdminServiceClient,
	observability *AdminObservability,
	jobTarballDir string,
	auditExportMax int,
) *adminGatewayHandler {
	limit := auditExportMax
	if limit <= 0 {
		limit = adminAuditExportMax
	}

	return &adminGatewayHandler{
		client:         client,
		eventBuffer:    newAdminEventBuffer(adminEventsReplaySize),
		observability:  observability,
		artifacts:      newAdminArtifactStore(jobTarballDir),
		auditExportMax: limit,
	}
}

func newAdminGatewayMux(handler *adminGatewayHandler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/health", handler.handleHealth)
	mux.HandleFunc("/admin/v1/overview", handler.handleOverview)
	mux.HandleFunc("/admin/v1/queues", handler.handleQueues)
	mux.HandleFunc("/admin/v1/queues/", handler.handleQueue)
	mux.HandleFunc("/admin/v1/jobs/events", handler.handleJobEvents)
	mux.HandleFunc("/admin/v1/audit/export", handler.handleAuditExport)
	mux.HandleFunc("/admin/v1/audit", handler.handleAudit)
	mux.HandleFunc("/admin/v1/metrics", handler.handleMetrics)
	mux.HandleFunc("/admin/v1/metrics/prometheus", handler.handleMetricsPrometheus)
	mux.HandleFunc("/admin/v1/jobs", handler.handleJobs)
	mux.HandleFunc("/admin/v1/jobs/", handler.handleJob)
	mux.HandleFunc("/admin/v1/schedules", handler.handleSchedules)
	mux.HandleFunc("/admin/v1/schedules/factories", handler.handleScheduleFactories)
	mux.HandleFunc("/admin/v1/schedules/events", handler.handleScheduleEvents)
	mux.HandleFunc("/admin/v1/schedules/pause", handler.handleSchedulesPause)
	mux.HandleFunc("/admin/v1/schedules/", handler.handleSchedule)
	mux.HandleFunc("/admin/v1/events", handler.handleEvents)
	mux.HandleFunc("/admin/v1/dlq", handler.handleDLQ)
	mux.HandleFunc("/admin/v1/dlq/", handler.handleDLQEntry)
	mux.HandleFunc("/admin/v1/pause", handler.handlePause)
	mux.HandleFunc("/admin/v1/resume", handler.handleResume)
	mux.HandleFunc("/admin/v1/dlq/replay", handler.handleReplay)
	mux.HandleFunc("/admin/v1/dlq/replay/ids", handler.handleReplayIDs)

	return mux
}

type adminGatewayHandler struct {
	client         workerpb.AdminServiceClient
	eventBuffer    *adminEventBuffer
	observability  *AdminObservability
	artifacts      adminArtifactStore
	auditExportMax int
}

func (h *adminGatewayHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetHealth(ctx, &workerpb.GetHealthRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	payload := adminHealthJSON{
		Status:    resp.GetStatus(),
		Version:   resp.GetVersion(),
		Commit:    resp.GetCommit(),
		BuildTime: resp.GetBuildTime(),
		GoVersion: resp.GetGoVersion(),
	}

	writeAdminJSON(w, http.StatusOK, payload)
}

func (h *adminGatewayHandler) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetOverview(ctx, &workerpb.GetOverviewRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	payload := adminOverviewJSON{
		Stats: overviewStatsJSON{
			ActiveWorkers: resp.GetStats().GetActiveWorkers(),
			QueuedTasks:   resp.GetStats().GetQueuedTasks(),
			Queues:        resp.GetStats().GetQueues(),
			AvgLatencyMs:  resp.GetStats().GetAvgLatencyMs(),
			P95LatencyMs:  resp.GetStats().GetP95LatencyMs(),
		},
		Coordination: coordinationJSON{
			GlobalRateLimit: resp.GetCoordination().GetGlobalRateLimit(),
			LeaderLock:      resp.GetCoordination().GetLeaderLock(),
			Lease:           resp.GetCoordination().GetLease(),
			Paused:          resp.GetCoordination().GetPaused(),
		},
		Actions: adminActionsJSON{
			Pause:  resp.GetActions().GetPause(),
			Resume: resp.GetActions().GetResume(),
			Replay: resp.GetActions().GetReplay(),
		},
	}

	writeAdminJSON(w, http.StatusOK, payload)
}

func (h *adminGatewayHandler) handleQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListQueues(ctx, &workerpb.ListQueuesRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	queues := make([]queueSummaryJSON, 0, len(resp.GetQueues()))
	for _, queue := range resp.GetQueues() {
		queues = append(queues, queueSummaryFromProto(queue))
	}

	writeAdminJSON(w, http.StatusOK, adminQueuesJSON{Queues: queues})
}

func (h *adminGatewayHandler) handleQueue(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/queues/")

	path = strings.Trim(path, adminPathSeparator)
	if path == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingQueue, adminErrQueueRequired, requestID(r, w))

		return
	}

	if before, ok := strings.CutSuffix(path, "/weight"); ok {
		name := strings.TrimSuffix(before, adminPathSeparator)
		h.handleQueueWeight(w, r, name)

		return
	}

	if before, ok := strings.CutSuffix(path, "/pause"); ok {
		name := strings.TrimSuffix(before, adminPathSeparator)
		h.handleQueuePause(w, r, name)

		return
	}

	if r.Method == http.MethodGet {
		h.handleQueueGet(w, r, path)

		return
	}

	writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
}

func (h *adminGatewayHandler) handleQueueGet(w http.ResponseWriter, r *http.Request, name string) {
	name = strings.Trim(name, adminPathSeparator)
	if name == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingQueue, adminErrQueueRequired, requestID(r, w))

		return
	}

	decoded, err := url.PathUnescape(name)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_queue", "Invalid queue name", requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetQueue(ctx, &workerpb.GetQueueRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	queue := resp.GetQueue()
	payload := adminQueueJSON{
		Queue: queueSummaryJSON{
			Name:       queue.GetName(),
			Ready:      queue.GetReady(),
			Processing: queue.GetProcessing(),
			Dead:       queue.GetDead(),
			Weight:     queue.GetWeight(),
			Paused:     queue.GetPaused(),
		},
	}

	writeAdminJSON(w, http.StatusOK, payload)
}

func (h *adminGatewayHandler) handleQueueWeight(w http.ResponseWriter, r *http.Request, name string) {
	name = strings.Trim(name, adminPathSeparator)
	if name == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingQueue, adminErrQueueRequired, requestID(r, w))

		return
	}

	decoded, err := url.PathUnescape(name)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_queue", "Invalid queue name", requestID(r, w))

		return
	}

	switch r.Method {
	case http.MethodPost:
		h.handleQueueWeightUpdate(w, r, decoded)
	case http.MethodDelete:
		h.handleQueueWeightReset(w, r, decoded)
	default:
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
	}
}

func (h *adminGatewayHandler) handleQueueWeightUpdate(w http.ResponseWriter, r *http.Request, name string) {
	reqID := requestID(r, w)

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	var payload adminQueueWeightRequest

	err = json.Unmarshal(body, &payload)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.UpdateQueueWeight(ctx, &workerpb.UpdateQueueWeightRequest{
		Name:   name,
		Weight: clampLimit32(payload.Weight),
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminQueueJSON{Queue: queueSummaryFromProto(resp.GetQueue())})
}

func (h *adminGatewayHandler) handleQueueWeightReset(w http.ResponseWriter, r *http.Request, name string) {
	reqID := requestID(r, w)

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ResetQueueWeight(ctx, &workerpb.ResetQueueWeightRequest{Name: name})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminQueueJSON{Queue: queueSummaryFromProto(resp.GetQueue())})
}

func (h *adminGatewayHandler) handleQueuePause(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	name = strings.Trim(name, adminPathSeparator)
	if name == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingQueue, adminErrQueueRequired, requestID(r, w))

		return
	}

	decoded, err := url.PathUnescape(name)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_queue", "Invalid queue name", requestID(r, w))

		return
	}

	reqID := requestID(r, w)

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	payload := adminQueuePauseRequest{Paused: true}
	if len(body) > 0 {
		err := json.Unmarshal(body, &payload)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.PauseQueue(ctx, &workerpb.PauseQueueRequest{
		Name:   decoded,
		Paused: payload.Paused,
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminQueueJSON{Queue: queueSummaryFromProto(resp.GetQueue())})
}

func (h *adminGatewayHandler) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleJobsList(w, r)
	case http.MethodPost:
		h.handleJobUpsert(w, r)
	default:
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
	}
}

func (h *adminGatewayHandler) handleJobsList(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListJobs(ctx, &workerpb.ListJobsRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	jobs := make([]adminJobJSON, 0, len(resp.GetJobs()))
	for _, job := range resp.GetJobs() {
		jobs = append(jobs, adminJobFromProto(job))
	}

	writeAdminJSON(w, http.StatusOK, adminJobsJSON{Jobs: jobs})
}

func (h *adminGatewayHandler) handleJobUpsert(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r, w)

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	var payload adminJobRequest
	if len(body) > 0 {
		err = json.Unmarshal(body, &payload)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	spec, err := jobSpecToProto(payload)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJob, err.Error(), reqID)

		return
	}

	resp, err := h.client.UpsertJob(ctx, &workerpb.UpsertJobRequest{
		Spec: spec,
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminJobResponse{Job: adminJobFromProto(resp.GetJob())})
}

func (h *adminGatewayHandler) handleJob(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/jobs/")

	path = strings.Trim(path, adminPathSeparator)
	if path == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingJob, adminErrJobRequired, requestID(r, w))

		return
	}

	if before, ok := strings.CutSuffix(path, "/artifact/meta"); ok {
		name := strings.TrimSuffix(before, adminPathSeparator)
		h.handleJobArtifactMeta(w, r, name)

		return
	}

	if before, ok := strings.CutSuffix(path, "/artifact"); ok {
		name := strings.TrimSuffix(before, adminPathSeparator)
		h.handleJobArtifact(w, r, name)

		return
	}

	if before, ok := strings.CutSuffix(path, "/run"); ok {
		name := strings.TrimSuffix(before, adminPathSeparator)
		h.handleJobRun(w, r, name)

		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleJobGet(w, r, path)
	case http.MethodDelete:
		h.handleJobDelete(w, r, path)
	default:
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
	}
}

func (h *adminGatewayHandler) handleJobGet(w http.ResponseWriter, r *http.Request, name string) {
	name = strings.Trim(name, adminPathSeparator)
	if name == "" {
		writeAdminError(w, http.StatusBadRequest, adminErrMissingJob, adminErrJobRequired, requestID(r, w))

		return
	}

	decoded, err := url.PathUnescape(name)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJob, adminErrInvalidJobMsg, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetJob(ctx, &workerpb.GetJobRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminJobResponse{Job: adminJobFromProto(resp.GetJob())})
}

func (h *adminGatewayHandler) handleJobDelete(w http.ResponseWriter, r *http.Request, name string) {
	reqID := requestID(r, w)

	decoded, err := url.PathUnescape(strings.TrimSpace(name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJob, adminErrInvalidJobMsg, reqID)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.DeleteJob(ctx, &workerpb.DeleteJobRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminJobDeleteResponse{Deleted: resp.GetDeleted()})
}

func (h *adminGatewayHandler) handleJobRun(w http.ResponseWriter, r *http.Request, name string) {
	handleRunAction(w, r, name, adminErrInvalidJob, adminErrInvalidJobMsg,
		func(ctx context.Context, decoded string) (string, error) {
			resp, err := h.client.RunJob(ctx, &workerpb.RunJobRequest{Name: decoded})
			if err != nil {
				return "", err
			}

			return resp.GetTaskId(), nil
		},
		func(taskID string) any {
			return adminJobRunResponse{TaskID: taskID}
		},
	)
}

func (h *adminGatewayHandler) handleJobArtifactMeta(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	job, reqID, err := h.fetchJobForRequest(r, w, name)
	if err != nil {
		return
	}

	artifact, err := h.artifacts.Resolve(job)
	if err != nil {
		writeAdminArtifactError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminJobArtifactMetaResponse{Artifact: artifact})
}

func (h *adminGatewayHandler) handleJobArtifact(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	job, reqID, err := h.fetchJobForRequest(r, w, name)
	if err != nil {
		return
	}

	artifact, err := h.artifacts.Resolve(job)
	if err != nil {
		writeAdminArtifactError(w, reqID, err)

		return
	}

	if artifact.RedirectURL != "" {
		http.Redirect(w, r, artifact.RedirectURL, http.StatusTemporaryRedirect)

		return
	}

	file, err := artifact.Reader.OpenFile(artifact.LocalPath)
	if err != nil {
		writeAdminArtifactError(w, reqID, ewrap.Wrap(err, "open artifact file"))

		return
	}

	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			log.Printf("admin gateway artifact close request_id=%s err=%v", reqID, closeErr)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		writeAdminArtifactError(w, reqID, ewrap.Wrap(err, "stat artifact file"))

		return
	}

	if !stat.Mode().IsRegular() {
		writeAdminError(w, http.StatusBadRequest, adminErrArtifactDown, "Artifact is not a regular file", reqID)

		return
	}

	w.Header().Set(adminHeaderType, adminTarballMimeType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", contentDispositionFilename(artifact.Filename))

	if artifact.SHA256 != "" {
		w.Header().Set("X-Tarball-Sha256", artifact.SHA256)
	}

	http.ServeContent(w, r, artifact.Filename, stat.ModTime(), file)
}

func (h *adminGatewayHandler) fetchJobForRequest(
	r *http.Request,
	w http.ResponseWriter,
	name string,
) (*workerpb.Job, string, error) {
	reqID := requestID(r, w)

	decoded, err := decodeAdminJobName(name)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJob, adminErrInvalidJobMsg, reqID)

		return nil, reqID, err
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetJob(ctx, &workerpb.GetJobRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return nil, reqID, err
	}

	return resp.GetJob(), reqID, nil
}

func writeAdminArtifactError(w http.ResponseWriter, requestID string, err error) {
	if err == nil {
		writeAdminError(w, http.StatusBadRequest, adminErrArtifactDown, "Artifact is unavailable", requestID)

		return
	}

	if errors.Is(err, ErrAdminJobTarballPathRequired) || errors.Is(err, ErrAdminJobTarballURLRequired) {
		writeAdminError(w, http.StatusBadRequest, adminErrArtifactDown, err.Error(), requestID)

		return
	}

	if errors.Is(err, os.ErrNotExist) {
		writeAdminError(w, http.StatusNotFound, "artifact_not_found", "Artifact not found", requestID)

		return
	}

	if errors.Is(err, ErrAdminUnsupported) {
		writeAdminError(w, http.StatusBadRequest, "artifact_unsupported", adminErrNoArtifact, requestID)

		return
	}

	writeAdminError(w, http.StatusBadGateway, adminErrArtifactDown, err.Error(), requestID)
}

func decodeAdminJobName(raw string) (string, error) {
	decoded, err := url.PathUnescape(strings.TrimSpace(raw))
	if err != nil {
		return "", ewrap.Wrap(err, "decode job name")
	}

	if decoded == "" {
		return "", ErrAdminJobNameRequired
	}

	return decoded, nil
}

func contentDispositionFilename(filename string) string {
	name := strings.TrimSpace(filename)
	if name == "" {
		name = "artifact.tar.gz"
	}

	return fmt.Sprintf(`attachment; filename="%s"`, name)
}

func (h *adminGatewayHandler) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	queryValues := r.URL.Query()
	limit := parseIntParam(queryValues.Get(adminQueryLimit), defaultAdminScheduleEventLimit)
	offset := parseOffsetParam(queryValues.Get("offset"))
	queueFilter := strings.TrimSpace(queryValues.Get("queue"))
	handlerFilter := strings.TrimSpace(queryValues.Get("handler"))
	queryFilter := strings.TrimSpace(queryValues.Get("query"))

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListDLQ(ctx, &workerpb.ListDLQRequest{
		Limit:   clampLimit32(limit),
		Offset:  clampLimit32(offset),
		Queue:   queueFilter,
		Handler: handlerFilter,
		Query:   queryFilter,
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	entries := make([]dlqEntryJSON, 0, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		entries = append(entries, dlqEntryJSON{
			ID:       entry.GetId(),
			Queue:    entry.GetQueue(),
			Handler:  entry.GetHandler(),
			Attempts: entry.GetAttempts(),
			AgeMs:    entry.GetAgeMs(),
		})
	}

	writeAdminJSON(w, http.StatusOK, adminDLQJSON{Entries: entries, Total: resp.GetTotal()})
}

func (h *adminGatewayHandler) handleDLQEntry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/dlq/")

	id := strings.Trim(path, adminPathSeparator)
	if id == "" {
		writeAdminError(w, http.StatusBadRequest, "missing_dlq_id", "DLQ id is required", requestID(r, w))

		return
	}

	decoded, err := url.PathUnescape(id)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_dlq_id", "Invalid DLQ id", requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetDLQEntry(ctx, &workerpb.GetDLQEntryRequest{Id: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminDLQEntryDetailJSON{
		Entry: dlqEntryDetailFromProto(resp.GetEntry()),
	})
}

func (h *adminGatewayHandler) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.PauseDequeue(ctx, &workerpb.PauseDequeueRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminPauseJSON{Paused: resp.GetPaused()})
}

func (h *adminGatewayHandler) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ResumeDequeue(ctx, &workerpb.ResumeDequeueRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminResumeJSON{Paused: resp.GetPaused()})
}

func (h *adminGatewayHandler) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	limit := defaultAdminDLQLimit

	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, "Invalid request body", requestID(r, w))

			return
		}

		if len(body) > 0 {
			var payload struct {
				Limit int `json:"limit"`
			}
			if json.Unmarshal(body, &payload) == nil && payload.Limit > 0 {
				limit = payload.Limit
			}
		}
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = withAdminMetadata(ctx, r, reqID)

	defer cancel()

	resp, err := h.client.ReplayDLQ(ctx, &workerpb.ReplayDLQRequest{Limit: clampLimit32(limit)})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminReplayJSON{Moved: resp.GetMoved()})
}

func (h *adminGatewayHandler) handleReplayIDs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, "Invalid request body", requestID(r, w))

		return
	}

	var payload struct {
		IDs []string `json:"ids"`
	}

	err = json.Unmarshal(body, &payload)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, "Invalid request body", requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = withAdminMetadata(ctx, r, reqID)

	defer cancel()

	resp, err := h.client.ReplayDLQByID(ctx, &workerpb.ReplayDLQByIDRequest{Ids: payload.IDs})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminReplayJSON{Moved: resp.GetMoved()})
}

func (h *adminGatewayHandler) handleScheduleFactories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListScheduleFactories(ctx, &workerpb.ListScheduleFactoriesRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	factories := make([]scheduleFactoryJSON, 0, len(resp.GetFactories()))
	for _, factory := range resp.GetFactories() {
		factories = append(factories, scheduleFactoryFromProto(factory))
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleFactoriesJSON{Factories: factories})
}

func (h *adminGatewayHandler) handleScheduleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	name, limit := parseAdminEventQuery(r, defaultAdminScheduleEventLimit)

	events, err := h.fetchScheduleEvents(r.Context(), requestID(r, w), name, limit)
	if err != nil {
		writeAdminGRPCError(w, requestID(r, w), err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleEventsJSON{Events: events})
}

func (h *adminGatewayHandler) handleJobEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	name, limit := parseAdminEventQuery(r, defaultAdminJobEventLimit)

	events, err := h.fetchJobEvents(r.Context(), requestID(r, w), name, limit)
	if err != nil {
		writeAdminGRPCError(w, requestID(r, w), err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminJobEventsJSON{Events: events})
}

func (h *adminGatewayHandler) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	queryValues := r.URL.Query()
	limit := parseIntParam(queryValues.Get(adminQueryLimit), defaultAdminAuditEventLimit)
	action := strings.TrimSpace(queryValues.Get("action"))
	target := strings.TrimSpace(queryValues.Get("target"))

	events, err := h.fetchAuditEvents(r.Context(), requestID(r, w), action, target, limit)
	if err != nil {
		writeAdminGRPCError(w, requestID(r, w), err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminAuditEventsJSON{Events: events})
}

func (h *adminGatewayHandler) handleAuditExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)
	queryValues := r.URL.Query()
	action := strings.TrimSpace(queryValues.Get("action"))
	target := strings.TrimSpace(queryValues.Get("target"))
	format := parseAuditExportFormat(queryValues.Get("format"))
	limit := parseIntParam(queryValues.Get(adminQueryLimit), defaultAdminAuditEventLimit)

	limit = min(limit, h.auditExportMax)
	if limit <= 0 {
		limit = defaultAdminAuditEventLimit
	}

	events, err := h.fetchAuditEvents(r.Context(), reqID, action, target, limit)
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAuditExportHeaders(w, format)

	switch format {
	case adminExportFmtJSON:
		writeAuditExportJSON(w, events)
	case adminExportFmtCSV:
		writeAuditExportCSV(w, events)
	default:
		writeAuditExportJSONL(w, events)
	}
}

func parseAuditExportFormat(raw string) string {
	format := strings.ToLower(strings.TrimSpace(raw))
	switch format {
	case adminExportFmtJSON, adminExportFmtCSV:
		return format
	default:
		return adminExportFmtJSONL
	}
}

func writeAuditExportHeaders(w http.ResponseWriter, format string) {
	filename := fmt.Sprintf("admin-audit-%d.%s", time.Now().UTC().Unix(), format)

	var contentType string

	switch format {
	case adminExportFmtJSON:
		contentType = "application/json"
	case adminExportFmtCSV:
		contentType = "text/csv"
	default:
		contentType = "application/x-ndjson"
	}

	w.Header().Set(adminHeaderType, contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Disposition", contentDispositionFilename(filename))
}

func writeAuditExportJSON(w http.ResponseWriter, events []adminAuditEventJSON) {
	writeAdminJSON(w, http.StatusOK, adminAuditEventsJSON{Events: events})
}

func writeAuditExportJSONL(w http.ResponseWriter, events []adminAuditEventJSON) {
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	for _, event := range events {
		err := encoder.Encode(event)
		if err != nil {
			log.Printf("admin gateway audit export jsonl encode: %v", err)

			return
		}
	}
}

func writeAuditExportCSV(w http.ResponseWriter, events []adminAuditEventJSON) {
	w.WriteHeader(http.StatusOK)

	writer := csv.NewWriter(w)

	err := writer.Write([]string{
		"at",
		"actor",
		"request_id",
		"action",
		"target",
		"status",
		"payload_hash",
		"detail",
	})
	if err != nil {
		log.Printf("admin gateway audit export csv header: %v", err)

		return
	}

	for _, event := range events {
		err = writer.Write([]string{
			strconv.FormatInt(event.AtMs, 10),
			event.Actor,
			event.RequestID,
			event.Action,
			event.Target,
			event.Status,
			event.PayloadHash,
			event.Detail,
		})
		if err != nil {
			log.Printf("admin gateway audit export csv row: %v", err)

			return
		}
	}

	writer.Flush()

	err = writer.Error()
	if err != nil {
		log.Printf("admin gateway audit export csv flush: %v", err)
	}
}

func (h *adminGatewayHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	h.handleMetricsWithFormat(w, r, strings.TrimSpace(r.URL.Query().Get("format")))
}

func (h *adminGatewayHandler) handleMetricsPrometheus(w http.ResponseWriter, r *http.Request) {
	h.handleMetricsWithFormat(w, r, adminMetricsFmtProm)
}

func (h *adminGatewayHandler) handleMetricsWithFormat(
	w http.ResponseWriter,
	r *http.Request,
	format string,
) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	format = normalizeMetricsFormat(format)
	if h == nil || h.observability == nil {
		if format == adminMetricsFmtProm {
			w.Header().Set(adminHeaderType, adminMetricsPromContentType)

			return
		}

		writeAdminJSON(w, http.StatusOK, AdminObservabilitySnapshot{})

		return
	}

	if format == adminMetricsFmtProm {
		w.Header().Set(adminHeaderType, adminMetricsPromContentType)

		// #nosec G705 -- payload is plain-text Prometheus exposition, not rendered as HTML
		_, err := io.WriteString(w, h.observability.Prometheus())
		if err != nil {
			log.Printf("admin gateway metrics write: %v", err)
		}

		return
	}

	writeAdminJSON(w, http.StatusOK, h.observability.Snapshot())
}

func normalizeMetricsFormat(format string) string {
	normalized := strings.ToLower(strings.TrimSpace(format))
	if normalized == "prom" || normalized == "prometheus" || normalized == "text" {
		return adminMetricsFmtProm
	}

	return adminMetricsFmtJSON
}

func (h *adminGatewayHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAdminError(w, http.StatusInternalServerError, "stream_not_supported", "streaming unsupported", requestID(r, w))

		return
	}

	w.Header().Set(adminHeaderType, "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	reqID := requestID(r, w)
	ctx := r.Context()

	ticker := time.NewTicker(adminEventsInterval)
	defer ticker.Stop()

	writer := newAdminEventWriter(w, flusher, h.eventBuffer)

	lastEventID, hasLastEventID := parseLastEventID(r)
	if hasLastEventID {
		ok = writeAdminEventOrError(writer, writer.ReplayFrom(lastEventID))
		if !ok {
			return
		}
	}

	if !hasLastEventID && !h.emitAdminEvents(ctx, reqID, writer) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !h.emitAdminEvents(ctx, reqID, writer) {
				return
			}
		}
	}
}

func parseLastEventID(r *http.Request) (int64, bool) {
	if r == nil {
		return 0, false
	}

	header := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if header == "" {
		header = strings.TrimSpace(r.URL.Query().Get("lastEventId"))
	}

	if header == "" {
		return 0, false
	}

	value, err := strconv.ParseInt(header, 10, adminParseIntBitSize)
	if err != nil || value <= 0 {
		return 0, false
	}

	return value, true
}

func parseAdminEventQuery(r *http.Request, defaultLimit int) (string, int) {
	queryValues := r.URL.Query()
	limit := parseIntParam(queryValues.Get(adminQueryLimit), defaultLimit)
	name := strings.TrimSpace(queryValues.Get("name"))

	return name, limit
}

func (h *adminGatewayHandler) fetchScheduleEvents(
	ctx context.Context,
	reqID string,
	name string,
	limit int,
) ([]scheduleEventJSON, error) {
	eventsCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	eventsCtx = metadata.AppendToOutgoingContext(eventsCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListScheduleEvents(eventsCtx, &workerpb.ListScheduleEventsRequest{
		Name:  name,
		Limit: clampLimit32(limit),
	})
	if err != nil {
		return nil, err
	}

	events := make([]scheduleEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, scheduleEventFromProto(event))
	}

	return events, nil
}

func (h *adminGatewayHandler) fetchJobEvents(
	ctx context.Context,
	reqID string,
	name string,
	limit int,
) ([]jobEventJSON, error) {
	eventsCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	eventsCtx = metadata.AppendToOutgoingContext(eventsCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListJobEvents(eventsCtx, &workerpb.ListJobEventsRequest{
		Name:  name,
		Limit: clampLimit32(limit),
	})
	if err != nil {
		return nil, err
	}

	events := make([]jobEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, jobEventFromProto(event))
	}

	return events, nil
}

func (h *adminGatewayHandler) fetchAuditEvents(
	ctx context.Context,
	reqID string,
	action string,
	target string,
	limit int,
) ([]adminAuditEventJSON, error) {
	eventsCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	eventsCtx = metadata.AppendToOutgoingContext(eventsCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListAuditEvents(eventsCtx, &workerpb.ListAuditEventsRequest{
		Action: action,
		Target: target,
		Limit:  clampLimit32(limit),
	})
	if err != nil {
		return nil, err
	}

	events := make([]adminAuditEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, adminAuditEventFromProto(event))
	}

	return events, nil
}

func (h *adminGatewayHandler) emitAdminEvents(
	ctx context.Context,
	reqID string,
	writer *adminEventWriter,
) bool {
	if !writeAdminEventOrError(writer, h.sendOverviewEvent(ctx, reqID, writer)) {
		return false
	}

	if !writeAdminEventOrError(writer, h.sendScheduleEventsEvent(ctx, reqID, writer)) {
		return false
	}

	if !writeAdminEventOrError(writer, h.sendJobEventsEvent(ctx, reqID, writer)) {
		return false
	}

	if !writeAdminEventOrError(writer, h.sendAuditEventsEvent(ctx, reqID, writer)) {
		return false
	}

	return writer.Write("heartbeat", map[string]any{"ts": time.Now().UnixMilli()}) == nil
}

func writeAdminEventOrError(writer *adminEventWriter, err error) bool {
	if err == nil {
		return true
	}

	return writer.Write("error", map[string]any{"message": err.Error()}) == nil
}

type adminEventWriter struct {
	writer  io.Writer
	flusher http.Flusher
	buffer  *adminEventBuffer
}

func newAdminEventWriter(writer io.Writer, flusher http.Flusher, buffer *adminEventBuffer) *adminEventWriter {
	return &adminEventWriter{writer: writer, flusher: flusher, buffer: buffer}
}

func (w *adminEventWriter) Write(eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return ewrap.Wrap(err, "marshal event payload").WithMetadata("event", eventName)
	}

	id := int64(0)
	if w.buffer != nil {
		id = w.buffer.Append(eventName, data)
	}

	if id > 0 {
		_, err = fmt.Fprintf(w.writer, "id: %d\nevent: %s\ndata: %s\n\n", id, eventName, data)
	} else {
		_, err = fmt.Fprintf(w.writer, "event: %s\ndata: %s\n\n", eventName, data)
	}

	if err != nil {
		return ewrap.Wrap(err, "write event payload").WithMetadata("event", eventName)
	}

	w.flusher.Flush()

	return nil
}

func (w *adminEventWriter) ReplayFrom(lastID int64) error {
	if w.buffer == nil {
		return nil
	}

	events := w.buffer.EventsSince(lastID)
	for _, event := range events {
		// #nosec G705 -- SSE frames carry JSON payload bytes and are consumed as event-stream, not HTML
		_, err := fmt.Fprintf(
			w.writer,
			"id: %d\nevent: %s\ndata: %s\n\n",
			event.ID,
			event.Name,
			event.Data,
		)
		if err != nil {
			return ewrap.Wrap(err, "write replay event payload").WithMetadata("event", event.Name)
		}
	}

	if len(events) > 0 {
		w.flusher.Flush()
	}

	return nil
}

type adminEventMessage struct {
	ID   int64
	Name string
	Data []byte
}

type adminEventBuffer struct {
	mu     sync.Mutex
	max    int
	nextID int64
	events []adminEventMessage
}

func newAdminEventBuffer(maxEvents int) *adminEventBuffer {
	if maxEvents <= 0 {
		maxEvents = adminEventsReplaySize
	}

	return &adminEventBuffer{
		max:    maxEvents,
		nextID: 1,
		events: make([]adminEventMessage, 0, maxEvents),
	}
}

func (b *adminEventBuffer) Append(name string, data []byte) int64 {
	if b == nil {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.nextID
	b.nextID++

	entry := adminEventMessage{
		ID:   id,
		Name: name,
		Data: append([]byte(nil), data...),
	}

	b.events = append(b.events, entry)
	if len(b.events) > b.max {
		b.events = b.events[len(b.events)-b.max:]
	}

	return id
}

func (b *adminEventBuffer) EventsSince(lastID int64) []adminEventMessage {
	if b == nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	events := make([]adminEventMessage, 0, len(b.events))
	for _, entry := range b.events {
		if entry.ID <= lastID {
			continue
		}

		events = append(events, adminEventMessage{
			ID:   entry.ID,
			Name: entry.Name,
			Data: append([]byte(nil), entry.Data...),
		})
	}

	return events
}

func (h *adminGatewayHandler) sendOverviewEvent(ctx context.Context, reqID string, writer *adminEventWriter) error {
	overviewCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	overviewCtx = metadata.AppendToOutgoingContext(overviewCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.GetOverview(overviewCtx, &workerpb.GetOverviewRequest{})
	if err != nil {
		return err
	}

	payload := adminOverviewJSON{
		Stats: overviewStatsJSON{
			ActiveWorkers: resp.GetStats().GetActiveWorkers(),
			QueuedTasks:   resp.GetStats().GetQueuedTasks(),
			Queues:        resp.GetStats().GetQueues(),
			AvgLatencyMs:  resp.GetStats().GetAvgLatencyMs(),
			P95LatencyMs:  resp.GetStats().GetP95LatencyMs(),
		},
		Coordination: coordinationJSON{
			GlobalRateLimit: resp.GetCoordination().GetGlobalRateLimit(),
			LeaderLock:      resp.GetCoordination().GetLeaderLock(),
			Lease:           resp.GetCoordination().GetLease(),
			Paused:          resp.GetCoordination().GetPaused(),
		},
		Actions: adminActionsJSON{
			Pause:  resp.GetActions().GetPause(),
			Resume: resp.GetActions().GetResume(),
			Replay: resp.GetActions().GetReplay(),
		},
	}

	return writer.Write("overview", payload)
}

func (h *adminGatewayHandler) sendScheduleEventsEvent(ctx context.Context, reqID string, writer *adminEventWriter) error {
	eventsCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	eventsCtx = metadata.AppendToOutgoingContext(eventsCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListScheduleEvents(eventsCtx, &workerpb.ListScheduleEventsRequest{
		Limit: clampLimit32(adminEventsMaxCount),
	})
	if err != nil {
		return err
	}

	events := make([]scheduleEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, scheduleEventFromProto(event))
	}

	return writer.Write("schedule_events", adminScheduleEventsJSON{Events: events})
}

func (h *adminGatewayHandler) sendJobEventsEvent(ctx context.Context, reqID string, writer *adminEventWriter) error {
	eventsCtx, cancel := context.WithTimeout(ctx, adminRequestTimeout)
	eventsCtx = metadata.AppendToOutgoingContext(eventsCtx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListJobEvents(eventsCtx, &workerpb.ListJobEventsRequest{
		Limit: clampLimit32(adminEventsMaxCount),
	})
	if err != nil {
		return err
	}

	events := make([]jobEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, jobEventFromProto(event))
	}

	return writer.Write("job_events", adminJobEventsJSON{Events: events})
}

func (h *adminGatewayHandler) sendAuditEventsEvent(ctx context.Context, reqID string, writer *adminEventWriter) error {
	events, err := h.fetchAuditEvents(ctx, reqID, "", "", adminEventsMaxCount)
	if err != nil {
		return err
	}

	return writer.Write("audit_events", adminAuditEventsJSON{Events: events})
}

func (h *adminGatewayHandler) handleSchedules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleSchedulesList(w, r)
	case http.MethodPost:
		h.handleScheduleCreate(w, r)
	default:
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
	}
}

func (h *adminGatewayHandler) handleSchedulesList(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListSchedules(ctx, &workerpb.ListSchedulesRequest{})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	schedules := make([]scheduleJSON, 0, len(resp.GetSchedules()))
	for _, schedule := range resp.GetSchedules() {
		schedules = append(schedules, scheduleFromProto(schedule))
	}

	writeAdminJSON(w, http.StatusOK, adminSchedulesJSON{Schedules: schedules})
}

func (h *adminGatewayHandler) handleScheduleCreate(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r, w)

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	var payload adminScheduleCreateRequest

	err = json.Unmarshal(body, &payload)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.CreateSchedule(ctx, &workerpb.CreateScheduleRequest{
		Name:    payload.Name,
		Spec:    payload.Spec,
		Durable: payload.Durable,
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleCreateResponse{
		Schedule: scheduleFromProto(resp.GetSchedule()),
	})
}

func (h *adminGatewayHandler) handleSchedulesPause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	payload := adminSchedulePauseRequest{Paused: true}
	if len(body) > 0 {
		err := json.Unmarshal(body, &payload)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.PauseSchedules(ctx, &workerpb.PauseSchedulesRequest{Paused: payload.Paused})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminSchedulesPauseResponse{
		Updated: resp.GetUpdated(),
		Paused:  resp.GetPaused(),
	})
}

func (h *adminGatewayHandler) handleSchedule(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/v1/schedules/")

	path = strings.TrimPrefix(path, adminPathSeparator)
	if path == "" {
		writeAdminError(w, http.StatusNotFound, "schedule_not_found", "schedule name is required", requestID(r, w))

		return
	}

	if before, ok := strings.CutSuffix(path, "/pause"); ok {
		name := before
		h.handleSchedulePause(w, r, strings.TrimSuffix(name, adminPathSeparator))

		return
	}

	if before, ok := strings.CutSuffix(path, "/run"); ok {
		name := before
		h.handleScheduleRun(w, r, strings.TrimSuffix(name, adminPathSeparator))

		return
	}

	if r.Method == http.MethodDelete {
		h.handleScheduleDelete(w, r, path)

		return
	}

	writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))
}

func (h *adminGatewayHandler) handleScheduleDelete(w http.ResponseWriter, r *http.Request, name string) {
	reqID := requestID(r, w)

	decoded, err := url.PathUnescape(strings.TrimSpace(name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_name", "invalid schedule name", reqID)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.DeleteSchedule(ctx, &workerpb.DeleteScheduleRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleDeleteResponse{Deleted: resp.GetDeleted()})
}

func (h *adminGatewayHandler) handleSchedulePause(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)

	decoded, err := url.PathUnescape(strings.TrimSpace(name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_name", "invalid schedule name", reqID)

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, adminBodyLimitBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, adminErrInvalidBody, adminErrReadBodyMsg, reqID)

		return
	}

	payload := adminSchedulePauseRequest{Paused: true}
	if len(body) > 0 {
		err := json.Unmarshal(body, &payload)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, adminErrInvalidJSON, adminErrParseBodyMsg, reqID)

			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.PauseSchedule(ctx, &workerpb.PauseScheduleRequest{
		Name:   decoded,
		Paused: payload.Paused,
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminSchedulePauseResponse{
		Schedule: scheduleFromProto(resp.GetSchedule()),
	})
}

func (h *adminGatewayHandler) handleScheduleRun(w http.ResponseWriter, r *http.Request, name string) {
	handleRunAction(w, r, name, "invalid_name", "invalid schedule name",
		func(ctx context.Context, decoded string) (string, error) {
			resp, err := h.client.RunSchedule(ctx, &workerpb.RunScheduleRequest{Name: decoded})
			if err != nil {
				return "", err
			}

			return resp.GetTaskId(), nil
		},
		func(taskID string) any {
			return adminScheduleRunResponse{TaskID: taskID}
		},
	)
}

func handleRunAction(
	w http.ResponseWriter,
	r *http.Request,
	name string,
	invalidCode string,
	invalidMessage string,
	run func(ctx context.Context, decoded string) (string, error),
	response func(taskID string) any,
) {
	if r.Method != http.MethodPost {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	reqID := requestID(r, w)

	decoded, err := url.PathUnescape(strings.TrimSpace(name))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, invalidCode, invalidMessage, reqID)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = withAdminMetadata(ctx, r, reqID)

	defer cancel()

	taskID, err := run(ctx, decoded)
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, response(taskID))
}

func withAdminMetadata(ctx context.Context, r *http.Request, reqID string) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)
	if r == nil {
		return ctx
	}

	approval := strings.TrimSpace(r.Header.Get(adminApprovalHeader))
	if approval != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, adminApprovalMetaKey, approval)
	}

	return ctx
}

type adminOverviewJSON struct {
	Stats        overviewStatsJSON `json:"stats"`
	Coordination coordinationJSON  `json:"coordination"`
	Actions      adminActionsJSON  `json:"actions"`
}

type overviewStatsJSON struct {
	ActiveWorkers int32 `json:"activeWorkers"`
	QueuedTasks   int64 `json:"queuedTasks"`
	Queues        int32 `json:"queues"`
	AvgLatencyMs  int64 `json:"avgLatencyMs"`
	P95LatencyMs  int64 `json:"p95LatencyMs"`
}

type coordinationJSON struct {
	GlobalRateLimit string `json:"globalRateLimit"`
	LeaderLock      string `json:"leaderLock"`
	Lease           string `json:"lease"`
	Paused          bool   `json:"paused"`
}

type adminActionsJSON struct {
	Pause  int64 `json:"pause"`
	Resume int64 `json:"resume"`
	Replay int64 `json:"replay"`
}

type adminQueuesJSON struct {
	Queues []queueSummaryJSON `json:"queues"`
}

type adminQueueJSON struct {
	Queue queueSummaryJSON `json:"queue"`
}

type adminJobsJSON struct {
	Jobs []adminJobJSON `json:"jobs"`
}

type adminJobResponse struct {
	Job adminJobJSON `json:"job"`
}

type adminJobArtifactMetaResponse struct {
	Artifact adminJobArtifactJSON `json:"artifact"`
}

type adminJobDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type adminJobRunResponse struct {
	TaskID string `json:"taskId"`
}

type adminSchedulesJSON struct {
	Schedules []scheduleJSON `json:"schedules"`
}

type adminScheduleFactoriesJSON struct {
	Factories []scheduleFactoryJSON `json:"factories"`
}

type adminScheduleEventsJSON struct {
	Events []scheduleEventJSON `json:"events"`
}

type adminJobEventsJSON struct {
	Events []jobEventJSON `json:"events"`
}

type adminAuditEventsJSON struct {
	Events []adminAuditEventJSON `json:"events"`
}

type adminQueueWeightRequest struct {
	Weight int `json:"weight"`
}

type adminQueuePauseRequest struct {
	Paused bool `json:"paused"`
}

type adminJobRequest struct {
	Name           string   `json:"name"`
	Description    string   `json:"description,omitempty"`
	Repo           string   `json:"repo"`
	Tag            string   `json:"tag"`
	Source         string   `json:"source,omitempty"`
	TarballURL     string   `json:"tarballUrl,omitempty"`
	TarballPath    string   `json:"tarballPath,omitempty"`
	TarballSHA     string   `json:"tarballSha256,omitempty"`
	Path           string   `json:"path,omitempty"`
	Dockerfile     string   `json:"dockerfile,omitempty"`
	Command        []string `json:"command,omitempty"`
	Env            []string `json:"env,omitempty"`
	Queue          string   `json:"queue,omitempty"`
	Retries        int      `json:"retries,omitempty"`
	TimeoutSeconds int64    `json:"timeoutSeconds,omitempty"`
}

type scheduleJSON struct {
	Name      string `json:"name"`
	Schedule  string `json:"schedule"`
	NextRun   string `json:"nextRun"`
	LastRun   string `json:"lastRun"`
	NextRunMs int64  `json:"nextRunMs"`
	LastRunMs int64  `json:"lastRunMs"`
	Status    string `json:"status"`
	Paused    bool   `json:"paused"`
	Durable   bool   `json:"durable"`
}

type adminJobJSON struct {
	Name           string   `json:"name"`
	Description    string   `json:"description,omitempty"`
	Repo           string   `json:"repo"`
	Tag            string   `json:"tag"`
	Source         string   `json:"source,omitempty"`
	TarballURL     string   `json:"tarballUrl,omitempty"`
	TarballPath    string   `json:"tarballPath,omitempty"`
	TarballSHA     string   `json:"tarballSha256,omitempty"`
	Path           string   `json:"path,omitempty"`
	Dockerfile     string   `json:"dockerfile,omitempty"`
	Command        []string `json:"command,omitempty"`
	Env            []string `json:"env,omitempty"`
	Queue          string   `json:"queue,omitempty"`
	Retries        int      `json:"retries,omitempty"`
	TimeoutSeconds int64    `json:"timeoutSeconds,omitempty"`
	CreatedAtMs    int64    `json:"createdAtMs"`
	UpdatedAtMs    int64    `json:"updatedAtMs"`
}

type adminJobArtifactJSON struct {
	Name         string           `json:"name"`
	Source       string           `json:"source"`
	Downloadable bool             `json:"downloadable"`
	RedirectURL  string           `json:"redirectUrl,omitempty"`
	Filename     string           `json:"filename,omitempty"`
	SizeBytes    int64            `json:"sizeBytes,omitempty"`
	SHA256       string           `json:"sha256,omitempty"`
	LocalPath    string           `json:"-"`
	Reader       *sectools.Client `json:"-"`
}

type scheduleFactoryJSON struct {
	Name    string `json:"name"`
	Durable bool   `json:"durable"`
}

type scheduleEventJSON struct {
	TaskID       string            `json:"taskId"`
	Name         string            `json:"name"`
	Spec         string            `json:"spec"`
	Durable      bool              `json:"durable"`
	Status       string            `json:"status"`
	Queue        string            `json:"queue"`
	StartedAtMs  int64             `json:"startedAtMs"`
	FinishedAtMs int64             `json:"finishedAtMs"`
	DurationMs   int64             `json:"durationMs"`
	Result       string            `json:"result"`
	Error        string            `json:"error"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type jobEventJSON struct {
	TaskID       string            `json:"taskId"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	Queue        string            `json:"queue"`
	Repo         string            `json:"repo"`
	Tag          string            `json:"tag"`
	Path         string            `json:"path,omitempty"`
	Dockerfile   string            `json:"dockerfile,omitempty"`
	Command      string            `json:"command,omitempty"`
	ScheduleName string            `json:"scheduleName,omitempty"`
	ScheduleSpec string            `json:"scheduleSpec,omitempty"`
	StartedAtMs  int64             `json:"startedAtMs"`
	FinishedAtMs int64             `json:"finishedAtMs"`
	DurationMs   int64             `json:"durationMs"`
	Result       string            `json:"result,omitempty"`
	Error        string            `json:"error,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type adminAuditEventJSON struct {
	AtMs        int64             `json:"atMs"`
	Actor       string            `json:"actor"`
	RequestID   string            `json:"requestId"`
	Action      string            `json:"action"`
	Target      string            `json:"target"`
	Status      string            `json:"status"`
	PayloadHash string            `json:"payloadHash"`
	Detail      string            `json:"detail"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type adminScheduleCreateRequest struct {
	Name    string `json:"name"`
	Spec    string `json:"spec"`
	Durable bool   `json:"durable"`
}

type adminScheduleCreateResponse struct {
	Schedule scheduleJSON `json:"schedule"`
}

type adminSchedulePauseRequest struct {
	Paused bool `json:"paused"`
}

type adminSchedulePauseResponse struct {
	Schedule scheduleJSON `json:"schedule"`
}

type adminSchedulesPauseResponse struct {
	Updated int32 `json:"updated"`
	Paused  bool  `json:"paused"`
}

type adminScheduleRunResponse struct {
	TaskID string `json:"taskId"`
}

type adminScheduleDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

type queueSummaryJSON struct {
	Name       string `json:"name"`
	Ready      int64  `json:"ready"`
	Processing int64  `json:"processing"`
	Dead       int64  `json:"dead"`
	Weight     int32  `json:"weight"`
	Paused     bool   `json:"paused"`
}

type adminDLQEntryDetailJSON struct {
	Entry dlqEntryDetailJSON `json:"entry"`
}

type dlqEntryDetailJSON struct {
	ID          string            `json:"id"`
	Queue       string            `json:"queue"`
	Handler     string            `json:"handler"`
	Attempts    int32             `json:"attempts"`
	AgeMs       int64             `json:"ageMs"`
	FailedAtMs  int64             `json:"failedAtMs"`
	UpdatedAtMs int64             `json:"updatedAtMs"`
	LastError   string            `json:"lastError"`
	PayloadSize int64             `json:"payloadSize"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type adminDLQJSON struct {
	Entries []dlqEntryJSON `json:"entries"`
	Total   int64          `json:"total"`
}

type dlqEntryJSON struct {
	ID       string `json:"id"`
	Queue    string `json:"queue"`
	Handler  string `json:"handler"`
	Attempts int32  `json:"attempts"`
	AgeMs    int64  `json:"ageMs"`
}

type adminPauseJSON struct {
	Paused bool `json:"paused"`
}

type adminResumeJSON struct {
	Paused bool `json:"paused"`
}

type adminReplayJSON struct {
	Moved int32 `json:"moved"`
}

type adminHealthJSON struct {
	Status    string `json:"status"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"buildTime"`
	GoVersion string `json:"goVersion"`
}

func writeAdminJSON(w http.ResponseWriter, statusCode int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "encode_failed", http.StatusInternalServerError)

		return
	}

	w.Header().Set(adminHeaderType, "application/json")
	w.WriteHeader(statusCode)

	// #nosec G705 -- response body is JSON-encoded and content-type is application/json
	_, err = w.Write(data)
	if err != nil {
		_ = err
	}
}

func writeAdminError(w http.ResponseWriter, statusCode int, code, message, requestID string) {
	writeAdminJSON(w, statusCode, adminErrorEnvelope{
		RequestID: requestID,
		Error: adminErrorJSON{
			Code:    code,
			Message: message,
		},
	})
}

func writeAdminGRPCError(w http.ResponseWriter, requestID string, err error) {
	httpStatus := http.StatusBadGateway

	code := codes.Unknown
	if st, ok := status.FromError(err); ok {
		code = st.Code()
		httpStatus = grpcCodeToHTTP(code)
	}

	// #nosec G706 -- request_id is normalized and error text is sanitized for control chars before logging
	log.Printf(
		"admin gateway error request_id=%s code=%s err=%s",
		sanitizeLogField(requestID),
		code.String(),
		sanitizeLogField(err.Error()),
	)
	writeAdminError(w, httpStatus, strings.ToLower(code.String()), err.Error(), requestID)
}

func parseIntParam(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}

func parseOffsetParam(raw string) int {
	if raw == "" {
		return 0
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}

	return value
}

func clampLimit32(limit int) int32 {
	if limit <= 0 {
		return 0
	}

	limit32, err := sectconv.ToInt32(limit)
	if err != nil {
		return adminMaxInt32
	}

	return limit32
}

func defaultDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}

	return value
}

func uniqueRoots(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))

	for _, value := range values {
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func requestID(r *http.Request, w http.ResponseWriter) string {
	header := r.Header.Get(adminRequestIDHeader)
	if header == "" {
		header = uuid.NewString()
	} else {
		header = sanitizeRequestID(header)
		if header == "" {
			header = uuid.NewString()
		}
	}

	w.Header().Set(adminRequestIDHeader, header)

	return header
}

func sanitizeRequestID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	builder := strings.Builder{}
	builder.Grow(len(raw))

	for _, char := range raw {
		if isRequestIDCharAllowed(char) {
			builder.WriteRune(char)
		}
	}

	value := builder.String()
	if len(value) > adminRequestIDMaxLen {
		value = value[:adminRequestIDMaxLen]
	}

	return value
}

func isRequestIDCharAllowed(char rune) bool {
	if (char >= 'a' && char <= 'z') ||
		(char >= 'A' && char <= 'Z') ||
		(char >= '0' && char <= '9') {
		return true
	}

	return char == '-' || char == '_' || char == '.'
}

func sanitizeLogField(raw string) string {
	builder := strings.Builder{}
	builder.Grow(len(raw))

	for _, char := range raw {
		if char == '\n' || char == '\r' || char == '\t' {
			builder.WriteRune(' ')

			continue
		}

		if char < adminASCIIControlMin || char == adminASCIIDelete {
			continue
		}

		builder.WriteRune(char)
	}

	return strings.TrimSpace(builder.String())
}

func grpcCodeToHTTP(code codes.Code) int {
	if mapped, ok := grpcToHTTPStatus()[code]; ok {
		return mapped
	}

	return http.StatusBadGateway
}

func grpcToHTTPStatus() map[codes.Code]int {
	return map[codes.Code]int{
		codes.OK:                 http.StatusOK,
		codes.InvalidArgument:    http.StatusBadRequest,
		codes.Unauthenticated:    http.StatusUnauthorized,
		codes.PermissionDenied:   http.StatusForbidden,
		codes.NotFound:           http.StatusNotFound,
		codes.AlreadyExists:      http.StatusConflict,
		codes.ResourceExhausted:  http.StatusTooManyRequests,
		codes.FailedPrecondition: http.StatusPreconditionFailed,
		codes.Aborted:            http.StatusConflict,
		codes.OutOfRange:         http.StatusBadRequest,
		codes.Unimplemented:      http.StatusNotImplemented,
		codes.Internal:           http.StatusInternalServerError,
		codes.DataLoss:           http.StatusInternalServerError,
		codes.DeadlineExceeded:   http.StatusGatewayTimeout,
		codes.Unavailable:        http.StatusServiceUnavailable,
		codes.Canceled:           http.StatusRequestTimeout,
		codes.Unknown:            http.StatusBadGateway,
	}
}

func scheduleFromProto(schedule *workerpb.ScheduleEntry) scheduleJSON {
	if schedule == nil {
		return scheduleJSON{
			Name:     "",
			Schedule: "",
			NextRun:  adminNotAvailable,
			LastRun:  adminNotAvailable,
			Status:   "paused",
			Paused:   true,
		}
	}

	paused := schedule.GetPaused()
	nextRun := time.Time{}
	lastRun := time.Time{}
	scheduleStatus := "healthy"

	nextRunMs := schedule.GetNextRunMs()
	if paused || nextRunMs <= 0 {
		scheduleStatus = "paused"
	} else {
		nextRun = time.UnixMilli(nextRunMs)
		if nextRun.Before(time.Now().Add(-adminScheduleLag)) {
			scheduleStatus = "lagging"
		}
	}

	lastRunMs := schedule.GetLastRunMs()
	if lastRunMs > 0 {
		lastRun = time.UnixMilli(lastRunMs)
	}

	return scheduleJSON{
		Name:      schedule.GetName(),
		Schedule:  schedule.GetSpec(),
		NextRun:   formatScheduleNext(nextRun),
		LastRun:   formatScheduleLast(lastRun),
		NextRunMs: nextRunMs,
		LastRunMs: lastRunMs,
		Status:    scheduleStatus,
		Paused:    paused,
		Durable:   schedule.GetDurable(),
	}
}

func scheduleFactoryFromProto(factory *workerpb.ScheduleFactory) scheduleFactoryJSON {
	if factory == nil {
		return scheduleFactoryJSON{}
	}

	return scheduleFactoryJSON{
		Name:    factory.GetName(),
		Durable: factory.GetDurable(),
	}
}

func scheduleEventFromProto(event *workerpb.ScheduleEvent) scheduleEventJSON {
	if event == nil {
		return scheduleEventJSON{}
	}

	return scheduleEventJSON{
		TaskID:       event.GetTaskId(),
		Name:         event.GetName(),
		Spec:         event.GetSpec(),
		Durable:      event.GetDurable(),
		Status:       event.GetStatus(),
		Queue:        event.GetQueue(),
		StartedAtMs:  event.GetStartedAtMs(),
		FinishedAtMs: event.GetFinishedAtMs(),
		DurationMs:   event.GetDurationMs(),
		Result:       event.GetResult(),
		Error:        event.GetError(),
		Metadata:     event.GetMetadata(),
	}
}

func jobEventFromProto(event *workerpb.JobEvent) jobEventJSON {
	if event == nil {
		return jobEventJSON{}
	}

	return jobEventJSON{
		TaskID:       event.GetTaskId(),
		Name:         event.GetName(),
		Status:       event.GetStatus(),
		Queue:        event.GetQueue(),
		Repo:         event.GetRepo(),
		Tag:          event.GetTag(),
		Path:         event.GetPath(),
		Dockerfile:   event.GetDockerfile(),
		Command:      event.GetCommand(),
		ScheduleName: event.GetScheduleName(),
		ScheduleSpec: event.GetScheduleSpec(),
		StartedAtMs:  event.GetStartedAtMs(),
		FinishedAtMs: event.GetFinishedAtMs(),
		DurationMs:   event.GetDurationMs(),
		Result:       event.GetResult(),
		Error:        event.GetError(),
		Metadata:     event.GetMetadata(),
	}
}

func adminAuditEventFromProto(event *workerpb.AuditEvent) adminAuditEventJSON {
	if event == nil {
		return adminAuditEventJSON{}
	}

	return adminAuditEventJSON{
		AtMs:        event.GetAtMs(),
		Actor:       event.GetActor(),
		RequestID:   event.GetRequestId(),
		Action:      event.GetAction(),
		Target:      event.GetTarget(),
		Status:      event.GetStatus(),
		PayloadHash: event.GetPayloadHash(),
		Detail:      event.GetDetail(),
		Metadata:    event.GetMetadata(),
	}
}

func adminJobFromProto(job *workerpb.Job) adminJobJSON {
	if job == nil {
		return adminJobJSON{}
	}

	spec := job.GetSpec()

	return adminJobJSON{
		Name:           spec.GetName(),
		Description:    spec.GetDescription(),
		Repo:           spec.GetRepo(),
		Tag:            spec.GetTag(),
		Source:         spec.GetSource(),
		TarballURL:     spec.GetTarballUrl(),
		TarballPath:    spec.GetTarballPath(),
		TarballSHA:     spec.GetTarballSha256(),
		Path:           spec.GetPath(),
		Dockerfile:     spec.GetDockerfile(),
		Command:        append([]string{}, spec.GetCommand()...),
		Env:            append([]string{}, spec.GetEnv()...),
		Queue:          spec.GetQueue(),
		Retries:        int(spec.GetRetries()),
		TimeoutSeconds: int64(spec.GetTimeoutSeconds()),
		CreatedAtMs:    job.GetCreatedAtMs(),
		UpdatedAtMs:    job.GetUpdatedAtMs(),
	}
}

func jobSpecToProto(payload adminJobRequest) (*workerpb.JobSpec, error) {
	retriesValue := max(payload.Retries, 0)
	timeoutValue := max(payload.TimeoutSeconds, int64(0))

	retries, err := sectconv.ToInt32(retriesValue)
	if err != nil {
		return nil, ewrap.Wrap(err, "job retries overflow")
	}

	timeoutSeconds, err := sectconv.SafeInt32FromInt64(timeoutValue)
	if err != nil {
		return nil, ewrap.Wrap(err, "job timeout overflow")
	}

	return &workerpb.JobSpec{
		Name:           strings.TrimSpace(payload.Name),
		Description:    strings.TrimSpace(payload.Description),
		Repo:           strings.TrimSpace(payload.Repo),
		Tag:            strings.TrimSpace(payload.Tag),
		Source:         strings.TrimSpace(payload.Source),
		TarballUrl:     strings.TrimSpace(payload.TarballURL),
		TarballPath:    strings.TrimSpace(payload.TarballPath),
		TarballSha256:  strings.TrimSpace(payload.TarballSHA),
		Path:           strings.TrimSpace(payload.Path),
		Dockerfile:     strings.TrimSpace(payload.Dockerfile),
		Command:        append([]string{}, payload.Command...),
		Env:            append([]string{}, payload.Env...),
		Queue:          strings.TrimSpace(payload.Queue),
		Retries:        retries,
		TimeoutSeconds: timeoutSeconds,
	}, nil
}

func dlqEntryDetailFromProto(entry *workerpb.DLQEntryDetail) dlqEntryDetailJSON {
	if entry == nil {
		return dlqEntryDetailJSON{}
	}

	return dlqEntryDetailJSON{
		ID:          entry.GetId(),
		Queue:       entry.GetQueue(),
		Handler:     entry.GetHandler(),
		Attempts:    entry.GetAttempts(),
		AgeMs:       entry.GetAgeMs(),
		FailedAtMs:  entry.GetFailedAtMs(),
		UpdatedAtMs: entry.GetUpdatedAtMs(),
		LastError:   entry.GetLastError(),
		PayloadSize: entry.GetPayloadSize(),
		Metadata:    entry.GetMetadata(),
	}
}

func queueSummaryFromProto(queue *workerpb.QueueSummary) queueSummaryJSON {
	if queue == nil {
		return queueSummaryJSON{}
	}

	return queueSummaryJSON{
		Name:       queue.GetName(),
		Ready:      queue.GetReady(),
		Processing: queue.GetProcessing(),
		Dead:       queue.GetDead(),
		Weight:     queue.GetWeight(),
		Paused:     queue.GetPaused(),
	}
}

func formatScheduleNext(next time.Time) string {
	if next.IsZero() {
		return adminNotAvailable
	}

	now := time.Now()
	if next.Before(now) {
		return "overdue"
	}

	return "in " + formatAdminDuration(next.Sub(now))
}

func formatScheduleLast(last time.Time) string {
	if last.IsZero() {
		return adminNotAvailable
	}

	now := time.Now()
	if last.After(now) {
		return adminNotAvailable
	}

	return formatAdminDuration(now.Sub(last)) + " ago"
}

type adminErrorEnvelope struct {
	RequestID string         `json:"requestId"`
	Error     adminErrorJSON `json:"error"`
}

type adminErrorJSON struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
