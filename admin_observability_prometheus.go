package worker

import (
	"sort"
	"strconv"
	"strings"
)

const (
	adminMetricsPromContentType = "text/plain; version=0.0.4; charset=utf-8"
	adminMetricsInitialLines    = 128
)

// Prometheus renders the current observability snapshot in Prometheus text format.
func (collector *AdminObservability) Prometheus() string {
	snapshot := collector.Snapshot()

	lines := make([]string, 0, adminMetricsInitialLines)
	lines = append(lines,
		"# HELP worker_admin_uptime_seconds Admin service uptime in seconds.",
		"# TYPE worker_admin_uptime_seconds gauge",
		"worker_admin_uptime_seconds "+strconv.FormatInt(snapshot.UptimeSec, 10),
		"# HELP worker_admin_http_calls_total Total HTTP requests per route.",
		"# TYPE worker_admin_http_calls_total counter",
		"# HELP worker_admin_http_errors_total Total HTTP error responses per route.",
		"# TYPE worker_admin_http_errors_total counter",
		"# HELP worker_admin_http_duration_milliseconds_sum Total HTTP latency in milliseconds per route.",
		"# TYPE worker_admin_http_duration_milliseconds_sum counter",
		"# HELP worker_admin_http_duration_milliseconds_max Maximum HTTP latency in milliseconds per route.",
		"# TYPE worker_admin_http_duration_milliseconds_max gauge",
		"# HELP worker_admin_grpc_calls_total Total gRPC requests per method.",
		"# TYPE worker_admin_grpc_calls_total counter",
		"# HELP worker_admin_grpc_errors_total Total gRPC errors per method.",
		"# TYPE worker_admin_grpc_errors_total counter",
		"# HELP worker_admin_grpc_duration_milliseconds_sum Total gRPC latency in milliseconds per method.",
		"# TYPE worker_admin_grpc_duration_milliseconds_sum counter",
		"# HELP worker_admin_grpc_duration_milliseconds_max Maximum gRPC latency in milliseconds per method.",
		"# TYPE worker_admin_grpc_duration_milliseconds_max gauge",
		"# HELP worker_admin_jobs_running Current number of running jobs.",
		"# TYPE worker_admin_jobs_running gauge",
		"# HELP worker_admin_jobs_completed_total Total completed jobs.",
		"# TYPE worker_admin_jobs_completed_total counter",
		"# HELP worker_admin_jobs_failed_total Total failed jobs.",
		"# TYPE worker_admin_jobs_failed_total counter",
		"# HELP worker_admin_jobs_duration_milliseconds_sum Total job runtime in milliseconds.",
		"# TYPE worker_admin_jobs_duration_milliseconds_sum counter",
		"# HELP worker_admin_jobs_duration_milliseconds_max Maximum job runtime in milliseconds.",
		"# TYPE worker_admin_jobs_duration_milliseconds_max gauge",
	)

	httpKeys := sortedKeys(snapshot.HTTP)
	for _, key := range httpKeys {
		stat := snapshot.HTTP[key]
		method, route := splitHTTPMetricKey(key)

		labels := `method="` + promEscape(method) + `",route="` + promEscape(route) + `",last_code="` + promEscape(stat.LastCode) + `"`
		lines = append(lines,
			`worker_admin_http_calls_total{`+labels+`} `+strconv.FormatInt(stat.Calls, 10),
			`worker_admin_http_errors_total{`+labels+`} `+strconv.FormatInt(stat.Errors, 10),
			`worker_admin_http_duration_milliseconds_sum{`+labels+`} `+strconv.FormatInt(stat.TotalMs, 10),
			`worker_admin_http_duration_milliseconds_max{`+labels+`} `+strconv.FormatInt(stat.MaxMs, 10),
		)
	}

	grpcKeys := sortedKeys(snapshot.GRPC)
	for _, key := range grpcKeys {
		stat := snapshot.GRPC[key]
		labels := `method="` + promEscape(key) + `",last_code="` + promEscape(stat.LastCode) + `"`
		lines = append(lines,
			`worker_admin_grpc_calls_total{`+labels+`} `+strconv.FormatInt(stat.Calls, 10),
			`worker_admin_grpc_errors_total{`+labels+`} `+strconv.FormatInt(stat.Errors, 10),
			`worker_admin_grpc_duration_milliseconds_sum{`+labels+`} `+strconv.FormatInt(stat.TotalMs, 10),
			`worker_admin_grpc_duration_milliseconds_max{`+labels+`} `+strconv.FormatInt(stat.MaxMs, 10),
		)
	}

	lines = append(lines,
		"worker_admin_jobs_running "+strconv.FormatInt(snapshot.Jobs.Running, 10),
		"worker_admin_jobs_completed_total "+strconv.FormatInt(snapshot.Jobs.Completed, 10),
		"worker_admin_jobs_failed_total "+strconv.FormatInt(snapshot.Jobs.Failed, 10),
		"worker_admin_jobs_duration_milliseconds_sum "+strconv.FormatInt(snapshot.Jobs.TotalMs, 10),
		"worker_admin_jobs_duration_milliseconds_max "+strconv.FormatInt(snapshot.Jobs.MaxMs, 10),
	)

	return strings.Join(lines, "\n") + "\n"
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func splitHTTPMetricKey(key string) (method, route string) {
	method, route, ok := strings.Cut(key, " ")
	if !ok {
		return "unknown", key
	}

	method = strings.TrimSpace(method)
	route = strings.TrimSpace(route)

	if method == "" {
		method = "unknown"
	}

	if route == "" {
		route = "/"
	}

	return method, route
}

func promEscape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)

	return value
}
