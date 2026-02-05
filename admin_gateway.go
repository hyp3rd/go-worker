package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
	adminRequestTimeout   = 5 * time.Second
	adminRequestIDHeader  = "X-Request-Id"
	adminRequestIDMetaKey = "x-request-id"
)

// AdminGatewayConfig configures the admin HTTP gateway.
type AdminGatewayConfig struct {
	GRPCAddr string
	HTTPAddr string

	// GRPCTLS is optional; when nil, the gateway dials gRPC without TLS.
	GRPCTLS *tls.Config

	// TLSCertFile, TLSKeyFile, and TLSCAFile are required for mTLS.
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string

	ReadTimeout  time.Duration
	WriteTimeout time.Duration
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
	grpcAddr := cfg.GRPCAddr
	if grpcAddr == "" {
		grpcAddr = adminDefaultGRPCAddr
	}

	httpAddr := cfg.HTTPAddr
	if httpAddr == "" {
		httpAddr = adminDefaultHTTPAddr
	}

	tlsConfig, err := LoadAdminMTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
	if err != nil {
		return nil, err
	}

	dialOpts := []grpc.DialOption{}
	if cfg.GRPCTLS != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.GRPCTLS)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(grpcAddr, dialOpts...)
	if err != nil {
		return nil, ewrap.Wrap(err, "dial admin gRPC")
	}

	client := workerpb.NewAdminServiceClient(conn)
	handler := &adminGatewayHandler{client: client}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/overview", handler.handleOverview)
	mux.HandleFunc("/admin/v1/queues", handler.handleQueues)
	mux.HandleFunc("/admin/v1/dlq", handler.handleDLQ)
	mux.HandleFunc("/admin/v1/pause", handler.handlePause)
	mux.HandleFunc("/admin/v1/resume", handler.handleResume)
	mux.HandleFunc("/admin/v1/dlq/replay", handler.handleReplay)

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      mux,
		TLSConfig:    tlsConfig,
		ReadTimeout:  defaultDuration(cfg.ReadTimeout, adminDefaultReadTime),
		WriteTimeout: defaultDuration(cfg.WriteTimeout, adminDefaultWriteTime),
	}
	server.RegisterOnShutdown(func() {
		err := conn.Close()
		if err != nil {
			_ = err
		}
	})

	return server, nil
}

type adminGatewayHandler struct {
	client workerpb.AdminServiceClient
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
		queues = append(queues, queueSummaryJSON{
			Name:       queue.GetName(),
			Ready:      queue.GetReady(),
			Processing: queue.GetProcessing(),
			Dead:       queue.GetDead(),
			Weight:     queue.GetWeight(),
		})
	}

	writeAdminJSON(w, http.StatusOK, adminQueuesJSON{Queues: queues})
}

func (h *adminGatewayHandler) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	limit := parseIntParam(r.URL.Query().Get("limit"), defaultAdminDLQLimit)

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListDLQ(ctx, &workerpb.ListDLQRequest{Limit: clampLimit32(limit)})
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

	writeAdminJSON(w, http.StatusOK, adminDLQJSON{Entries: entries})
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
			writeAdminError(w, http.StatusBadRequest, "invalid_body", "Invalid request body", requestID(r, w))

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
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ReplayDLQ(ctx, &workerpb.ReplayDLQRequest{Limit: clampLimit32(limit)})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminReplayJSON{Moved: resp.GetMoved()})
}

type adminOverviewJSON struct {
	Stats        overviewStatsJSON `json:"stats"`
	Coordination coordinationJSON  `json:"coordination"`
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

type adminQueuesJSON struct {
	Queues []queueSummaryJSON `json:"queues"`
}

type queueSummaryJSON struct {
	Name       string `json:"name"`
	Ready      int64  `json:"ready"`
	Processing int64  `json:"processing"`
	Dead       int64  `json:"dead"`
	Weight     int32  `json:"weight"`
}

type adminDLQJSON struct {
	Entries []dlqEntryJSON `json:"entries"`
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

func writeAdminJSON(w http.ResponseWriter, statusCode int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "encode_failed", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

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

	log.Printf("admin gateway error request_id=%s code=%s err=%v", requestID, code.String(), err)
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
	}

	w.Header().Set(adminRequestIDHeader, header)

	return header
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

type adminErrorEnvelope struct {
	RequestID string         `json:"requestId"`
	Error     adminErrorJSON `json:"error"`
}

type adminErrorJSON struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
