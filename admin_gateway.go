package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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
	adminErrInvalidJSON   = "invalid_json"
	adminErrReadBodyMsg   = "failed to read request body"
	adminErrParseBodyMsg  = "failed to parse request body"
	adminErrInvalidBody   = "invalid_body"
	adminErrMissingQueue  = "missing_queue"
	adminErrQueueRequired = "Queue name is required"
	adminRequestTimeout   = 5 * time.Second
	adminRequestIDHeader  = "X-Request-Id"
	adminRequestIDMetaKey = "x-request-id"
	adminPathSeparator    = "/"
	adminScheduleLag      = 2 * time.Minute
	adminEventsInterval   = 10 * time.Second
	adminEventsMaxCount   = 25
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
	mux.HandleFunc("/admin/v1/health", handler.handleHealth)
	mux.HandleFunc("/admin/v1/overview", handler.handleOverview)
	mux.HandleFunc("/admin/v1/queues", handler.handleQueues)
	mux.HandleFunc("/admin/v1/queues/", handler.handleQueue)
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

func (h *adminGatewayHandler) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminError(w, http.StatusMethodNotAllowed, adminErrMethodBlocked, adminErrMethodMessage, requestID(r, w))

		return
	}

	queryValues := r.URL.Query()
	limit := parseIntParam(queryValues.Get("limit"), defaultAdminScheduleEventLimit)
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
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

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
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

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

	queryValues := r.URL.Query()
	limit := parseIntParam(queryValues.Get("limit"), defaultAdminDLQLimit)
	name := strings.TrimSpace(queryValues.Get("name"))

	reqID := requestID(r, w)
	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.ListScheduleEvents(ctx, &workerpb.ListScheduleEventsRequest{
		Name:  name,
		Limit: clampLimit32(limit),
	})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	events := make([]scheduleEventJSON, 0, len(resp.GetEvents()))
	for _, event := range resp.GetEvents() {
		events = append(events, scheduleEventFromProto(event))
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleEventsJSON{Events: events})
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

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	reqID := requestID(r, w)
	ctx := r.Context()

	ticker := time.NewTicker(adminEventsInterval)
	defer ticker.Stop()

	writer := newAdminEventWriter(w, flusher)
	emit := func() bool {
		err := h.sendOverviewEvent(ctx, reqID, writer)
		if err != nil {
			err := writer.Write("error", map[string]any{"message": err.Error()})
			if err != nil {
				return false
			}
		}

		err = h.sendScheduleEventsEvent(ctx, reqID, writer)
		if err != nil {
			err := writer.Write("error", map[string]any{"message": err.Error()})
			if err != nil {
				return false
			}
		}

		err = writer.Write("heartbeat", map[string]any{"ts": time.Now().UnixMilli()})

		return err == nil
	}

	if !emit() {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !emit() {
				return
			}
		}
	}
}

type adminEventWriter struct {
	writer  io.Writer
	flusher http.Flusher
}

func newAdminEventWriter(writer io.Writer, flusher http.Flusher) *adminEventWriter {
	return &adminEventWriter{writer: writer, flusher: flusher}
}

func (w *adminEventWriter) Write(eventName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return ewrap.Wrap(err, "marshal event payload").WithMetadata("event", eventName)
	}

	_, err = fmt.Fprintf(w.writer, "event: %s\ndata: %s\n\n", eventName, data)
	if err != nil {
		return ewrap.Wrap(err, "write event payload").WithMetadata("event", eventName)
	}

	w.flusher.Flush()

	return nil
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

	ctx, cancel := context.WithTimeout(r.Context(), adminRequestTimeout)
	ctx = metadata.AppendToOutgoingContext(ctx, adminRequestIDMetaKey, reqID)

	defer cancel()

	resp, err := h.client.RunSchedule(ctx, &workerpb.RunScheduleRequest{Name: decoded})
	if err != nil {
		writeAdminGRPCError(w, reqID, err)

		return
	}

	writeAdminJSON(w, http.StatusOK, adminScheduleRunResponse{TaskID: resp.GetTaskId()})
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

type adminSchedulesJSON struct {
	Schedules []scheduleJSON `json:"schedules"`
}

type adminScheduleFactoriesJSON struct {
	Factories []scheduleFactoryJSON `json:"factories"`
}

type adminScheduleEventsJSON struct {
	Events []scheduleEventJSON `json:"events"`
}

type adminQueueWeightRequest struct {
	Weight int `json:"weight"`
}

type adminQueuePauseRequest struct {
	Paused bool `json:"paused"`
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
