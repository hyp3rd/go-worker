package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	testErrStatus      = "status = %d, want 200"
	testErrUnexpected  = "unexpected payload: %+v"
	testQueueDefault   = "default"
	testSampleID       = "abc123"
	testScheduleName1  = "nightly"
	testScheduleCron   = "0 0 * * *"
	testWeightUpdate   = 42
	testDLQLimit       = 50
	testDLQOffset      = 10
	testAuditLimit     = 20
	testOverviewStats1 = 3
	testOverviewStats2 = 42
	testOverviewStats3 = 5
	testOverviewStats4 = 100
	testOverviewStats5 = 250
	testActionReplay   = 3
	testMovedCount     = 5
	testAttemptsCount  = 3
	testEntriesCount   = 3
)

// mockAdminClient is a hand-rolled stub for workerpb.AdminServiceClient. Tests assign
// the per-method function fields they care about; unset methods return an empty
// response and nil error by default. The client also captures the last outgoing
// context so assertions about propagated metadata (request IDs, approval headers)
// can inspect what the gateway forwarded.
//
// assertions can inspect propagated gRPC metadata (request ID, approval).
//
//nolint:containedctx // test-only: mock captures the last outgoing context so
type mockAdminClient struct {
	lastCtx context.Context

	GetHealthFn             func(ctx context.Context, in *workerpb.GetHealthRequest) (*workerpb.GetHealthResponse, error)
	GetOverviewFn           func(ctx context.Context, in *workerpb.GetOverviewRequest) (*workerpb.GetOverviewResponse, error)
	ListQueuesFn            func(ctx context.Context, in *workerpb.ListQueuesRequest) (*workerpb.ListQueuesResponse, error)
	GetQueueFn              func(ctx context.Context, in *workerpb.GetQueueRequest) (*workerpb.GetQueueResponse, error)
	UpdateQueueWeightFn     func(ctx context.Context, in *workerpb.UpdateQueueWeightRequest) (*workerpb.UpdateQueueWeightResponse, error)
	ResetQueueWeightFn      func(ctx context.Context, in *workerpb.ResetQueueWeightRequest) (*workerpb.ResetQueueWeightResponse, error)
	PauseQueueFn            func(ctx context.Context, in *workerpb.PauseQueueRequest) (*workerpb.PauseQueueResponse, error)
	ListJobsFn              func(ctx context.Context, in *workerpb.ListJobsRequest) (*workerpb.ListJobsResponse, error)
	ListJobEventsFn         func(ctx context.Context, in *workerpb.ListJobEventsRequest) (*workerpb.ListJobEventsResponse, error)
	ListAuditEventsFn       func(ctx context.Context, in *workerpb.ListAuditEventsRequest) (*workerpb.ListAuditEventsResponse, error)
	GetJobFn                func(ctx context.Context, in *workerpb.GetJobRequest) (*workerpb.GetJobResponse, error)
	UpsertJobFn             func(ctx context.Context, in *workerpb.UpsertJobRequest) (*workerpb.UpsertJobResponse, error)
	DeleteJobFn             func(ctx context.Context, in *workerpb.DeleteJobRequest) (*workerpb.DeleteJobResponse, error)
	RunJobFn                func(ctx context.Context, in *workerpb.RunJobRequest) (*workerpb.RunJobResponse, error)
	ListScheduleFactoriesFn func(
		ctx context.Context, in *workerpb.ListScheduleFactoriesRequest,
	) (*workerpb.ListScheduleFactoriesResponse, error)
	ListScheduleEventsFn func(
		ctx context.Context, in *workerpb.ListScheduleEventsRequest,
	) (*workerpb.ListScheduleEventsResponse, error)
	ListSchedulesFn  func(ctx context.Context, in *workerpb.ListSchedulesRequest) (*workerpb.ListSchedulesResponse, error)
	CreateScheduleFn func(ctx context.Context, in *workerpb.CreateScheduleRequest) (*workerpb.CreateScheduleResponse, error)
	DeleteScheduleFn func(ctx context.Context, in *workerpb.DeleteScheduleRequest) (*workerpb.DeleteScheduleResponse, error)
	PauseScheduleFn  func(ctx context.Context, in *workerpb.PauseScheduleRequest) (*workerpb.PauseScheduleResponse, error)
	RunScheduleFn    func(ctx context.Context, in *workerpb.RunScheduleRequest) (*workerpb.RunScheduleResponse, error)
	PauseSchedulesFn func(ctx context.Context, in *workerpb.PauseSchedulesRequest) (*workerpb.PauseSchedulesResponse, error)
	ListDLQFn        func(ctx context.Context, in *workerpb.ListDLQRequest) (*workerpb.ListDLQResponse, error)
	GetDLQEntryFn    func(ctx context.Context, in *workerpb.GetDLQEntryRequest) (*workerpb.GetDLQEntryResponse, error)
	PauseDequeueFn   func(ctx context.Context, in *workerpb.PauseDequeueRequest) (*workerpb.PauseDequeueResponse, error)
	ResumeDequeueFn  func(ctx context.Context, in *workerpb.ResumeDequeueRequest) (*workerpb.ResumeDequeueResponse, error)
	ReplayDLQFn      func(ctx context.Context, in *workerpb.ReplayDLQRequest) (*workerpb.ReplayDLQResponse, error)
	ReplayDLQByIDFn  func(ctx context.Context, in *workerpb.ReplayDLQByIDRequest) (*workerpb.ReplayDLQByIDResponse, error)
}

func (m *mockAdminClient) GetHealth(
	ctx context.Context,
	in *workerpb.GetHealthRequest,
	_ ...grpc.CallOption,
) (*workerpb.GetHealthResponse, error) {
	m.lastCtx = ctx
	if m.GetHealthFn != nil {
		return m.GetHealthFn(ctx, in)
	}

	return &workerpb.GetHealthResponse{}, nil
}

func (m *mockAdminClient) GetOverview(
	ctx context.Context,
	in *workerpb.GetOverviewRequest,
	_ ...grpc.CallOption,
) (*workerpb.GetOverviewResponse, error) {
	m.lastCtx = ctx
	if m.GetOverviewFn != nil {
		return m.GetOverviewFn(ctx, in)
	}

	return &workerpb.GetOverviewResponse{}, nil
}

func (m *mockAdminClient) ListQueues(
	ctx context.Context,
	in *workerpb.ListQueuesRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListQueuesResponse, error) {
	m.lastCtx = ctx
	if m.ListQueuesFn != nil {
		return m.ListQueuesFn(ctx, in)
	}

	return &workerpb.ListQueuesResponse{}, nil
}

func (m *mockAdminClient) GetQueue(
	ctx context.Context,
	in *workerpb.GetQueueRequest,
	_ ...grpc.CallOption,
) (*workerpb.GetQueueResponse, error) {
	m.lastCtx = ctx
	if m.GetQueueFn != nil {
		return m.GetQueueFn(ctx, in)
	}

	return &workerpb.GetQueueResponse{Queue: &workerpb.QueueSummary{}}, nil
}

func (m *mockAdminClient) UpdateQueueWeight(
	ctx context.Context,
	in *workerpb.UpdateQueueWeightRequest,
	_ ...grpc.CallOption,
) (*workerpb.UpdateQueueWeightResponse, error) {
	m.lastCtx = ctx
	if m.UpdateQueueWeightFn != nil {
		return m.UpdateQueueWeightFn(ctx, in)
	}

	return &workerpb.UpdateQueueWeightResponse{Queue: &workerpb.QueueSummary{}}, nil
}

func (m *mockAdminClient) ResetQueueWeight(
	ctx context.Context,
	in *workerpb.ResetQueueWeightRequest,
	_ ...grpc.CallOption,
) (*workerpb.ResetQueueWeightResponse, error) {
	m.lastCtx = ctx
	if m.ResetQueueWeightFn != nil {
		return m.ResetQueueWeightFn(ctx, in)
	}

	return &workerpb.ResetQueueWeightResponse{Queue: &workerpb.QueueSummary{}}, nil
}

func (m *mockAdminClient) PauseQueue(
	ctx context.Context,
	in *workerpb.PauseQueueRequest,
	_ ...grpc.CallOption,
) (*workerpb.PauseQueueResponse, error) {
	m.lastCtx = ctx
	if m.PauseQueueFn != nil {
		return m.PauseQueueFn(ctx, in)
	}

	return &workerpb.PauseQueueResponse{Queue: &workerpb.QueueSummary{}}, nil
}

func (m *mockAdminClient) ListJobs(
	ctx context.Context,
	in *workerpb.ListJobsRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListJobsResponse, error) {
	m.lastCtx = ctx
	if m.ListJobsFn != nil {
		return m.ListJobsFn(ctx, in)
	}

	return &workerpb.ListJobsResponse{}, nil
}

func (m *mockAdminClient) ListJobEvents(
	ctx context.Context,
	in *workerpb.ListJobEventsRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListJobEventsResponse, error) {
	m.lastCtx = ctx
	if m.ListJobEventsFn != nil {
		return m.ListJobEventsFn(ctx, in)
	}

	return &workerpb.ListJobEventsResponse{}, nil
}

func (m *mockAdminClient) ListAuditEvents(
	ctx context.Context,
	in *workerpb.ListAuditEventsRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListAuditEventsResponse, error) {
	m.lastCtx = ctx
	if m.ListAuditEventsFn != nil {
		return m.ListAuditEventsFn(ctx, in)
	}

	return &workerpb.ListAuditEventsResponse{}, nil
}

func (m *mockAdminClient) GetJob(ctx context.Context, in *workerpb.GetJobRequest, _ ...grpc.CallOption) (*workerpb.GetJobResponse, error) {
	m.lastCtx = ctx
	if m.GetJobFn != nil {
		return m.GetJobFn(ctx, in)
	}

	return &workerpb.GetJobResponse{Job: &workerpb.Job{}}, nil
}

func (m *mockAdminClient) UpsertJob(
	ctx context.Context,
	in *workerpb.UpsertJobRequest,
	_ ...grpc.CallOption,
) (*workerpb.UpsertJobResponse, error) {
	m.lastCtx = ctx
	if m.UpsertJobFn != nil {
		return m.UpsertJobFn(ctx, in)
	}

	return &workerpb.UpsertJobResponse{Job: &workerpb.Job{}}, nil
}

func (m *mockAdminClient) DeleteJob(
	ctx context.Context,
	in *workerpb.DeleteJobRequest,
	_ ...grpc.CallOption,
) (*workerpb.DeleteJobResponse, error) {
	m.lastCtx = ctx
	if m.DeleteJobFn != nil {
		return m.DeleteJobFn(ctx, in)
	}

	return &workerpb.DeleteJobResponse{Deleted: true}, nil
}

func (m *mockAdminClient) RunJob(ctx context.Context, in *workerpb.RunJobRequest, _ ...grpc.CallOption) (*workerpb.RunJobResponse, error) {
	m.lastCtx = ctx
	if m.RunJobFn != nil {
		return m.RunJobFn(ctx, in)
	}

	return &workerpb.RunJobResponse{}, nil
}

func (m *mockAdminClient) ListScheduleFactories(
	ctx context.Context,
	in *workerpb.ListScheduleFactoriesRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListScheduleFactoriesResponse, error) {
	m.lastCtx = ctx
	if m.ListScheduleFactoriesFn != nil {
		return m.ListScheduleFactoriesFn(ctx, in)
	}

	return &workerpb.ListScheduleFactoriesResponse{}, nil
}

func (m *mockAdminClient) ListScheduleEvents(
	ctx context.Context,
	in *workerpb.ListScheduleEventsRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListScheduleEventsResponse, error) {
	m.lastCtx = ctx
	if m.ListScheduleEventsFn != nil {
		return m.ListScheduleEventsFn(ctx, in)
	}

	return &workerpb.ListScheduleEventsResponse{}, nil
}

func (m *mockAdminClient) ListSchedules(
	ctx context.Context,
	in *workerpb.ListSchedulesRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListSchedulesResponse, error) {
	m.lastCtx = ctx
	if m.ListSchedulesFn != nil {
		return m.ListSchedulesFn(ctx, in)
	}

	return &workerpb.ListSchedulesResponse{}, nil
}

func (m *mockAdminClient) CreateSchedule(
	ctx context.Context,
	in *workerpb.CreateScheduleRequest,
	_ ...grpc.CallOption,
) (*workerpb.CreateScheduleResponse, error) {
	m.lastCtx = ctx
	if m.CreateScheduleFn != nil {
		return m.CreateScheduleFn(ctx, in)
	}

	return &workerpb.CreateScheduleResponse{Schedule: &workerpb.ScheduleEntry{}}, nil
}

func (m *mockAdminClient) DeleteSchedule(
	ctx context.Context,
	in *workerpb.DeleteScheduleRequest,
	_ ...grpc.CallOption,
) (*workerpb.DeleteScheduleResponse, error) {
	m.lastCtx = ctx
	if m.DeleteScheduleFn != nil {
		return m.DeleteScheduleFn(ctx, in)
	}

	return &workerpb.DeleteScheduleResponse{Deleted: true}, nil
}

func (m *mockAdminClient) PauseSchedule(
	ctx context.Context,
	in *workerpb.PauseScheduleRequest,
	_ ...grpc.CallOption,
) (*workerpb.PauseScheduleResponse, error) {
	m.lastCtx = ctx
	if m.PauseScheduleFn != nil {
		return m.PauseScheduleFn(ctx, in)
	}

	return &workerpb.PauseScheduleResponse{Schedule: &workerpb.ScheduleEntry{}}, nil
}

func (m *mockAdminClient) RunSchedule(
	ctx context.Context,
	in *workerpb.RunScheduleRequest,
	_ ...grpc.CallOption,
) (*workerpb.RunScheduleResponse, error) {
	m.lastCtx = ctx
	if m.RunScheduleFn != nil {
		return m.RunScheduleFn(ctx, in)
	}

	return &workerpb.RunScheduleResponse{}, nil
}

func (m *mockAdminClient) PauseSchedules(
	ctx context.Context,
	in *workerpb.PauseSchedulesRequest,
	_ ...grpc.CallOption,
) (*workerpb.PauseSchedulesResponse, error) {
	m.lastCtx = ctx
	if m.PauseSchedulesFn != nil {
		return m.PauseSchedulesFn(ctx, in)
	}

	return &workerpb.PauseSchedulesResponse{}, nil
}

func (m *mockAdminClient) ListDLQ(
	ctx context.Context,
	in *workerpb.ListDLQRequest,
	_ ...grpc.CallOption,
) (*workerpb.ListDLQResponse, error) {
	m.lastCtx = ctx
	if m.ListDLQFn != nil {
		return m.ListDLQFn(ctx, in)
	}

	return &workerpb.ListDLQResponse{}, nil
}

func (m *mockAdminClient) GetDLQEntry(
	ctx context.Context,
	in *workerpb.GetDLQEntryRequest,
	_ ...grpc.CallOption,
) (*workerpb.GetDLQEntryResponse, error) {
	m.lastCtx = ctx
	if m.GetDLQEntryFn != nil {
		return m.GetDLQEntryFn(ctx, in)
	}

	return &workerpb.GetDLQEntryResponse{Entry: &workerpb.DLQEntryDetail{}}, nil
}

func (m *mockAdminClient) PauseDequeue(
	ctx context.Context,
	in *workerpb.PauseDequeueRequest,
	_ ...grpc.CallOption,
) (*workerpb.PauseDequeueResponse, error) {
	m.lastCtx = ctx
	if m.PauseDequeueFn != nil {
		return m.PauseDequeueFn(ctx, in)
	}

	return &workerpb.PauseDequeueResponse{Paused: true}, nil
}

func (m *mockAdminClient) ResumeDequeue(
	ctx context.Context,
	in *workerpb.ResumeDequeueRequest,
	_ ...grpc.CallOption,
) (*workerpb.ResumeDequeueResponse, error) {
	m.lastCtx = ctx
	if m.ResumeDequeueFn != nil {
		return m.ResumeDequeueFn(ctx, in)
	}

	return &workerpb.ResumeDequeueResponse{Paused: false}, nil
}

func (m *mockAdminClient) ReplayDLQ(
	ctx context.Context,
	in *workerpb.ReplayDLQRequest,
	_ ...grpc.CallOption,
) (*workerpb.ReplayDLQResponse, error) {
	m.lastCtx = ctx
	if m.ReplayDLQFn != nil {
		return m.ReplayDLQFn(ctx, in)
	}

	return &workerpb.ReplayDLQResponse{}, nil
}

func (m *mockAdminClient) ReplayDLQByID(
	ctx context.Context,
	in *workerpb.ReplayDLQByIDRequest,
	_ ...grpc.CallOption,
) (*workerpb.ReplayDLQByIDResponse, error) {
	m.lastCtx = ctx
	if m.ReplayDLQByIDFn != nil {
		return m.ReplayDLQByIDFn(ctx, in)
	}

	return &workerpb.ReplayDLQByIDResponse{}, nil
}

// newTestGateway builds an httptest.Server wrapping the admin gateway mux backed by the
// supplied mock. Server close is registered via t.Cleanup.
func newTestGateway(t *testing.T, client *mockAdminClient) *httptest.Server {
	t.Helper()

	handler := newAdminGatewayHandler(client, nil, "", 0)
	srv := httptest.NewServer(newAdminGatewayMux(handler))
	t.Cleanup(srv.Close)

	return srv
}

// testResponse is a captured HTTP response with the body already drained. Using a
// value type lets tests avoid the bodyclose lint false positive that fires whenever
// an *http.Response is passed around without a lexically local .Body.Close() call.
type testResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func doRequest(t *testing.T, method, url string, body []byte, headers map[string]string) testResponse {
	t.Helper()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, url, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			t.Logf("close resp body: %v", closeErr)
		}
	}()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read resp body: %v", err)
	}

	return testResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: data}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return b
}

func decodeJSON[T any](t *testing.T, resp testResponse) T {
	t.Helper()

	var out T

	err := json.Unmarshal(resp.Body, &out)
	if err != nil {
		t.Fatalf("decode body %q: %v", string(resp.Body), err)
	}

	return out
}

func mustHeaderMetadata(ctx context.Context, t *testing.T, key string) []string {
	t.Helper()

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("no outgoing metadata in context")
	}

	return md.Get(key)
}

// =====================================================================
// Health + Overview + Metrics
// =====================================================================

func TestAdminGateway_Health_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		GetHealthFn: func(_ context.Context, _ *workerpb.GetHealthRequest) (*workerpb.GetHealthResponse, error) {
			return &workerpb.GetHealthResponse{
				Status:    "ok",
				Version:   "1.2.3",
				Commit:    "abc123",
				BuildTime: "2026-04-19T00:00:00Z",
				GoVersion: "go1.26",
			}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminHealthJSON](t, resp)
	if payload.Status != "ok" || payload.Version != "1.2.3" || payload.Commit != "abc123" {
		t.Fatalf(testErrUnexpected, payload)
	}
}

func TestAdminGateway_Health_WrongMethod(t *testing.T) {
	t.Parallel()

	srv := newTestGateway(t, &mockAdminClient{})
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/health", nil, nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}

	env := decodeJSON[adminErrorEnvelope](t, resp)
	if env.Error.Code != adminErrMethodBlocked {
		t.Fatalf("error code = %q, want %q", env.Error.Code, adminErrMethodBlocked)
	}

	if env.RequestID == "" {
		t.Fatal("missing request ID in error envelope")
	}
}

func TestAdminGateway_Health_GRPCError(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		GetHealthFn: func(_ context.Context, _ *workerpb.GetHealthRequest) (*workerpb.GetHealthResponse, error) {
			return nil, status.Error(codes.Unavailable, "backend down")
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestAdminGateway_Overview_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		GetOverviewFn: func(_ context.Context, _ *workerpb.GetOverviewRequest) (*workerpb.GetOverviewResponse, error) {
			return &workerpb.GetOverviewResponse{
				Stats: &workerpb.OverviewStats{
					ActiveWorkers: testOverviewStats1,
					QueuedTasks:   testOverviewStats2,
					Queues:        testOverviewStats3,
					AvgLatencyMs:  testOverviewStats4,
					P95LatencyMs:  testOverviewStats5,
				},
				Coordination: &workerpb.CoordinationStatus{Paused: true},
				Actions:      &workerpb.AdminActionCounters{Pause: 1, Resume: 2, Replay: testActionReplay},
			}, nil
		},
	}

	srv := newTestGateway(t, mock)

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/overview", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminOverviewJSON](t, resp)
	if payload.Stats.ActiveWorkers != testOverviewStats1 || payload.Stats.QueuedTasks != testOverviewStats2 {
		t.Fatalf("unexpected stats: %+v", payload.Stats)
	}

	if !payload.Coordination.Paused {
		t.Fatal("expected coordination.paused=true")
	}

	if payload.Actions.Replay != testActionReplay {
		t.Fatalf("actions.replay = %d, want %d", payload.Actions.Replay, testActionReplay)
	}
}

// =====================================================================
// Queues
// =====================================================================

func TestAdminGateway_ListQueues_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		ListQueuesFn: func(_ context.Context, _ *workerpb.ListQueuesRequest) (*workerpb.ListQueuesResponse, error) {
			return &workerpb.ListQueuesResponse{
				Queues: []*workerpb.QueueSummary{
					{Name: "default", Ready: 10, Weight: 1},
					{Name: "priority", Ready: testOverviewStats3, Weight: 10, Paused: true},
				},
			}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/queues", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminQueuesJSON](t, resp)
	if len(payload.Queues) != 2 {
		t.Fatalf("queues length = %d, want 2", len(payload.Queues))
	}

	if payload.Queues[1].Name != "priority" || !payload.Queues[1].Paused {
		t.Fatalf("unexpected queue[1]: %+v", payload.Queues[1])
	}
}

func TestAdminGateway_GetQueue_DecodesPath(t *testing.T) {
	t.Parallel()

	var gotName string

	mock := &mockAdminClient{
		GetQueueFn: func(_ context.Context, in *workerpb.GetQueueRequest) (*workerpb.GetQueueResponse, error) {
			gotName = in.GetName()

			return &workerpb.GetQueueResponse{Queue: &workerpb.QueueSummary{Name: in.GetName()}}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	// URL-encoded queue name "my queue"
	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/queues/my%20queue", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotName != "my queue" {
		t.Fatalf("backend received name %q, want %q", gotName, "my queue")
	}
}

func TestAdminGateway_GetQueue_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestGateway(t, &mockAdminClient{})
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/queues/", nil, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	env := decodeJSON[adminErrorEnvelope](t, resp)
	if env.Error.Code != adminErrMissingQueue {
		t.Fatalf("error code = %q, want %q", env.Error.Code, adminErrMissingQueue)
	}
}

func TestAdminGateway_PauseQueue_PropagatesApprovalHeader(t *testing.T) {
	t.Parallel()

	var gotReq *workerpb.PauseQueueRequest

	mock := &mockAdminClient{
		PauseQueueFn: func(_ context.Context, in *workerpb.PauseQueueRequest) (*workerpb.PauseQueueResponse, error) {
			gotReq = in

			return &workerpb.PauseQueueResponse{
				Queue: &workerpb.QueueSummary{Name: in.GetName(), Paused: in.GetPaused()},
			}, nil
		},
	}

	srv := newTestGateway(t, mock)

	body := mustMarshal(t, adminQueuePauseRequest{Paused: true})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/queues/default/pause", body, map[string]string{
		adminApprovalHeader: "operator-42",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotReq.GetName() != testQueueDefault || !gotReq.GetPaused() {
		t.Fatalf("request not propagated: %+v", gotReq)
	}

	approvalValues := mustHeaderMetadata(mock.lastCtx, t, adminApprovalMetaKey)
	if len(approvalValues) != 1 || approvalValues[0] != "operator-42" {
		t.Fatalf("approval metadata = %v, want [operator-42]", approvalValues)
	}
}

func TestAdminGateway_QueueWeight_UpdateAndReset(t *testing.T) {
	t.Parallel()

	var updatedWeight int32

	mock := &mockAdminClient{
		UpdateQueueWeightFn: func(_ context.Context, in *workerpb.UpdateQueueWeightRequest) (*workerpb.UpdateQueueWeightResponse, error) {
			updatedWeight = in.GetWeight()

			return &workerpb.UpdateQueueWeightResponse{Queue: &workerpb.QueueSummary{Name: in.GetName(), Weight: in.GetWeight()}}, nil
		},
		ResetQueueWeightFn: func(_ context.Context, _ *workerpb.ResetQueueWeightRequest) (*workerpb.ResetQueueWeightResponse, error) {
			return &workerpb.ResetQueueWeightResponse{Queue: &workerpb.QueueSummary{Name: "default", Weight: 1}}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	body := mustMarshal(t, adminQueueWeightRequest{Weight: testWeightUpdate})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/queues/default/weight", body, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, want 200", resp.StatusCode)
	}

	if updatedWeight != testWeightUpdate {
		t.Fatalf("backend received weight %d, want %d", updatedWeight, testWeightUpdate)
	}

	resp = doRequest(t, http.MethodDelete, srv.URL+"/admin/v1/queues/default/weight", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200", resp.StatusCode)
	}
}

// =====================================================================
// Pause / Resume / DLQ
// =====================================================================

func TestAdminGateway_Pause_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		PauseDequeueFn: func(_ context.Context, _ *workerpb.PauseDequeueRequest) (*workerpb.PauseDequeueResponse, error) {
			return &workerpb.PauseDequeueResponse{Paused: true}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/pause", nil, map[string]string{
		adminApprovalHeader: "ops",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminPauseJSON](t, resp)
	if !payload.Paused {
		t.Fatal("expected paused=true")
	}
}

func TestAdminGateway_Resume_WrongMethod(t *testing.T) {
	t.Parallel()

	srv := newTestGateway(t, &mockAdminClient{})
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/resume", nil, nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestAdminGateway_ListDLQ_PassesFilters(t *testing.T) {
	t.Parallel()

	var got *workerpb.ListDLQRequest

	mock := &mockAdminClient{
		ListDLQFn: func(_ context.Context, in *workerpb.ListDLQRequest) (*workerpb.ListDLQResponse, error) {
			got = in

			return &workerpb.ListDLQResponse{
				Entries: []*workerpb.DLQEntry{{Id: "abc", Queue: testQueueDefault, Handler: "h1", Attempts: testAttemptsCount}},
				Total:   1,
			}, nil
		},
	}

	srv := newTestGateway(t, mock)

	url := srv.URL + "/admin/v1/dlq?limit=50&offset=10&queue=default&handler=h1&query=boom"

	resp := doRequest(t, http.MethodGet, url, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if got.GetLimit() != testDLQLimit || got.GetOffset() != testDLQOffset {
		t.Fatalf("limit/offset = %d/%d, want %d/%d", got.GetLimit(), got.GetOffset(), testDLQLimit, testDLQOffset)
	}

	if got.GetQueue() != testQueueDefault || got.GetHandler() != "h1" || got.GetQuery() != "boom" {
		t.Fatalf("filters not propagated: %+v", got)
	}

	payload := decodeJSON[adminDLQJSON](t, resp)
	if payload.Total != 1 || len(payload.Entries) != 1 || payload.Entries[0].ID != "abc" {
		t.Fatalf(testErrUnexpected, payload)
	}
}

func TestAdminGateway_GetDLQEntry_MissingID(t *testing.T) {
	t.Parallel()

	srv := newTestGateway(t, &mockAdminClient{})
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/dlq/", nil, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	env := decodeJSON[adminErrorEnvelope](t, resp)
	if env.Error.Code != "missing_dlq_id" {
		t.Fatalf("error code = %q, want missing_dlq_id", env.Error.Code)
	}
}

func TestAdminGateway_Replay_UsesDefaultLimitWhenBodyEmpty(t *testing.T) {
	t.Parallel()

	var gotLimit int32

	mock := &mockAdminClient{
		ReplayDLQFn: func(_ context.Context, in *workerpb.ReplayDLQRequest) (*workerpb.ReplayDLQResponse, error) {
			gotLimit = in.GetLimit()

			return &workerpb.ReplayDLQResponse{Moved: testMovedCount}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/dlq/replay", nil, map[string]string{
		adminApprovalHeader: "op",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotLimit <= 0 {
		t.Fatalf("expected a positive default limit, got %d", gotLimit)
	}

	payload := decodeJSON[adminReplayJSON](t, resp)
	if payload.Moved != testMovedCount {
		t.Fatalf("moved = %d, want %d", payload.Moved, testMovedCount)
	}

	approvalValues := mustHeaderMetadata(mock.lastCtx, t, adminApprovalMetaKey)
	if len(approvalValues) == 0 {
		t.Fatal("approval header not propagated to backend")
	}
}

func TestAdminGateway_ReplayIDs_PropagatesIDsAndApproval(t *testing.T) {
	t.Parallel()

	var gotIDs []string

	mock := &mockAdminClient{
		ReplayDLQByIDFn: func(_ context.Context, in *workerpb.ReplayDLQByIDRequest) (*workerpb.ReplayDLQByIDResponse, error) {
			gotIDs = in.GetIds()

			return &workerpb.ReplayDLQByIDResponse{}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	body := mustMarshal(t, struct {
		IDs []string `json:"ids"`
	}{IDs: []string{"a", "b", "c"}})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/dlq/replay/ids", body, map[string]string{
		adminApprovalHeader: "approver",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if len(gotIDs) != testEntriesCount || gotIDs[2] != "c" {
		t.Fatalf("ids not propagated: %v", gotIDs)
	}
}

// =====================================================================
// Schedules
// =====================================================================

func TestAdminGateway_ListSchedules_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		ListSchedulesFn: func(_ context.Context, _ *workerpb.ListSchedulesRequest) (*workerpb.ListSchedulesResponse, error) {
			return &workerpb.ListSchedulesResponse{
				Schedules: []*workerpb.ScheduleEntry{{Name: "nightly", Spec: "0 0 * * *"}},
			}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/schedules", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminSchedulesJSON](t, resp)
	if len(payload.Schedules) != 1 || payload.Schedules[0].Name != "nightly" {
		t.Fatalf(testErrUnexpected, payload)
	}
}

func TestAdminGateway_CreateSchedule_ValidatesBody(t *testing.T) {
	t.Parallel()

	srv := newTestGateway(t, &mockAdminClient{})
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/schedules", []byte("not json"), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	env := decodeJSON[adminErrorEnvelope](t, resp)
	if env.Error.Code != adminErrInvalidJSON {
		t.Fatalf("error code = %q, want %q", env.Error.Code, adminErrInvalidJSON)
	}
}

func TestAdminGateway_CreateSchedule_Success(t *testing.T) {
	t.Parallel()

	var gotReq *workerpb.CreateScheduleRequest

	mock := &mockAdminClient{
		CreateScheduleFn: func(_ context.Context, in *workerpb.CreateScheduleRequest) (*workerpb.CreateScheduleResponse, error) {
			gotReq = in

			return &workerpb.CreateScheduleResponse{Schedule: &workerpb.ScheduleEntry{Name: in.GetName(), Spec: in.GetSpec()}}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	body := mustMarshal(t, map[string]any{"name": testScheduleName1, "spec": testScheduleCron, "durable": true})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/schedules", body, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotReq.GetName() != "nightly" || !gotReq.GetDurable() {
		t.Fatalf("request not propagated: %+v", gotReq)
	}
}

func TestAdminGateway_SchedulesPause_PropagatesApproval(t *testing.T) {
	t.Parallel()

	var gotReq *workerpb.PauseSchedulesRequest

	mock := &mockAdminClient{
		PauseSchedulesFn: func(_ context.Context, in *workerpb.PauseSchedulesRequest) (*workerpb.PauseSchedulesResponse, error) {
			gotReq = in

			return &workerpb.PauseSchedulesResponse{Updated: testEntriesCount, Paused: in.GetPaused()}, nil
		},
	}

	srv := newTestGateway(t, mock)

	body := mustMarshal(t, map[string]bool{"paused": false})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/schedules/pause", body, map[string]string{
		adminApprovalHeader: "ops",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotReq == nil || gotReq.GetPaused() {
		t.Fatalf("paused=false not propagated: %+v", gotReq)
	}

	approvalValues := mustHeaderMetadata(mock.lastCtx, t, adminApprovalMetaKey)
	if len(approvalValues) == 0 {
		t.Fatal("approval header not propagated")
	}
}

func TestAdminGateway_SchedulePause_PropagatesApproval(t *testing.T) {
	t.Parallel()

	var gotReq *workerpb.PauseScheduleRequest

	mock := &mockAdminClient{
		PauseScheduleFn: func(_ context.Context, in *workerpb.PauseScheduleRequest) (*workerpb.PauseScheduleResponse, error) {
			gotReq = in

			return &workerpb.PauseScheduleResponse{
				Schedule: &workerpb.ScheduleEntry{Name: in.GetName(), Paused: in.GetPaused()},
			}, nil
		},
	}

	srv := newTestGateway(t, mock)

	body := mustMarshal(t, adminSchedulePauseRequest{Paused: true})

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/schedules/nightly/pause", body, map[string]string{
		adminApprovalHeader: "ops",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotReq == nil || gotReq.GetName() != testScheduleName1 || !gotReq.GetPaused() {
		t.Fatalf("request not propagated: %+v", gotReq)
	}

	approvalValues := mustHeaderMetadata(mock.lastCtx, t, adminApprovalMetaKey)
	if len(approvalValues) == 0 {
		t.Fatal("approval header not propagated")
	}
}

// =====================================================================
// Jobs
// =====================================================================

func TestAdminGateway_ListJobs_Success(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		ListJobsFn: func(_ context.Context, _ *workerpb.ListJobsRequest) (*workerpb.ListJobsResponse, error) {
			return &workerpb.ListJobsResponse{Jobs: []*workerpb.Job{{Spec: &workerpb.JobSpec{Name: "backup"}}}}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/jobs", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminJobsJSON](t, resp)
	if len(payload.Jobs) != 1 || payload.Jobs[0].Name != "backup" {
		t.Fatalf(testErrUnexpected, payload)
	}
}

func TestAdminGateway_GetJob_DecodesPath(t *testing.T) {
	t.Parallel()

	var gotName string

	mock := &mockAdminClient{
		GetJobFn: func(_ context.Context, in *workerpb.GetJobRequest) (*workerpb.GetJobResponse, error) {
			gotName = in.GetName()

			return &workerpb.GetJobResponse{Job: &workerpb.Job{Spec: &workerpb.JobSpec{Name: in.GetName()}}}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/jobs/my%20job", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if gotName != "my job" {
		t.Fatalf("got job name %q, want %q", gotName, "my job")
	}
}

func TestAdminGateway_RunJob_PropagatesApproval(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{
		RunJobFn: func(_ context.Context, _ *workerpb.RunJobRequest) (*workerpb.RunJobResponse, error) {
			return &workerpb.RunJobResponse{TaskId: "task-99"}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodPost, srv.URL+"/admin/v1/jobs/backup/run", nil, map[string]string{
		adminApprovalHeader: "ops",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	payload := decodeJSON[adminJobRunResponse](t, resp)
	if payload.TaskID != "task-99" {
		t.Fatalf("taskId = %q, want task-99", payload.TaskID)
	}

	approvalValues := mustHeaderMetadata(mock.lastCtx, t, adminApprovalMetaKey)
	if len(approvalValues) == 0 {
		t.Fatal("approval header not propagated")
	}
}

// =====================================================================
// Audit / Events
// =====================================================================

func TestAdminGateway_ListAudit_PassesFilters(t *testing.T) {
	t.Parallel()

	var got *workerpb.ListAuditEventsRequest

	mock := &mockAdminClient{
		ListAuditEventsFn: func(_ context.Context, in *workerpb.ListAuditEventsRequest) (*workerpb.ListAuditEventsResponse, error) {
			got = in

			return &workerpb.ListAuditEventsResponse{}, nil
		},
	}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	url := srv.URL + "/admin/v1/audit?limit=20&action=pause&target=queue%3Adefault"

	resp := doRequest(t, http.MethodGet, url, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if got.GetLimit() != testAuditLimit || got.GetAction() != "pause" || got.GetTarget() != "queue:default" {
		t.Fatalf("filters not propagated: %+v", got)
	}
}

// =====================================================================
// Request-ID handling
// =====================================================================

func TestAdminGateway_RequestID_PropagatedAndEchoed(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, map[string]string{
		adminRequestIDHeader: "req-abc123",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if got := resp.Header.Get(adminRequestIDHeader); got != "req-abc123" {
		t.Fatalf("response request ID = %q, want req-abc123", got)
	}

	reqIDValues := mustHeaderMetadata(mock.lastCtx, t, adminRequestIDMetaKey)
	if len(reqIDValues) != 1 || reqIDValues[0] != "req-abc123" {
		t.Fatalf("gRPC metadata request ID = %v, want [req-abc123]", reqIDValues)
	}
}

func TestAdminGateway_RequestID_StripsDisallowedChars(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, map[string]string{
		adminRequestIDHeader: "abc!@#def ghi",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	echoed := resp.Header.Get(adminRequestIDHeader)
	if strings.ContainsAny(echoed, "!@# ") {
		t.Fatalf("request ID not sanitized: %q", echoed)
	}

	if echoed != "abcdefghi" {
		t.Fatalf("sanitized request ID = %q, want abcdefghi", echoed)
	}
}

func TestAdminGateway_RequestID_GeneratedWhenMissing(t *testing.T) {
	t.Parallel()

	mock := &mockAdminClient{}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf(testErrStatus, resp.StatusCode)
	}

	if got := resp.Header.Get(adminRequestIDHeader); got == "" {
		t.Fatal("expected a generated request ID, got empty header")
	}
}

func TestAdminGateway_RequestID_TruncatedAtMaxLength(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", adminRequestIDMaxLen+50)

	mock := &mockAdminClient{}

	srv := newTestGateway(t, mock)
	defer srv.Close()

	resp := doRequest(t, http.MethodGet, srv.URL+"/admin/v1/health", nil, map[string]string{
		adminRequestIDHeader: long,
	})

	echoed := resp.Header.Get(adminRequestIDHeader)
	if len(echoed) != adminRequestIDMaxLen {
		t.Fatalf("request ID length = %d, want %d", len(echoed), adminRequestIDMaxLen)
	}
}

// =====================================================================
// Helpers (unit tests for sanitization, parsing)
// =====================================================================

func TestSanitizeRequestID(t *testing.T) {
	t.Parallel()

	const (
		alnum     = testSampleID
		stripped  = "abc"
		dashes    = "req-abc-123"
		underscrs = "req_abc_123"
		dots      = "req.abc.123"
	)

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"alphanumeric", alnum, alnum},
		{"with dashes", dashes, dashes},
		{"with underscores", underscrs, underscrs},
		{"with dots", dots, dots},
		{"strips spaces", "a b c", stripped},
		{"strips symbols", "a!@#$%^&*()b", "ab"},
		{"empty input", "", ""},
		{"only spaces", "   ", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := sanitizeRequestID(tc.in); got != tc.want {
				t.Fatalf("sanitizeRequestID(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestGRPCCodeToHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, http.StatusOK},
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.NotFound, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.ResourceExhausted, http.StatusTooManyRequests},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Internal, http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.code.String(), func(t *testing.T) {
			t.Parallel()

			if got := grpcCodeToHTTP(tc.code); got != tc.want {
				t.Fatalf("grpcCodeToHTTP(%s) = %d, want %d", tc.code, got, tc.want)
			}
		})
	}
}

func TestParseIntParam(t *testing.T) {
	t.Parallel()

	const defaultLimit = 10

	tests := []struct {
		in       string
		fallback int
		want     int
	}{
		{"", defaultLimit, defaultLimit},
		{"42", defaultLimit, testWeightUpdate},
		{"-5", defaultLimit, defaultLimit},
		{"0", defaultLimit, defaultLimit},
		{"abc", defaultLimit, defaultLimit},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			if got := parseIntParam(tc.in, tc.fallback); got != tc.want {
				t.Fatalf("parseIntParam(%q, %d) = %d, want %d", tc.in, tc.fallback, got, tc.want)
			}
		})
	}
}
