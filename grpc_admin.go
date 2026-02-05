package worker

import (
	"context"
	"errors"

	"github.com/hyp3rd/ewrap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const defaultAdminDLQLimit = 100

func (s *GRPCServer) adminBackend() (AdminBackend, error) {
	if s == nil || s.admin == nil {
		return nil, ErrAdminBackendUnavailable
	}

	return s.admin, nil
}

// GetOverview returns the admin overview snapshot.
func (s *GRPCServer) GetOverview(ctx context.Context, req *workerpb.GetOverviewRequest) (*workerpb.GetOverviewResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_GetOverview_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	overview, err := backend.AdminOverview(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	stats := &workerpb.OverviewStats{
		ActiveWorkers: clampInt32(overview.ActiveWorkers),
		QueuedTasks:   overview.QueuedTasks,
		Queues:        clampInt32(overview.Queues),
		AvgLatencyMs:  overview.AvgLatencyMs,
		P95LatencyMs:  overview.P95LatencyMs,
	}

	coordination := &workerpb.CoordinationStatus{
		GlobalRateLimit: overview.Coordination.GlobalRateLimit,
		LeaderLock:      overview.Coordination.LeaderLock,
		Lease:           overview.Coordination.Lease,
		Paused:          overview.Coordination.Paused,
	}

	return &workerpb.GetOverviewResponse{
		Stats:        stats,
		Coordination: coordination,
	}, nil
}

// ListQueues returns queue summaries.
func (s *GRPCServer) ListQueues(ctx context.Context, req *workerpb.ListQueuesRequest) (*workerpb.ListQueuesResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListQueues_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	queues, err := backend.AdminQueues(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListQueuesResponse{
		Queues: make([]*workerpb.QueueSummary, 0, len(queues)),
	}

	for _, queue := range queues {
		resp.Queues = append(resp.Queues, &workerpb.QueueSummary{
			Name:       queue.Name,
			Ready:      queue.Ready,
			Processing: queue.Processing,
			Dead:       queue.Dead,
			Weight:     clampInt32(queue.Weight),
		})
	}

	return resp, nil
}

// ListDLQ returns DLQ entries.
func (s *GRPCServer) ListDLQ(ctx context.Context, req *workerpb.ListDLQRequest) (*workerpb.ListDLQResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListDLQ_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultAdminDLQLimit
	}

	entries, err := backend.AdminDLQ(ctx, limit)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListDLQResponse{
		Entries: make([]*workerpb.DLQEntry, 0, len(entries)),
	}

	for _, entry := range entries {
		resp.Entries = append(resp.Entries, &workerpb.DLQEntry{
			Id:       entry.ID,
			Queue:    entry.Queue,
			Handler:  entry.Handler,
			Attempts: clampInt32(entry.Attempts),
			AgeMs:    entry.AgeMs,
		})
	}

	return resp, nil
}

// PauseDequeue pauses durable dequeue.
func (s *GRPCServer) PauseDequeue(ctx context.Context, req *workerpb.PauseDequeueRequest) (*workerpb.PauseDequeueResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_PauseDequeue_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	err = backend.AdminPause(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.PauseDequeueResponse{Paused: true}, nil
}

// ResumeDequeue resumes durable dequeue.
func (s *GRPCServer) ResumeDequeue(ctx context.Context, req *workerpb.ResumeDequeueRequest) (*workerpb.ResumeDequeueResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ResumeDequeue_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	err = backend.AdminResume(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.ResumeDequeueResponse{Paused: false}, nil
}

// ReplayDLQ replays DLQ entries.
func (s *GRPCServer) ReplayDLQ(ctx context.Context, req *workerpb.ReplayDLQRequest) (*workerpb.ReplayDLQResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ReplayDLQ_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultAdminDLQLimit
	}

	moved, err := backend.AdminReplayDLQ(ctx, limit)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.ReplayDLQResponse{Moved: clampInt32(moved)}, nil
}

func clampInt32(value int) int32 {
	if value > int(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}

	if value < -int(^uint32(0)>>1)-1 {
		return int32(-int(^uint32(0)>>1) - 1)
	}

	return int32(value)
}

func toAdminStatus(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrAdminBackendUnavailable) || errors.Is(err, ErrAdminUnsupported) {
		return ewrap.Wrap(status.Error(codes.Unimplemented, err.Error()), "admin backend unavailable or unsupported")
	}

	if errors.Is(err, ErrInvalidTaskContext) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "invalid task context")
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return ewrap.Wrap(status.Error(codes.Internal, err.Error()), "internal error")
}
