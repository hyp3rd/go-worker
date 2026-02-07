package worker

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	sectconv "github.com/hyp3rd/sectools/pkg/converters"
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

// GetHealth returns build and runtime info for the admin service.
func (s *GRPCServer) GetHealth(ctx context.Context, req *workerpb.GetHealthRequest) (*workerpb.GetHealthResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_GetHealth_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	info := readBuildInfo()

	return &workerpb.GetHealthResponse{
		Status:    "ok",
		Version:   info.Version,
		Commit:    info.Commit,
		BuildTime: info.BuildTime,
		GoVersion: info.GoVersion,
	}, nil
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
		Actions: &workerpb.AdminActionCounters{
			Pause:  overview.Actions.Pause,
			Resume: overview.Actions.Resume,
			Replay: overview.Actions.Replay,
		},
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
			Paused:     queue.Paused,
		})
	}

	return resp, nil
}

// GetQueue returns a queue summary by name.
func (s *GRPCServer) GetQueue(ctx context.Context, req *workerpb.GetQueueRequest) (*workerpb.GetQueueResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_GetQueue_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	name := strings.TrimSpace(req.GetName())

	queue, err := backend.AdminQueue(ctx, name)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.GetQueueResponse{
		Queue: &workerpb.QueueSummary{
			Name:       queue.Name,
			Ready:      queue.Ready,
			Processing: queue.Processing,
			Dead:       queue.Dead,
			Weight:     clampInt32(queue.Weight),
			Paused:     queue.Paused,
		},
	}, nil
}

// UpdateQueueWeight updates a queue weight.
func (s *GRPCServer) UpdateQueueWeight(
	ctx context.Context,
	req *workerpb.UpdateQueueWeightRequest,
) (*workerpb.UpdateQueueWeightResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_UpdateQueueWeight_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	name := strings.TrimSpace(req.GetName())
	weight := int(req.GetWeight())

	queue, err := backend.AdminSetQueueWeight(ctx, name, weight)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.UpdateQueueWeightResponse{
		Queue: &workerpb.QueueSummary{
			Name:       queue.Name,
			Ready:      queue.Ready,
			Processing: queue.Processing,
			Dead:       queue.Dead,
			Weight:     clampInt32(queue.Weight),
			Paused:     queue.Paused,
		},
	}, nil
}

// ResetQueueWeight resets a queue weight to default.
func (s *GRPCServer) ResetQueueWeight(
	ctx context.Context,
	req *workerpb.ResetQueueWeightRequest,
) (*workerpb.ResetQueueWeightResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ResetQueueWeight_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	name := strings.TrimSpace(req.GetName())

	queue, err := backend.AdminResetQueueWeight(ctx, name)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.ResetQueueWeightResponse{
		Queue: &workerpb.QueueSummary{
			Name:       queue.Name,
			Ready:      queue.Ready,
			Processing: queue.Processing,
			Dead:       queue.Dead,
			Weight:     clampInt32(queue.Weight),
			Paused:     queue.Paused,
		},
	}, nil
}

// ListJobs returns admin job definitions.
func (s *GRPCServer) ListJobs(ctx context.Context, req *workerpb.ListJobsRequest) (*workerpb.ListJobsResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListJobs_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	jobs, err := backend.AdminJobs(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListJobsResponse{Jobs: make([]*workerpb.Job, 0, len(jobs))}
	for _, job := range jobs {
		resp.Jobs = append(resp.Jobs, jobToProto(job))
	}

	return resp, nil
}

// GetJob returns a job definition by name.
func (s *GRPCServer) GetJob(ctx context.Context, req *workerpb.GetJobRequest) (*workerpb.GetJobResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_GetJob_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	job, err := backend.AdminJob(ctx, strings.TrimSpace(req.GetName()))
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.GetJobResponse{Job: jobToProto(job)}, nil
}

// UpsertJob creates or updates a job definition.
func (s *GRPCServer) UpsertJob(ctx context.Context, req *workerpb.UpsertJobRequest) (*workerpb.UpsertJobResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_UpsertJob_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	spec := jobSpecFromProto(req.GetSpec())

	job, err := backend.AdminUpsertJob(ctx, spec)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.UpsertJobResponse{Job: jobToProto(job)}, nil
}

// DeleteJob removes a job definition.
func (s *GRPCServer) DeleteJob(ctx context.Context, req *workerpb.DeleteJobRequest) (*workerpb.DeleteJobResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_DeleteJob_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	deleted, err := backend.AdminDeleteJob(ctx, strings.TrimSpace(req.GetName()))
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.DeleteJobResponse{Deleted: deleted}, nil
}

// RunJob enqueues a job immediately.
func (s *GRPCServer) RunJob(ctx context.Context, req *workerpb.RunJobRequest) (*workerpb.RunJobResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_RunJob_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	runner, ok := s.admin.(interface {
		AdminRunJob(ctx context.Context, name string) (string, error)
	})
	if !ok {
		return nil, toAdminStatus(ErrAdminUnsupported)
	}

	taskID, err := runner.AdminRunJob(ctx, strings.TrimSpace(req.GetName()))
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.RunJobResponse{TaskId: taskID}, nil
}

// PauseQueue pauses or resumes a queue.
func (s *GRPCServer) PauseQueue(ctx context.Context, req *workerpb.PauseQueueRequest) (*workerpb.PauseQueueResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_PauseQueue_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	queue, err := backend.AdminPauseQueue(ctx, req.GetName(), req.GetPaused())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.PauseQueueResponse{
		Queue: &workerpb.QueueSummary{
			Name:       queue.Name,
			Ready:      queue.Ready,
			Processing: queue.Processing,
			Dead:       queue.Dead,
			Weight:     clampInt32(queue.Weight),
			Paused:     queue.Paused,
		},
	}, nil
}

// ListSchedules returns cron schedule summaries.
func (s *GRPCServer) ListSchedules(ctx context.Context, req *workerpb.ListSchedulesRequest) (*workerpb.ListSchedulesResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListSchedules_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	schedules, err := backend.AdminSchedules(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListSchedulesResponse{
		Schedules: make([]*workerpb.ScheduleEntry, 0, len(schedules)),
	}

	for _, schedule := range schedules {
		resp.Schedules = append(resp.Schedules, scheduleToProto(schedule))
	}

	return resp, nil
}

// ListScheduleFactories returns registered schedule factories.
func (s *GRPCServer) ListScheduleFactories(
	ctx context.Context,
	req *workerpb.ListScheduleFactoriesRequest,
) (*workerpb.ListScheduleFactoriesResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListScheduleFactories_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	factories, err := backend.AdminScheduleFactories(ctx)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListScheduleFactoriesResponse{
		Factories: make([]*workerpb.ScheduleFactory, 0, len(factories)),
	}

	for _, factory := range factories {
		resp.Factories = append(resp.Factories, scheduleFactoryToProto(factory))
	}

	return resp, nil
}

// ListScheduleEvents returns recent cron schedule execution events.
func (s *GRPCServer) ListScheduleEvents(
	ctx context.Context,
	req *workerpb.ListScheduleEventsRequest,
) (*workerpb.ListScheduleEventsResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ListScheduleEvents_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	page, err := backend.AdminScheduleEvents(ctx, AdminScheduleEventFilter{
		Name:  req.GetName(),
		Limit: int(req.GetLimit()),
	})
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListScheduleEventsResponse{
		Events: make([]*workerpb.ScheduleEvent, 0, len(page.Events)),
	}

	for _, event := range page.Events {
		resp.Events = append(resp.Events, scheduleEventToProto(event))
	}

	return resp, nil
}

// CreateSchedule registers or updates a cron schedule.
func (s *GRPCServer) CreateSchedule(ctx context.Context, req *workerpb.CreateScheduleRequest) (*workerpb.CreateScheduleResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_CreateSchedule_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	schedule, err := backend.AdminCreateSchedule(ctx, AdminScheduleSpec{
		Name:    req.GetName(),
		Spec:    req.GetSpec(),
		Durable: req.GetDurable(),
	})
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.CreateScheduleResponse{Schedule: scheduleToProto(schedule)}, nil
}

// DeleteSchedule removes a cron schedule.
func (s *GRPCServer) DeleteSchedule(ctx context.Context, req *workerpb.DeleteScheduleRequest) (*workerpb.DeleteScheduleResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_DeleteSchedule_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	deleted, err := backend.AdminDeleteSchedule(ctx, req.GetName())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.DeleteScheduleResponse{Deleted: deleted}, nil
}

// PauseSchedule pauses or resumes a cron schedule.
func (s *GRPCServer) PauseSchedule(ctx context.Context, req *workerpb.PauseScheduleRequest) (*workerpb.PauseScheduleResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_PauseSchedule_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	schedule, err := backend.AdminPauseSchedule(ctx, req.GetName(), req.GetPaused())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.PauseScheduleResponse{Schedule: scheduleToProto(schedule)}, nil
}

// PauseSchedules pauses or resumes all cron schedules.
func (s *GRPCServer) PauseSchedules(
	ctx context.Context,
	req *workerpb.PauseSchedulesRequest,
) (*workerpb.PauseSchedulesResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_PauseSchedules_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	updated, err := backend.AdminPauseSchedules(ctx, req.GetPaused())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.PauseSchedulesResponse{
		Updated: clampInt32(updated),
		Paused:  req.GetPaused(),
	}, nil
}

// RunSchedule triggers a cron schedule immediately.
func (s *GRPCServer) RunSchedule(ctx context.Context, req *workerpb.RunScheduleRequest) (*workerpb.RunScheduleResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_RunSchedule_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	taskID, err := backend.AdminRunSchedule(ctx, req.GetName())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.RunScheduleResponse{TaskId: taskID}, nil
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

	offset := max(int(req.GetOffset()), 0)

	filter := AdminDLQFilter{
		Limit:   limit,
		Offset:  offset,
		Queue:   strings.TrimSpace(req.GetQueue()),
		Handler: strings.TrimSpace(req.GetHandler()),
		Query:   strings.TrimSpace(req.GetQuery()),
	}

	page, err := backend.AdminDLQ(ctx, filter)
	if err != nil {
		return nil, toAdminStatus(err)
	}

	resp := &workerpb.ListDLQResponse{
		Entries: make([]*workerpb.DLQEntry, 0, len(page.Entries)),
		Total:   page.Total,
	}

	for _, entry := range page.Entries {
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

// GetDLQEntry returns a detailed DLQ entry by id.
func (s *GRPCServer) GetDLQEntry(ctx context.Context, req *workerpb.GetDLQEntryRequest) (*workerpb.GetDLQEntryResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_GetDLQEntry_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	entry, err := backend.AdminDLQEntry(ctx, req.GetId())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.GetDLQEntryResponse{
		Entry: &workerpb.DLQEntryDetail{
			Id:          entry.ID,
			Queue:       entry.Queue,
			Handler:     entry.Handler,
			Attempts:    clampInt32(entry.Attempts),
			AgeMs:       entry.AgeMs,
			FailedAtMs:  entry.FailedAtMs,
			UpdatedAtMs: entry.UpdatedAtMs,
			LastError:   entry.LastError,
			PayloadSize: entry.PayloadSize,
			Metadata:    entry.Metadata,
		},
	}, nil
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

// ReplayDLQByID replays DLQ entries by ID.
func (s *GRPCServer) ReplayDLQByID(ctx context.Context, req *workerpb.ReplayDLQByIDRequest) (*workerpb.ReplayDLQByIDResponse, error) {
	err := s.authorize(ctx, workerpb.AdminService_ReplayDLQByID_FullMethodName, req)
	if err != nil {
		return nil, err
	}

	backend, err := s.adminBackend()
	if err != nil {
		return nil, toAdminStatus(err)
	}

	moved, err := backend.AdminReplayDLQByID(ctx, req.GetIds())
	if err != nil {
		return nil, toAdminStatus(err)
	}

	return &workerpb.ReplayDLQByIDResponse{Moved: clampInt32(moved)}, nil
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

func safeInt32FromInt64(value int64) int32 {
	safe, err := sectconv.SafeInt32FromInt64(value)
	if err == nil {
		return safe
	}

	if value < 0 {
		return 0
	}

	return int32(^uint32(0) >> 1)
}

func timeToMillis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}

	return value.UnixMilli()
}

func scheduleToProto(schedule AdminSchedule) *workerpb.ScheduleEntry {
	nextRun := int64(0)
	if !schedule.NextRun.IsZero() {
		nextRun = schedule.NextRun.UnixMilli()
	}

	lastRun := int64(0)
	if !schedule.LastRun.IsZero() {
		lastRun = schedule.LastRun.UnixMilli()
	}

	return &workerpb.ScheduleEntry{
		Name:      schedule.Name,
		Spec:      schedule.Spec,
		NextRunMs: nextRun,
		LastRunMs: lastRun,
		Durable:   schedule.Durable,
		Paused:    schedule.Paused,
	}
}

func scheduleFactoryToProto(factory AdminScheduleFactory) *workerpb.ScheduleFactory {
	return &workerpb.ScheduleFactory{
		Name:    factory.Name,
		Durable: factory.Durable,
	}
}

func scheduleEventToProto(event AdminScheduleEvent) *workerpb.ScheduleEvent {
	return &workerpb.ScheduleEvent{
		TaskId:       event.TaskID,
		Name:         event.Name,
		Spec:         event.Spec,
		Durable:      event.Durable,
		Status:       event.Status,
		Queue:        event.Queue,
		StartedAtMs:  timeToMillis(event.StartedAt),
		FinishedAtMs: timeToMillis(event.FinishedAt),
		DurationMs:   event.DurationMs,
		Result:       event.Result,
		Error:        event.Error,
		Metadata:     event.Metadata,
	}
}

func jobSpecFromProto(spec *workerpb.JobSpec) AdminJobSpec {
	if spec == nil {
		return AdminJobSpec{}
	}

	timeout := time.Duration(spec.GetTimeoutSeconds()) * time.Second

	return AdminJobSpec{
		Name:        strings.TrimSpace(spec.GetName()),
		Description: strings.TrimSpace(spec.GetDescription()),
		Repo:        strings.TrimSpace(spec.GetRepo()),
		Tag:         strings.TrimSpace(spec.GetTag()),
		Path:        strings.TrimSpace(spec.GetPath()),
		Dockerfile:  strings.TrimSpace(spec.GetDockerfile()),
		Command:     append([]string{}, spec.GetCommand()...),
		Env:         append([]string{}, spec.GetEnv()...),
		Queue:       strings.TrimSpace(spec.GetQueue()),
		Retries:     int(spec.GetRetries()),
		Timeout:     timeout,
	}
}

func jobToProto(job AdminJob) *workerpb.Job {
	timeoutSeconds := safeInt32FromInt64(int64(job.Timeout.Seconds()))

	return &workerpb.Job{
		Spec: &workerpb.JobSpec{
			Name:           job.Name,
			Description:    job.Description,
			Repo:           job.Repo,
			Tag:            job.Tag,
			Path:           job.Path,
			Dockerfile:     job.Dockerfile,
			Command:        append([]string{}, job.Command...),
			Env:            append([]string{}, job.Env...),
			Queue:          job.Queue,
			Retries:        clampInt32(job.Retries),
			TimeoutSeconds: timeoutSeconds,
		},
		CreatedAtMs: job.CreatedAt.UnixMilli(),
		UpdatedAtMs: job.UpdatedAt.UnixMilli(),
	}
}

func toAdminStatus(err error) error {
	if err == nil {
		return nil
	}

	mapped := mapAdminError(err)
	if mapped != nil {
		return mapped
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return ewrap.Wrap(status.Error(codes.Internal, err.Error()), "internal error")
}

func mapAdminError(err error) error {
	if errors.Is(err, ErrAdminBackendUnavailable) || errors.Is(err, ErrAdminUnsupported) {
		return ewrap.Wrap(status.Error(codes.Unimplemented, err.Error()), "admin backend unavailable or unsupported")
	}

	if errors.Is(err, ErrInvalidTaskContext) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "invalid task context")
	}

	mapped := mapAdminQueueError(err)
	if mapped != nil {
		return mapped
	}

	mapped = mapAdminDLQError(err)
	if mapped != nil {
		return mapped
	}

	mapped = mapAdminScheduleError(err)
	if mapped != nil {
		return mapped
	}

	mapped = mapAdminJobError(err)
	if mapped != nil {
		return mapped
	}

	return nil
}

func mapAdminQueueError(err error) error {
	if errors.Is(err, ErrAdminQueueNameRequired) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "queue name required")
	}

	if errors.Is(err, ErrAdminQueueNotFound) {
		return ewrap.Wrap(status.Error(codes.NotFound, err.Error()), "queue not found")
	}

	if errors.Is(err, ErrAdminQueueWeightInvalid) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "queue weight invalid")
	}

	return nil
}

func mapAdminDLQError(err error) error {
	if errors.Is(err, ErrAdminDLQFilterTooLarge) {
		return ewrap.Wrap(status.Error(codes.ResourceExhausted, err.Error()), "dlq filter too large")
	}

	if errors.Is(err, ErrAdminDLQEntryIDRequired) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "dlq id required")
	}

	if errors.Is(err, ErrAdminDLQEntryNotFound) {
		return ewrap.Wrap(status.Error(codes.NotFound, err.Error()), "dlq entry not found")
	}

	if errors.Is(err, ErrAdminReplayIDsRequired) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "replay ids required")
	}

	if errors.Is(err, ErrAdminReplayIDsTooLarge) {
		return ewrap.Wrap(status.Error(codes.ResourceExhausted, err.Error()), "replay ids too large")
	}

	return nil
}

func mapAdminScheduleError(err error) error {
	if errors.Is(err, ErrAdminScheduleNameRequired) || errors.Is(err, ErrAdminScheduleSpecRequired) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "schedule request invalid")
	}

	if errors.Is(err, ErrAdminScheduleNotFound) {
		return ewrap.Wrap(status.Error(codes.NotFound, err.Error()), "schedule not found")
	}

	if errors.Is(err, ErrAdminScheduleFactoryMissing) || errors.Is(err, ErrAdminScheduleDurableMismatch) {
		return ewrap.Wrap(status.Error(codes.FailedPrecondition, err.Error()), "schedule factory missing")
	}

	return nil
}

func mapAdminJobError(err error) error {
	if errors.Is(err, ErrAdminJobNameRequired) || errors.Is(err, ErrAdminJobRepoRequired) || errors.Is(err, ErrAdminJobTagRequired) {
		return ewrap.Wrap(status.Error(codes.InvalidArgument, err.Error()), "job request invalid")
	}

	if errors.Is(err, ErrAdminJobNotFound) {
		return ewrap.Wrap(status.Error(codes.NotFound, err.Error()), "job not found")
	}

	return nil
}
