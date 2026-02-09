package worker

import (
	"context"
	"time"

	"github.com/hyp3rd/ewrap"
)

var (
	// ErrAdminBackendUnavailable indicates the admin backend is not configured.
	ErrAdminBackendUnavailable = ewrap.New("admin backend unavailable")
	// ErrAdminUnsupported indicates the backend does not support the admin operation.
	ErrAdminUnsupported = ewrap.New("admin operation unsupported")
	// ErrAdminQueueNotFound indicates the queue was not found.
	ErrAdminQueueNotFound = ewrap.New("admin queue not found")
	// ErrAdminQueueNameRequired indicates a queue name is required.
	ErrAdminQueueNameRequired = ewrap.New("admin queue name is required")
	// ErrAdminQueueWeightInvalid indicates a queue weight is invalid.
	ErrAdminQueueWeightInvalid = ewrap.New("admin queue weight is invalid")
	// ErrAdminQueuePauseUnsupported indicates pausing queues is not supported.
	ErrAdminQueuePauseUnsupported = ewrap.New("admin queue pause unsupported")
	// ErrAdminDLQFilterTooLarge indicates the DLQ is too large for filtered queries.
	ErrAdminDLQFilterTooLarge = ewrap.New("DLQ too large for filtered query")
	// ErrAdminDLQEntryIDRequired indicates a DLQ id is required.
	ErrAdminDLQEntryIDRequired = ewrap.New("admin DLQ id is required")
	// ErrAdminDLQEntryNotFound indicates the DLQ entry was not found.
	ErrAdminDLQEntryNotFound = ewrap.New("admin DLQ entry not found")
	// ErrAdminReplayIDsRequired indicates replay-by-id requires at least one id.
	ErrAdminReplayIDsRequired = ewrap.New("admin replay ids are required")
	// ErrAdminReplayIDsTooLarge indicates too many ids were provided.
	ErrAdminReplayIDsTooLarge = ewrap.New("admin replay ids limit exceeded")
	// ErrAdminScheduleNameRequired indicates a schedule name is required.
	ErrAdminScheduleNameRequired = ewrap.New("admin schedule name is required")
	// ErrAdminScheduleSpecRequired indicates a schedule spec is required.
	ErrAdminScheduleSpecRequired = ewrap.New("admin schedule spec is required")
	// ErrAdminScheduleNotFound indicates the schedule was not found.
	ErrAdminScheduleNotFound = ewrap.New("admin schedule not found")
	// ErrAdminScheduleFactoryMissing indicates a factory was not registered.
	ErrAdminScheduleFactoryMissing = ewrap.New("admin schedule factory missing")
	// ErrAdminScheduleDurableMismatch indicates durable flag mismatched.
	ErrAdminScheduleDurableMismatch = ewrap.New("admin schedule durable mismatch")
	// ErrAdminJobNameRequired indicates a job name is required.
	ErrAdminJobNameRequired = ewrap.New("admin job name is required")
	// ErrAdminJobRepoRequired indicates a job repo is required.
	ErrAdminJobRepoRequired = ewrap.New("admin job repo is required")
	// ErrAdminJobTagRequired indicates a job tag is required.
	ErrAdminJobTagRequired = ewrap.New("admin job tag is required")
	// ErrAdminJobSourceInvalid indicates the job source is invalid.
	ErrAdminJobSourceInvalid = ewrap.New("admin job source is invalid")
	// ErrAdminJobTarballURLRequired indicates a tarball URL is required.
	ErrAdminJobTarballURLRequired = ewrap.New("admin job tarball url is required")
	// ErrAdminJobTarballPathRequired indicates a tarball path is required.
	ErrAdminJobTarballPathRequired = ewrap.New("admin job tarball path is required")
	// ErrAdminJobTarballSHAInvalid indicates tarball SHA256 is invalid.
	ErrAdminJobTarballSHAInvalid = ewrap.New("admin job tarball sha256 is invalid")
	// ErrAdminJobCommandRequired indicates a job command is required.
	ErrAdminJobCommandRequired = ewrap.New("admin job command is required")
	// ErrAdminJobNotFound indicates the job was not found.
	ErrAdminJobNotFound = ewrap.New("admin job not found")
)

// AdminOverview describes the admin overview snapshot.
type AdminOverview struct {
	ActiveWorkers int
	QueuedTasks   int64
	Queues        int
	AvgLatencyMs  int64
	P95LatencyMs  int64
	Coordination  AdminCoordination
	Actions       AdminActionCounters
}

// AdminCoordination describes coordination state for durable dequeue.
type AdminCoordination struct {
	GlobalRateLimit string
	LeaderLock      string
	Lease           string
	Paused          bool
}

// AdminQueueSummary represents queue counts and weights.
type AdminQueueSummary struct {
	Name       string
	Ready      int64
	Processing int64
	Dead       int64
	Weight     int
	Paused     bool
}

// AdminDLQEntry represents a DLQ entry.
type AdminDLQEntry struct {
	ID       string
	Queue    string
	Handler  string
	Attempts int
	AgeMs    int64
}

// AdminDLQEntryDetail represents a detailed DLQ entry.
type AdminDLQEntryDetail struct {
	ID          string
	Queue       string
	Handler     string
	Attempts    int
	AgeMs       int64
	FailedAtMs  int64
	UpdatedAtMs int64
	LastError   string
	PayloadSize int64
	Metadata    map[string]string
}

// AdminSchedule represents a cron schedule entry.
type AdminSchedule struct {
	Name    string
	Spec    string
	NextRun time.Time
	LastRun time.Time
	Durable bool
	Paused  bool
}

// AdminScheduleEvent describes a cron schedule execution event.
type AdminScheduleEvent struct {
	TaskID     string
	Name       string
	Spec       string
	Durable    bool
	Status     string
	Queue      string
	StartedAt  time.Time
	FinishedAt time.Time
	DurationMs int64
	Result     string
	Error      string
	Metadata   map[string]string
}

// AdminJobEvent describes a containerized job execution event.
type AdminJobEvent struct {
	TaskID       string            `json:"task_id"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	Queue        string            `json:"queue"`
	Repo         string            `json:"repo"`
	Tag          string            `json:"tag"`
	Path         string            `json:"path"`
	Dockerfile   string            `json:"dockerfile"`
	Command      string            `json:"command"`
	ScheduleName string            `json:"schedule_name"`
	ScheduleSpec string            `json:"schedule_spec"`
	StartedAt    time.Time         `json:"started_at"`
	FinishedAt   time.Time         `json:"finished_at"`
	DurationMs   int64             `json:"duration_ms"`
	Result       string            `json:"result"`
	Error        string            `json:"error"`
	Metadata     map[string]string `json:"metadata"`
}

// AdminJobEventFilter filters job execution events.
type AdminJobEventFilter struct {
	Name  string
	Limit int
}

// AdminJobEventPage represents job execution events.
type AdminJobEventPage struct {
	Events []AdminJobEvent
}

// AdminScheduleEventFilter filters schedule events.
type AdminScheduleEventFilter struct {
	Name  string
	Limit int
}

// AdminScheduleEventPage represents schedule events.
type AdminScheduleEventPage struct {
	Events []AdminScheduleEvent
}

// AdminScheduleFactory describes a registered schedule factory.
type AdminScheduleFactory struct {
	Name    string
	Durable bool
}

// AdminScheduleSpec defines a schedule request.
type AdminScheduleSpec struct {
	Name    string
	Spec    string
	Durable bool
}

// AdminJobSpec defines a job configuration for containerized execution.
type AdminJobSpec struct {
	Name        string
	Description string
	Repo        string
	Tag         string
	Source      string
	TarballURL  string
	TarballPath string
	TarballSHA  string
	Path        string
	Dockerfile  string
	Command     []string
	Env         []string
	Queue       string
	Retries     int
	Timeout     time.Duration
}

// AdminJob represents a persisted job definition.
type AdminJob struct {
	AdminJobSpec
	CreatedAt time.Time
	UpdatedAt time.Time
}

// AdminActionCounters tracks admin action counts.
type AdminActionCounters struct {
	Pause  int64
	Resume int64
	Replay int64
}

// AdminDLQFilter controls DLQ listing.
type AdminDLQFilter struct {
	Limit   int
	Offset  int
	Queue   string
	Handler string
	Query   string
}

// AdminDLQPage represents a page of DLQ entries.
type AdminDLQPage struct {
	Entries []AdminDLQEntry
	Total   int64
}

// AdminBackend provides admin data and actions for a backend.
type AdminBackend interface {
	AdminOverview(ctx context.Context) (AdminOverview, error)
	adminQueues
	adminSchedules
	adminDLQ
	adminJobs
}

type adminQueues interface {
	AdminQueues(ctx context.Context) ([]AdminQueueSummary, error)
	AdminQueue(ctx context.Context, name string) (AdminQueueSummary, error)
	AdminPauseQueue(ctx context.Context, name string, paused bool) (AdminQueueSummary, error)
	AdminSetQueueWeight(ctx context.Context, name string, weight int) (AdminQueueSummary, error)
	AdminResetQueueWeight(ctx context.Context, name string) (AdminQueueSummary, error)
}

type adminSchedules interface {
	AdminSchedules(ctx context.Context) ([]AdminSchedule, error)
	AdminScheduleFactories(ctx context.Context) ([]AdminScheduleFactory, error)
	AdminScheduleEvents(ctx context.Context, filter AdminScheduleEventFilter) (AdminScheduleEventPage, error)
	AdminPauseSchedules(ctx context.Context, paused bool) (int, error)
	AdminRunSchedule(ctx context.Context, name string) (string, error)
	AdminCreateSchedule(ctx context.Context, spec AdminScheduleSpec) (AdminSchedule, error)
	AdminDeleteSchedule(ctx context.Context, name string) (bool, error)
	AdminPauseSchedule(ctx context.Context, name string, paused bool) (AdminSchedule, error)
}

type adminDLQ interface {
	AdminDLQ(ctx context.Context, filter AdminDLQFilter) (AdminDLQPage, error)
	AdminDLQEntry(ctx context.Context, id string) (AdminDLQEntryDetail, error)
	AdminPause(ctx context.Context) error
	AdminResume(ctx context.Context) error
	AdminReplayDLQ(ctx context.Context, limit int) (int, error)
	AdminReplayDLQByID(ctx context.Context, ids []string) (int, error)
}

type adminJobs interface {
	AdminJobs(ctx context.Context) ([]AdminJob, error)
	AdminJob(ctx context.Context, name string) (AdminJob, error)
	AdminUpsertJob(ctx context.Context, spec AdminJobSpec) (AdminJob, error)
	AdminDeleteJob(ctx context.Context, name string) (bool, error)
	AdminJobEvents(ctx context.Context, filter AdminJobEventFilter) (AdminJobEventPage, error)
}
