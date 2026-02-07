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
	// ErrAdminDLQFilterTooLarge indicates the DLQ is too large for filtered queries.
	ErrAdminDLQFilterTooLarge = ewrap.New("DLQ too large for filtered query")
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
}

// AdminDLQEntry represents a DLQ entry.
type AdminDLQEntry struct {
	ID       string
	Queue    string
	Handler  string
	Attempts int
	AgeMs    int64
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
}

type adminQueues interface {
	AdminQueues(ctx context.Context) ([]AdminQueueSummary, error)
	AdminQueue(ctx context.Context, name string) (AdminQueueSummary, error)
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
	AdminPause(ctx context.Context) error
	AdminResume(ctx context.Context) error
	AdminReplayDLQ(ctx context.Context, limit int) (int, error)
	AdminReplayDLQByID(ctx context.Context, ids []string) (int, error)
}
