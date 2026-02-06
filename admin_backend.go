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
	// ErrAdminDLQFilterTooLarge indicates the DLQ is too large for filtered queries.
	ErrAdminDLQFilterTooLarge = ewrap.New("DLQ too large for filtered query")
	// ErrAdminReplayIDsRequired indicates replay-by-id requires at least one id.
	ErrAdminReplayIDsRequired = ewrap.New("admin replay ids are required")
	// ErrAdminReplayIDsTooLarge indicates too many ids were provided.
	ErrAdminReplayIDsTooLarge = ewrap.New("admin replay ids limit exceeded")
)

// AdminOverview describes the admin overview snapshot.
type AdminOverview struct {
	ActiveWorkers int
	QueuedTasks   int64
	Queues        int
	AvgLatencyMs  int64
	P95LatencyMs  int64
	Coordination  AdminCoordination
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
	AdminQueues(ctx context.Context) ([]AdminQueueSummary, error)
	AdminQueue(ctx context.Context, name string) (AdminQueueSummary, error)
	AdminSchedules(ctx context.Context) ([]AdminSchedule, error)
	AdminDLQ(ctx context.Context, filter AdminDLQFilter) (AdminDLQPage, error)
	AdminPause(ctx context.Context) error
	AdminResume(ctx context.Context) error
	AdminReplayDLQ(ctx context.Context, limit int) (int, error)
	AdminReplayDLQByID(ctx context.Context, ids []string) (int, error)
}
