package worker

import (
	"context"

	"github.com/hyp3rd/ewrap"
)

var (
	// ErrAdminBackendUnavailable indicates the admin backend is not configured.
	ErrAdminBackendUnavailable = ewrap.New("admin backend unavailable")
	// ErrAdminUnsupported indicates the backend does not support the admin operation.
	ErrAdminUnsupported = ewrap.New("admin operation unsupported")
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

// AdminBackend provides admin data and actions for a backend.
type AdminBackend interface {
	AdminOverview(ctx context.Context) (AdminOverview, error)
	AdminQueues(ctx context.Context) ([]AdminQueueSummary, error)
	AdminDLQ(ctx context.Context, limit int) ([]AdminDLQEntry, error)
	AdminPause(ctx context.Context) error
	AdminResume(ctx context.Context) error
	AdminReplayDLQ(ctx context.Context, limit int) (int, error)
}
