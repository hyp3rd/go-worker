package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Service is an interface for a task manager.
type Service interface {
	workerOperations
	taskOperations
	metricsOperations
}

type workerOperations interface {
	// RegisterTask registers a new task to the worker.
	RegisterTask(ctx context.Context, task *Task) error
	// RegisterTasks registers multiple tasks to the worker.
	RegisterTasks(ctx context.Context, tasks ...*Task) error
	// StartWorkers starts the task manager's workers (idempotent).
	StartWorkers(ctx context.Context)
	// SetMaxWorkers adjusts the worker pool size.
	SetMaxWorkers(n int)
	// Wait waits for all tasks to finish or context cancellation.
	Wait(ctx context.Context) error
	// StopGraceful stops accepting new tasks and waits for completion.
	StopGraceful(ctx context.Context) error
	// StopNow cancels running tasks and stops workers immediately.
	StopNow()
}

type taskOperations interface {
	// CancelAll cancels all tasks.
	CancelAll()
	// CancelTask cancels a task by its ID.
	CancelTask(id uuid.UUID) error
	// GetActiveTasks returns the number of running tasks.
	GetActiveTasks() int
	// SubscribeResults returns a results channel and unsubscribe function.
	SubscribeResults(buffer int) (<-chan Result, func())
	// GetTask gets a task by its ID.
	GetTask(id uuid.UUID) (task *Task, err error)
	// GetTasks gets all tasks.
	GetTasks() []*Task
	// ExecuteTask executes a task given its ID and returns the result.
	ExecuteTask(ctx context.Context, id uuid.UUID, timeout time.Duration) (any, error)
}

type metricsOperations interface {
	// GetMetrics returns a snapshot of task metrics.
	GetMetrics() MetricsSnapshot
}

// Middleware describes a generic middleware.
type Middleware[T any] func(T) T

// RegisterMiddleware registers middlewares to the provided service.
func RegisterMiddleware[T any](svc T, mw ...Middleware[T]) T {
	for _, m := range mw {
		svc = m(svc)
	}

	return svc
}
