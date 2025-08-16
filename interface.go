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
}

type workerOperations interface {
	// RegisterTask registers a new task to the worker
	RegisterTask(ctx context.Context, task *Task) error
	// RegisterTasks registers multiple tasks to the worker
	RegisterTasks(ctx context.Context, tasks ...*Task)
	// StartWorkers starts the task manager's workers
	StartWorkers(ctx context.Context)
	// SetMaxWorkers adjusts the worker pool size
	SetMaxWorkers(n int)
	// Wait for all tasks to finish
	Wait(timeout time.Duration)
	// Stop the task manage
	Stop()
}

type taskOperations interface {
	// CancelAll cancels all tasks
	CancelAll()
	// CancelTask cancels a task by its ID
	CancelTask(id uuid.UUID)
	// GetActiveTasks returns the number of active tasks
	GetActiveTasks() int
	// StreamResults streams the `Result` channel
	StreamResults() <-chan Result
	// GetResults returns the `Result` channel
	GetResults() []Result
	// GetCancelledTasks gets the cancelled tasks channel
	GetCancelledTasks() <-chan *Task
	// GetTask gets a task by its ID
	GetTask(id uuid.UUID) (task *Task, err error)
	// GetTasks gets all tasks
	GetTasks() []*Task
	// ExecuteTask executes a task given its ID and returns the result
	ExecuteTask(ctx context.Context, id uuid.UUID, timeout time.Duration) (any, error)
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
