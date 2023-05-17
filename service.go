package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Service is an interface for a task manager
type Service interface {
	// RegisterTask registers a new task to the worker
	RegisterTask(ctx context.Context, task Task) error
	// RegisterTasks registers multiple tasks to the worker
	RegisterTasks(ctx context.Context, tasks ...Task)
	// StartWorkers starts the task manager's workers
	StartWorkers()
	// Wait for all tasks to finish
	Wait(timeout time.Duration)
	// Close the task manage
	Close()
	// CloseAndWait the task manage to finish all tasks
	CancelAllAndWait()
	// CancelAll cancels all tasks
	CancelAll()
	// CancelTask cancels a task by its ID
	CancelTask(id uuid.UUID)
	// GetActiveTasks returns the number of active tasks
	GetActiveTasks() int
	// StreamResults streams the `Result` channel
	StreamResults() <-chan Result
	// GetResults retruns the `Result` channel
	GetResults() []Result
	// GetCancelled gets the cancelled tasks channel
	GetCancelled() <-chan Task
	// GetTask gets a task by its ID
	GetTask(id uuid.UUID) (task *Task, err error)
	// GetTasks gets all tasks
	GetTasks() []Task
	// ExecuteTask executes a task given its ID and returns the result
	ExecuteTask(id uuid.UUID, timeout time.Duration) (interface{}, error)
}

// Middleware describes a `Service` middleware.
type Middleware func(Service) Service

// RegisterMiddleware registers middlewares to the `Service`.
func RegisterMiddleware(svc Service, mw ...Middleware) Service {
	// Register each middleware in the chain
	for _, m := range mw {
		svc = m(svc)
	}
	// Return the decorated service
	return svc
}
