package worker

import "github.com/google/uuid"

// Service is an interface for a task manager
type Service interface {
	// RegisterTask registers a new task to the worker
	RegisterTask(tasks ...Task)
	// Start the task manager
	Start(numWorkers int)
	// Stop the task manage
	Stop()
	// GetResults gets the results channel
	GetResults() <-chan interface{}
	// GetTask gets a task by its ID
	GetTask(id uuid.UUID) (task Task, ok bool)
	// GetTasks gets all tasks
	GetTasks() []Task
	// ExecuteTask executes a task given its ID and returns the result
	ExecuteTask(id uuid.UUID) (interface{}, error)
	// CancelAll cancels all tasks
	CancelAll()
	// CancelTask cancels a task by its ID
	CancelTask(id uuid.UUID)
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
