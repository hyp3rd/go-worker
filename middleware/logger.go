package middleware

import (
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
)

// Logger describes a logging interface allowing to implement different external, or custom logger.
type Logger interface {
	Printf(format string, v ...interface{})
	// Errorf(format string, v ...interface{})
}

// loggerMiddleware is a middleware that logs the time it takes to execute the next middleware.
// Must implement the `worker.Service` interface.
type loggerMiddleware struct {
	next   worker.Service
	logger Logger
}

// NewLoggerMiddleware returns a new loggerMiddleware.
func NewLoggerMiddleware(next worker.Service, logger Logger) worker.Service {
	return &loggerMiddleware{next: next, logger: logger}
}

// RegisterTask registers a new task to the worker
func (mw *loggerMiddleware) RegisterTask(tasks ...worker.Task) {
	defer func(begin time.Time) {
		mw.logger.Printf("`RegisterTask` took: %s", time.Since(begin))
	}(time.Now())

	for _, t := range tasks {
		mw.logger.Printf("registered task ID %v with priority: %v", t.ID, t.Priority)
	}

	mw.next.RegisterTask(tasks...)
}

// Start the task manager
func (mw *loggerMiddleware) Start(numWorkers int) {
	defer func(begin time.Time) {
		mw.logger.Printf("`Start` took: %s", time.Since(begin))
		mw.logger.Printf("the task manager started with %v workers", numWorkers)
	}(time.Now())

	mw.next.Start(numWorkers)
}

// Stop the task manage
func (mw *loggerMiddleware) Stop() {
	mw.next.Stop()
}

// GetResults gets the results channel
func (mw *loggerMiddleware) GetResults() <-chan interface{} {
	return mw.next.GetResults()
}

// GetTask gets a task by its ID
func (mw *loggerMiddleware) GetTask(id uuid.UUID) (task worker.Task, ok bool) {
	return mw.next.GetTask(id)
}

// GetTasks gets all tasks
func (mw *loggerMiddleware) GetTasks() []worker.Task {
	return mw.next.GetTasks()
}

// CancellAll cancels all tasks
func (mw *loggerMiddleware) CancellAll() {
	mw.next.CancellAll()
}

// CancelTask cancels a task by its ID
func (mw *loggerMiddleware) CancelTask(id uuid.UUID) {
	defer func(begin time.Time) {
		mw.logger.Printf("`CancelTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("cancelling task ID %v", id)

	mw.next.CancelTask(id)
}
