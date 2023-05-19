package middleware

import (
	"context"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
)

// Logger describes a logging interface allowing to implement different external, or custom logger.
type Logger interface {
	Printf(format string, v ...interface{})
	// Errorf(format string, v ...interface{})
}

// this is a safeguard, breaking on compile time in case
// `log.Logger` does not adhere to our `Logger` interface.
// see https://golang.org/doc/faq#guarantee_satisfies_interface
var _ Logger = &log.Logger{}

// DefaultLogger returns a `Logger` implementation
// backed by stdlib's log
func DefaultLogger() *log.Logger {
	return log.New(os.Stdout, "", log.LstdFlags)
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
func (mw *loggerMiddleware) RegisterTask(ctx context.Context, task worker.Task) error {
	defer func(begin time.Time) {
		if task.Error.Load() != nil {
			mw.logger.Printf("error while registering task ID %v - %v", task.ID, task.Error.Load())
			return
		}
		mw.logger.Printf("registering task ID %v with priority: %v", task.ID, task.Priority)

		mw.logger.Printf("`RegisterTask` took: %s", time.Since(begin))
	}(time.Now())

	return mw.next.RegisterTask(ctx, task)
}

// RegisterTasks registers multiple tasks to the worker
func (mw *loggerMiddleware) RegisterTasks(ctx context.Context, tasks ...worker.Task) {
	defer func(begin time.Time) {
		for _, t := range tasks {
			if t.Error.Load() != nil {
				mw.logger.Printf("error while registering task ID %v - %v", t.ID, t.Error.Load())
				continue
			}
			mw.logger.Printf("registering task ID %v with priority: %v", t.ID, t.Priority)
		}
		mw.logger.Printf("`RegisterTasks` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.RegisterTasks(ctx, tasks...)
}

// Start the task manager
func (mw *loggerMiddleware) StartWorkers() {
	defer func(begin time.Time) {
		// var numCPU = runtime.GOMAXPROCS(0)
		var numCPU = runtime.NumCPU()
		mw.logger.Printf("the task manager is running on %v CPUs", numCPU)
		mw.logger.Printf("`StartWorkers` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.StartWorkers()
}

// Stop the task manage
func (mw *loggerMiddleware) Stop() {
	defer func(begin time.Time) {
		mw.logger.Printf("`Stop` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.Stop()
}

// Wait for the task manager to finish all tasks
func (mw *loggerMiddleware) Wait(timeout time.Duration) {
	mw.next.Wait(timeout)
}

// CancelAll cancels all tasks
func (mw *loggerMiddleware) CancelAll() {
	defer func(begin time.Time) {
		mw.logger.Printf("`CancelAll` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.CancelAll()
}

// CancelTask cancels a task by its ID
func (mw *loggerMiddleware) CancelTask(id uuid.UUID) {
	defer func(begin time.Time) {
		mw.logger.Printf("`CancelTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("cancelling task ID %v", id)

	mw.next.CancelTask(id)
}

// GetActiveTasks gets the number of active tasks
func (mw *loggerMiddleware) GetActiveTasks() int {
	return mw.next.GetActiveTasks()
}

// StreamResults streams the results channel
func (mw *loggerMiddleware) StreamResults() <-chan worker.Result {
	return mw.next.StreamResults()
}

// GetResults returns the results channel
func (mw *loggerMiddleware) GetResults() []worker.Result {
	return mw.next.GetResults()
}

// GetCancelledTasks streams the cancelled tasks channel
func (mw *loggerMiddleware) GetCancelledTasks() <-chan worker.Task {
	return mw.next.GetCancelledTasks()
}

// GetTask gets a task by its ID
func (mw *loggerMiddleware) GetTask(id uuid.UUID) (task *worker.Task, err error) {
	return mw.next.GetTask(id)
}

// GetTasks gets all tasks
func (mw *loggerMiddleware) GetTasks() []worker.Task {
	return mw.next.GetTasks()
}

// ExecuteTask executes a task given its ID and returns the result
func (mw *loggerMiddleware) ExecuteTask(id uuid.UUID, timeout time.Duration) (res interface{}, err error) {
	defer func(begin time.Time) {
		if err != nil {
			mw.logger.Printf("error while executing task ID %v - %v", id, err)
		}
		mw.logger.Printf("`ExecuteTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("executing task ID %v", id)

	return mw.next.ExecuteTask(id, timeout)
}
