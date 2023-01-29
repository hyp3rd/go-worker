package middleware

import (
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
func (mw *loggerMiddleware) RegisterTask(tasks ...worker.Task) {
	defer func(begin time.Time) {
		for _, t := range tasks {
			mw.logger.Printf("registering task ID %v with priority: %v", t.ID, t.Priority)
		}
		mw.logger.Printf("`RegisterTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.RegisterTask(tasks...)
}

// Start the task manager
func (mw *loggerMiddleware) Start(numWorkers int) {
	defer func(begin time.Time) {
		// var numCPU = runtime.GOMAXPROCS(0)
		var numCPU = runtime.NumCPU()
		mw.logger.Printf("the task manager is running on %v CPUs", numCPU)
		mw.logger.Printf("the task manager started with %v workers", numWorkers)
		mw.logger.Printf("`Start` took: %s", time.Since(begin))
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

// ExecuteTask executes a task given its ID and returns the result
func (mw *loggerMiddleware) ExecuteTask(id uuid.UUID) (res interface{}, err error) {
	defer func(begin time.Time) {
		mw.logger.Printf("`ExecuteTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("executing task ID %v", id)

	// if res, err = mw.next.ExecuteTask(id); err != nil {
	// 	mw.logger.Printf("error while executing task ID %v - %v", id, err)
	// }

	return mw.next.ExecuteTask(id)
}

// CancelAll cancels all tasks
func (mw *loggerMiddleware) CancelAll() {
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
