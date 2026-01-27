package middleware

import (
	"context"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	worker "github.com/hyp3rd/go-worker"
)

// Logger describes a logging interface allowing to implement different external, or custom logger.
type Logger interface {
	Printf(format string, v ...any)
}

// this is a safeguard, breaking on compile time in case
// `log.Logger` does not adhere to our `Logger` interface.
// see https://golang.org/doc/faq#guarantee_satisfies_interface
var _ Logger = &log.Logger{}

// DefaultLogger returns a `Logger` implementation backed by stdlib's log.
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

// RegisterTask registers a new task to the worker.
func (mw *loggerMiddleware) RegisterTask(ctx context.Context, task *worker.Task) error {
	defer func(begin time.Time) {
		err := task.Error()
		if err != nil {
			mw.logger.Printf("error while registering task ID %v - %v", task.ID, err)

			return
		}

		mw.logger.Printf("registering task ID %v with priority: %v", task.ID, task.Priority)
		mw.logger.Printf("`RegisterTask` took: %s", time.Since(begin))
	}(time.Now())

	return mw.next.RegisterTask(ctx, task)
}

// RegisterTasks registers multiple tasks to the worker.
func (mw *loggerMiddleware) RegisterTasks(ctx context.Context, tasks ...*worker.Task) error {
	defer func(begin time.Time) {
		for _, task := range tasks {
			err := task.Error()
			if err != nil {
				mw.logger.Printf("error while registering task ID %v - %v", task.ID, err)

				continue
			}

			mw.logger.Printf("registering task ID %v with priority: %v", task.ID, task.Priority)
		}

		mw.logger.Printf("`RegisterTasks` took: %s", time.Since(begin))
	}(time.Now())

	return mw.next.RegisterTasks(ctx, tasks...)
}

// StartWorkers starts the task manager.
func (mw *loggerMiddleware) StartWorkers(ctx context.Context) {
	defer func(begin time.Time) {
		numCPU := runtime.NumCPU()
		mw.logger.Printf("the task manager is running on %v CPUs", numCPU)
		mw.logger.Printf("`StartWorkers` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.StartWorkers(ctx)
}

// SetMaxWorkers adjusts the worker pool size.
func (mw *loggerMiddleware) SetMaxWorkers(n int) {
	mw.next.SetMaxWorkers(n)
}

// Wait for the task manager to finish all tasks.
func (mw *loggerMiddleware) Wait(ctx context.Context) error {
	return mw.next.Wait(ctx)
}

// StopGraceful stops the task manager after all tasks finish.
func (mw *loggerMiddleware) StopGraceful(ctx context.Context) error {
	defer func(begin time.Time) {
		mw.logger.Printf("`StopGraceful` took: %s", time.Since(begin))
	}(time.Now())

	return mw.next.StopGraceful(ctx)
}

// StopNow stops the task manager immediately.
func (mw *loggerMiddleware) StopNow() {
	defer func(begin time.Time) {
		mw.logger.Printf("`StopNow` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.StopNow()
}

// CancelAll cancels all tasks.
func (mw *loggerMiddleware) CancelAll() {
	defer func(begin time.Time) {
		mw.logger.Printf("`CancelAll` took: %s", time.Since(begin))
	}(time.Now())

	mw.next.CancelAll()
}

// CancelTask cancels a task by its ID.
func (mw *loggerMiddleware) CancelTask(id uuid.UUID) error {
	defer func(begin time.Time) {
		mw.logger.Printf("`CancelTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("cancelling task ID %v", id)

	return mw.next.CancelTask(id)
}

// GetActiveTasks gets the number of active tasks.
func (mw *loggerMiddleware) GetActiveTasks() int {
	return mw.next.GetActiveTasks()
}

// GetResults returns a results channel (compatibility shim for legacy API).
func (mw *loggerMiddleware) GetResults() <-chan worker.Result {
	return mw.next.GetResults()
}

// SubscribeResults streams the results channel.
func (mw *loggerMiddleware) SubscribeResults(buffer int) (<-chan worker.Result, func()) {
	return mw.next.SubscribeResults(buffer)
}

// SetResultsDropPolicy configures how full subscriber buffers are handled.
func (mw *loggerMiddleware) SetResultsDropPolicy(policy worker.ResultDropPolicy) {
	mw.next.SetResultsDropPolicy(policy)
}

// GetTask gets a task by its ID.
func (mw *loggerMiddleware) GetTask(id uuid.UUID) (task *worker.Task, err error) {
	return mw.next.GetTask(id)
}

// GetTasks gets all tasks.
func (mw *loggerMiddleware) GetTasks() []*worker.Task {
	return mw.next.GetTasks()
}

// ExecuteTask executes a task given its ID and returns the result.
func (mw *loggerMiddleware) ExecuteTask(ctx context.Context, id uuid.UUID, timeout time.Duration) (res any, err error) {
	defer func(begin time.Time) {
		if err != nil {
			mw.logger.Printf("error while executing task ID %v - %v", id, err)
		}

		mw.logger.Printf("`ExecuteTask` took: %s", time.Since(begin))
	}(time.Now())

	mw.logger.Printf("executing task ID %v", id)

	return mw.next.ExecuteTask(ctx, id, timeout)
}

// GetMetrics returns a snapshot of metrics.
func (mw *loggerMiddleware) GetMetrics() worker.MetricsSnapshot {
	return mw.next.GetMetrics()
}

// SetMeterProvider enables OpenTelemetry metrics collection.
func (mw *loggerMiddleware) SetMeterProvider(provider metric.MeterProvider, opts ...worker.OTelMetricsOption) error {
	return mw.next.SetMeterProvider(provider, opts...)
}

// SetRetentionPolicy configures task registry retention.
func (mw *loggerMiddleware) SetRetentionPolicy(policy worker.RetentionPolicy) {
	mw.next.SetRetentionPolicy(policy)
}

// SetHooks configures task lifecycle hooks.
func (mw *loggerMiddleware) SetHooks(hooks worker.TaskHooks) {
	mw.next.SetHooks(hooks)
}

// SetTracer configures task tracing.
func (mw *loggerMiddleware) SetTracer(tracer worker.TaskTracer) {
	mw.next.SetTracer(tracer)
}
