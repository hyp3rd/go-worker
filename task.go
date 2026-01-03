package worker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
)

// Errors returned by the TaskManager.
var (
	// ErrInvalidTaskID is returned when a task has an invalid ID.
	ErrInvalidTaskID = ewrap.New("invalid task id")
	// ErrInvalidTaskFunc is returned when a task has an invalid function.
	ErrInvalidTaskFunc = ewrap.New("invalid task function")
	// ErrInvalidTaskContext is returned when a task has an invalid context.
	ErrInvalidTaskContext = ewrap.New("invalid task context")
	// ErrTaskNotFound is returned when a task is not found.
	ErrTaskNotFound = ewrap.New("task not found")
	// ErrTaskTimeout is returned when a task times out.
	ErrTaskTimeout = ewrap.New("task timeout")
	// ErrTaskCancelled is returned when a task is cancelled.
	ErrTaskCancelled = ewrap.New("task cancelled")
	// ErrTaskAlreadyStarted is returned when a task is already started.
	ErrTaskAlreadyStarted = ewrap.New("task already started")
	// ErrTaskCompleted is returned when a task is already completed.
	ErrTaskCompleted = ewrap.New("task completed")
)

type (
	// TaskStatus is a value used to represent the task status.
	TaskStatus uint8

	// TaskFunc signature of `Task` function.
	TaskFunc func(ctx context.Context, args ...any) (any, error)
)

// CancelReason values
//   - 1: `ContextDeadlineReached`
//   - 2: `RateLimited`
//   - 3: `Cancelled`
//   - 4: `Failed`
//   - 5: `Queued`
//   - 6: `Running`
//   - 7: `Invalid`
//   - 8: `Completed`
const (
	// ContextDeadlineReached means the context is past its deadline.
	ContextDeadlineReached = TaskStatus(1)
	// RateLimited means the number of concurrent tasks per second exceeded the maximum allowed.
	RateLimited = TaskStatus(2)
	// Cancelled means `CancelTask` was invoked and the `Task` was cancelled.
	Cancelled = TaskStatus(3)
	// Failed means the `Task` failed.
	Failed = TaskStatus(4)
	// Queued means the `Task` is queued.
	Queued = TaskStatus(5)
	// Running means the `Task` is running.
	Running = TaskStatus(6)
	// Invalid means the `Task` is invalid.
	Invalid = TaskStatus(7)
	// Completed means the `Task` is completed.
	Completed = TaskStatus(8)
)

const (
	waitForTaskCancelled = 100 * time.Millisecond
)

// String returns the string representation of the task status.
func (ts TaskStatus) String() string {
	switch ts {
	case Cancelled:
		return "Cancelled"
	case RateLimited:
		return "RateLimited"
	case ContextDeadlineReached:
		return "ContextDeadlineReached"
	case Failed:
		return "Failed"
	case Queued:
		return "Queued"
	case Running:
		return "Running"
	case Invalid:
		return "Invalid"
	case Completed:
		return "Completed"
	default:
		return "Unknown"
	}
}

// Task represents a function that can be executed by the task manager.
//
//nolint:containedctx
type Task struct {
	ID          uuid.UUID          `json:"id"`          // ID is the id of the task
	Name        string             `json:"name"`        // Name is the name of the task
	Description string             `json:"description"` // Description is the description of the task
	Priority    int                `json:"priority"`    // Priority is the priority of the task
	Execute     TaskFunc           `json:"-"`           // Execute is the function that will be executed by the task
	Ctx         context.Context    `json:"context"`     // Ctx is the context of the task
	CancelFunc  context.CancelFunc `json:"-"`           // CancelFunc is the cancel function of the task
	Status      TaskStatus         `json:"task_status"` // TaskStatus is stores the status of the task
	Result      atomic.Value       `json:"result"`      // Result is the result of the task
	Error       atomic.Value       `json:"error"`       // Error is the error of the task
	Started     atomic.Int64       `json:"started"`     // Started is the time the task started
	Completed   atomic.Int64       `json:"completed"`   // Completed is the time the task completed
	Cancelled   atomic.Int64       `json:"cancelled"`   // Cancelled is the time the task was cancelled
	Retries     int                `json:"retries"`     // Retries is the maximum number of retries for failed tasks
	RetryDelay  time.Duration      `json:"retry_delay"` // RetryDelay is the time delay between retries for failed tasks
	index       int                // index is the index of the task in the task manager
}

// NewTask creates a new task with the provided function and context.
func NewTask(ctx context.Context, fn TaskFunc) (*Task, error) {
	task := &Task{
		ID:         uuid.New(),
		Execute:    fn,
		Ctx:        ctx,
		Retries:    0,
		RetryDelay: 0,
	}

	err := task.IsValid()
	if err != nil {
		// prevent the task from being rescheduled
		task.Status = Invalid
		task.setCancelled()

		return nil, err
	}

	return task, nil
}

// IsValid returns an error if the task is invalid.
func (task *Task) IsValid() (err error) {
	if task.ID == uuid.Nil {
		err = ErrInvalidTaskID
		task.Error.Store(err.Error())

		return err
	}

	if task.Ctx == nil {
		err = ErrInvalidTaskContext
		task.Error.Store(err.Error())

		return err
	}

	if task.Execute == nil {
		err = ErrInvalidTaskFunc
		task.Error.Store(err.Error())

		return err
	}

	return err
}

// WaitCancelled waits for the task to be cancelled.
func (task *Task) WaitCancelled() {
	select {
	case <-task.Ctx.Done():
		return
	case <-time.After(waitForTaskCancelled):
		task.WaitCancelled()
	}
}

// CancelledChan returns a channel which gets closed when the task is cancelled.
func (task *Task) CancelledChan() <-chan struct{} {
	return task.Ctx.Done()
}

// ShouldSchedule returns an error if the task should not be scheduled.
func (task *Task) ShouldSchedule() error {
	// check if the task has been cancelled
	if task.Cancelled.Load() > 0 && task.Status != Cancelled {
		return ewrap.Wrapf(ErrTaskCancelled, "Task ID %s is already cancelled", task.ID)
	}

	// check if the task has started
	if task.Started.Load() > 0 {
		return ewrap.Wrapf(ErrTaskAlreadyStarted, "Task ID %s has already started", task.ID)
	}

	// check if the task has completed
	if task.Completed.Load() > 0 {
		return ewrap.Wrapf(ErrTaskCompleted, "Task ID %s has already completed", task.ID)
	}

	return nil
}

// setStarted handles the start of a task by setting the start time.
func (task *Task) setStarted() {
	task.Started.Store(time.Now().UnixNano())
	task.Status = Running
}

// setCompleted handles the finish of a task by setting the finish time.
func (task *Task) setCompleted() {
	task.Completed.Store(time.Now().UnixNano())
	task.Status = Completed
}

// setCancelled handles the cancellation of a task by setting the cancellation time.
func (task *Task) setCancelled() {
	task.Cancelled.Store(time.Now().UnixNano())
	task.Status = Cancelled
}

// setQueued handles the queuing of a task by setting the status to queued.
func (task *Task) setQueued() {
	task.Status = Queued
}

// setRateLimited handles the rate limiting of a task by setting the status to rate limited.
func (task *Task) setRateLimited() {
	task.Status = RateLimited
}

// setFailed handles the failure of a task by setting the status to failed.
func (task *Task) setFailed(err any) {
	if err != nil {
		task.Error.Store(err)
	}

	task.Status = Failed
}

// setError handles the error of a task by setting the error.
func (task *Task) setError(err error) {
	if err != nil {
		task.Error.Store(err.Error())
	}
}

// setResult handles the result of a task by setting the result.
func (task *Task) setResult(result any) {
	if result != nil {
		task.Result.Store(result)
	}
}
