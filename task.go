package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Errors returned by the TaskManager
var (
	// ErrInvalidTaskID is returned when a task has an invalid ID
	ErrInvalidTaskID = errors.New("invalid task id")
	// ErrInvalidTaskFunc is returned when a task has an invalid function
	ErrInvalidTaskFunc = errors.New("invalid task function")
	// ErrTaskNotFound is returned when a task is not found
	ErrTaskNotFound = errors.New("task not found")
	// ErrTaskTimeout is returned when a task times out
	ErrTaskTimeout = errors.New("task timeout")
	// ErrTaskCancelled is returned when a task is cancelled
	ErrTaskCancelled = errors.New("task cancelled")
	// ErrTaskAlreadyStarted is returned when a task is already started
	ErrTaskAlreadyStarted = errors.New("task already started")
	// ErrTaskCompleted is returned when a task is already completed
	ErrTaskCompleted = errors.New("task completed")
)

type (
	// TaskStatus is a value used to represent the task status.
	TaskStatus uint8

	// TaskFunc signature of `Task` function
	TaskFunc func() (interface{}, error)
)

// CancelReason values
//   - 1: `ContextDeadlineReached`
//   - 2: `RateLimited`
//   - 3: `Cancelled`
//   - 4: `Failed`
const (
	// ContextDeadlineReached means the context is past its deadline.
	ContextDeadlineReached = TaskStatus(1)
	// RateLimited means the number of concurrent tasks per second exceeded the maximum allowed.
	RateLimited = TaskStatus(2)
	// Cancelled means `CancelTask` was invked and the `Task` was cancelled.
	Cancelled = TaskStatus(3)
	// Failed means the `Task` failed.
	Failed = TaskStatus(4)
	// Queued means the `Task` is queued.
	Queued = TaskStatus(5)
	// Running means the `Task` is running.
	Running = TaskStatus(6)
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
	default:
		return "Unknown"
	}
}

// Task represents a function that can be executed by the task manager
type Task struct {
	ID          uuid.UUID          `json:"id"`          // ID is the id of the task
	Name        string             `json:"name"`        // Name is the name of the task
	Description string             `json:"description"` // Description is the description of the task
	Priority    int                `json:"priority"`    // Priority is the priority of the task
	Fn          TaskFunc           `json:"-"`           // Fn is the function that will be executed by the task
	Ctx         context.Context    `json:"context"`     // Ctx is the context of the task
	CancelFunc  context.CancelFunc `json:"-"`           // CancelFunc is the cancel function of the task
	Status      TaskStatus         `json:"task_status"` // TaskStatus is stores the status of the task
	Error       atomic.Value       `json:"error"`       // Error is the error of the task
	Started     atomic.Int64       `json:"started"`     // Started is the time the task started
	Completed   atomic.Int64       `json:"completed"`   // Completed is the time the task completed
	Cancelled   atomic.Int64       `json:"cancelled"`   // Cancelled is the time the task was cancelled
	Retries     int                `json:"retries"`     // Retries is the maximum number of retries for failed tasks
	RetryDelay  time.Duration      `json:"retry_delay"` // RetryDelay is the time delay between retries for failed tasks
	index       int                `json:"-"`           // index is the index of the task in the task manager
}

// IsValid returns an error if the task is invalid
func (task *Task) IsValid() (err error) {
	if task.ID == uuid.Nil {
		err = ErrInvalidTaskID
		task.Error.Store(err.Error())
		return
	}
	if task.Fn == nil {
		err = ErrInvalidTaskFunc
		task.Error.Store(err.Error())
		return
	}
	return
}

// setStarted handles the start of a task by setting the start time
func (task *Task) setStarted() {
	task.Started.Store(time.Now().UnixNano())
}

// setCompleted handles the finish of a task by setting the finish time
func (task *Task) setCompleted() {
	task.Completed.Store(time.Now().UnixNano())
}

// setCancelled handles the cancellation of a task by setting the cancellation time
func (task *Task) setCancelled() {
	task.Cancelled.Store(time.Now().UnixNano())
}

// setRetryDelay sets the retry delay for the task
func (task *Task) setRetryDelay(delay time.Duration) {
	task.RetryDelay = delay
}

// WaitCancelled waits for the task to be cancelled
func (task *Task) WaitCancelled() {
	for task.Cancelled.Load() == 0 {
		// create a timer with a short duration to check if the task has been cancelled
		timer := time.NewTimer(time.Millisecond * 100)
		defer timer.Stop()
		defer timer.Reset(0)

		// wait for either the timer to fire or the task to be cancelled
		select {
		case <-timer.C:
			// timer expired, check if the task has been cancelled
			continue
		case <-task.CancelledChan():
			// task has been cancelled, return
			return
		}
	}
}

// CancelledChan returns a channel that will be closed when the task is cancelled
func (task *Task) CancelledChan() <-chan struct{} {
	if task.Cancelled.Load() > 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	cancelledChan := make(chan struct{})
	go func() {
		defer close(cancelledChan)
		for {
			if task.Cancelled.Load() > 0 {
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()
	return cancelledChan
}

// ShouldExecute returns an error if the task should not be executed
func (task *Task) ShouldExecute() error {

	// check if the task has been cancelled
	if task.Cancelled.Load() > 0 && task.Status != Cancelled {
		return ErrTaskCancelled
	}

	// check if the task has started
	if task.Started.Load() > 0 {
		return ErrTaskAlreadyStarted
	}

	// check if the task has completed
	if task.Completed.Load() > 0 {
		return ErrTaskCompleted
	}

	return nil
}
