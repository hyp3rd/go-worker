package worker

import (
	"context"
	"errors"
	"sync"
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

const (
	retryJitterDivisor  = 5
	retryJitterShiftA   = 13
	retryJitterShiftB   = 7
	retryJitterShiftC   = 17
	retryJitterMinInt   = -1 << 63
	retryJitterHalfUUID = 8
	retryJitterByteBits = 8
)

type (
	// TaskStatus is a value used to represent the task status.
	TaskStatus uint8

	// TaskFunc signature of `Task` function.
	TaskFunc func(ctx context.Context, args ...any) (any, error)
)

// TaskStatus values.
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

func isTerminalStatus(status TaskStatus) bool {
	switch status {
	case Cancelled, Failed, Completed, Invalid, ContextDeadlineReached:
		return true
	case RateLimited, Queued, Running:
		return false
	}

	return false
}

// Task represents a function that can be executed by the task manager.
//
// Note: access Task state via methods to avoid data races.
//
//nolint:containedctx
type Task struct {
	ID          uuid.UUID          `json:"id"`          // ID is the id of the task
	Name        string             `json:"name"`        // Name is the name of the task
	Description string             `json:"description"` // Description is the description of the task
	Priority    int                `json:"priority"`    // Priority is the priority of the task
	Execute     TaskFunc           `json:"-"`           // Execute is the function that will be executed by the task
	Ctx         context.Context    `json:"-"`           // Ctx is the context of the task
	CancelFunc  context.CancelFunc `json:"-"`           // CancelFunc is the cancel function of the task

	Retries    int           `json:"retries"`     // Retries is the maximum number of retries for failed tasks
	RetryDelay time.Duration `json:"retry_delay"` // RetryDelay is the time delay between retries for failed tasks

	// internal state
	mu               sync.Mutex
	status           TaskStatus
	result           any
	err              error
	started          time.Time
	completed        time.Time
	cancelled        time.Time
	retriesRemaining int
	retryBackoff     time.Duration
	index            int
	doneOnce         sync.Once
}

// NewTask creates a new task with the provided function and context.
func NewTask(ctx context.Context, fn TaskFunc) (*Task, error) {
	task := &Task{
		ID:         uuid.New(),
		Execute:    fn,
		Ctx:        ctx,
		Retries:    0,
		RetryDelay: 0,
		index:      -1,
	}

	err := task.IsValid()
	if err != nil {
		// prevent the task from being scheduled
		task.setStatusLocked(Invalid)
		task.cancelled = time.Now()
		task.err = err

		return nil, err
	}

	return task, nil
}

// IsValid returns an error if the task is invalid.
func (task *Task) IsValid() (err error) {
	if task.ID == uuid.Nil {
		return ErrInvalidTaskID
	}

	if task.Ctx == nil {
		return ErrInvalidTaskContext
	}

	if task.Execute == nil {
		return ErrInvalidTaskFunc
	}

	return nil
}

// Status returns the current task status.
func (task *Task) Status() TaskStatus {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.status
}

// Result returns the task result if available.
func (task *Task) Result() any {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.result
}

// Error returns the task error if available.
func (task *Task) Error() error {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.err
}

// StartedAt returns the start time if set.
func (task *Task) StartedAt() time.Time {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.started
}

// CompletedAt returns the completion time if set.
func (task *Task) CompletedAt() time.Time {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.completed
}

// CancelledAt returns the cancellation time if set.
func (task *Task) CancelledAt() time.Time {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.cancelled
}

// CancelledChan returns a channel which gets closed when the task is cancelled.
func (task *Task) CancelledChan() <-chan struct{} {
	if task.Ctx == nil {
		return nil
	}

	return task.Ctx.Done()
}

// ShouldSchedule returns an error if the task should not be scheduled.
func (task *Task) ShouldSchedule() error {
	task.mu.Lock()
	defer task.mu.Unlock()

	switch task.status {
	case Cancelled:
		return ewrap.Wrapf(ErrTaskCancelled, "task %s is cancelled", task.ID)
	case Running:
		return ewrap.Wrapf(ErrTaskAlreadyStarted, "task %s has already started", task.ID)
	case Completed:
		return ewrap.Wrapf(ErrTaskCompleted, "task %s has already completed", task.ID)
	case Failed, Invalid, ContextDeadlineReached:
		return ewrap.Wrapf(ErrTaskCompleted, "task %s is in terminal state %s", task.ID, task.status)
	case RateLimited, Queued:
		return nil
	}

	return nil
}

func (task *Task) latency() (time.Duration, bool) {
	task.mu.Lock()
	defer task.mu.Unlock()

	if task.started.IsZero() {
		return 0, false
	}

	if !isTerminalStatus(task.status) {
		return 0, false
	}

	var end time.Time
	if task.status == Cancelled {
		end = task.cancelled
	} else {
		end = task.completed
	}

	if end.IsZero() || end.Before(task.started) {
		return 0, false
	}

	return end.Sub(task.started), true
}

func (task *Task) terminalAt() (time.Time, bool) {
	task.mu.Lock()
	defer task.mu.Unlock()

	if !isTerminalStatus(task.status) {
		return time.Time{}, false
	}

	if task.status == Cancelled {
		return task.cancelled, true
	}

	return task.completed, true
}

func (task *Task) setStatusLocked(status TaskStatus) {
	task.status = status
}

func (task *Task) setQueued() {
	task.mu.Lock()
	task.status = Queued
	task.mu.Unlock()
}

func (task *Task) markRunning() bool {
	task.mu.Lock()
	defer task.mu.Unlock()

	if task.status == Cancelled || task.status == Completed || task.status == Failed || task.status == Invalid || task.status == Running ||
		task.status == ContextDeadlineReached {
		return false
	}

	task.status = Running
	task.started = time.Now()

	return true
}

func (task *Task) markTerminal(status TaskStatus, result any, err error) bool {
	task.mu.Lock()
	defer task.mu.Unlock()

	if isTerminalStatus(task.status) {
		return false
	}

	if !isTerminalStatus(status) {
		return false
	}

	now := time.Now()
	//nolint:exhaustive
	switch status {
	case Completed, Failed, ContextDeadlineReached, Invalid:
		task.completed = now
	case Cancelled:
		task.cancelled = now
	default:
		return false
	}

	task.status = status

	if err != nil {
		task.err = err
	}

	if result != nil {
		task.result = result
	}

	return true
}

func (task *Task) isCancelled() bool {
	if task.Ctx != nil && errors.Is(task.Ctx.Err(), context.Canceled) {
		return true
	}

	task.mu.Lock()
	defer task.mu.Unlock()

	return task.status == Cancelled
}

func (task *Task) isQueued() bool {
	task.mu.Lock()
	defer task.mu.Unlock()

	return task.status == Queued
}

func (task *Task) nextRetryDelay() (time.Duration, bool) {
	task.mu.Lock()
	defer task.mu.Unlock()

	if task.retriesRemaining <= 0 {
		return 0, false
	}

	attempt := task.retriesRemaining

	task.retriesRemaining--
	if task.retryBackoff <= 0 {
		task.retryBackoff = DefaultRetryDelay
	}

	delay := task.retryBackoff
	// exponential backoff, capped to 1 minute
	next := min(task.retryBackoff*2, time.Minute)

	task.retryBackoff = next

	delay = retryJitterDelay(delay, task.ID, attempt)

	return delay, true
}

func retryJitterDelay(delay time.Duration, id uuid.UUID, attempt int) time.Duration {
	if delay <= 0 {
		return delay
	}

	span := delay / retryJitterDivisor
	if span <= 0 {
		return delay
	}

	hash := retryJitterAbs(retryJitterHash(id, attempt))
	if hash == 0 {
		return delay
	}

	spanInt := int64(span)
	if spanInt <= 0 {
		return delay
	}

	offset := time.Duration(hash%spanInt) - span/2

	return delay + offset
}

func retryJitterHash(id uuid.UUID, attempt int) int64 {
	left, right := retryJitterUUIDParts(id)
	mix := left ^ right ^ int64(attempt)

	mix ^= mix << retryJitterShiftA
	mix ^= mix >> retryJitterShiftB
	mix ^= mix << retryJitterShiftC

	return mix
}

func retryJitterAbs(val int64) int64 {
	if val == retryJitterMinInt {
		return 0
	}

	if val < 0 {
		return -val
	}

	return val
}

func retryJitterUUIDParts(id uuid.UUID) (left, right int64) {
	for i := range retryJitterHalfUUID {
		left |= int64(id[i]) << (retryJitterByteBits * i)
	}

	for i := range retryJitterHalfUUID {
		right |= int64(id[i+retryJitterHalfUUID]) << (retryJitterByteBits * i)
	}

	return left, right
}

func (task *Task) initRetryState(defaultDelay time.Duration, maxRetries int) {
	task.mu.Lock()
	defer task.mu.Unlock()

	if task.Retries < 0 {
		task.Retries = 0
	}

	if task.Retries > maxRetries {
		task.Retries = maxRetries
	}

	if task.RetryDelay <= 0 {
		task.RetryDelay = defaultDelay
	}

	task.retriesRemaining = task.Retries

	task.retryBackoff = task.RetryDelay
	if task.status == 0 {
		task.status = Queued
	}
}
