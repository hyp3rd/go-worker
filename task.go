package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrInvalidTaskID is returned when a task has an invalid ID
	ErrInvalidTaskID = errors.New("invalid task id")
	// ErrInvalidTaskFunc is returned when a task has an invalid function
	ErrInvalidTaskFunc = errors.New("invalid task function")
	// ErrTaskNotFound is returned when a task is not found
	ErrTaskNotFound = errors.New("task not found")
)

// CancelReason is a value used to represent the cancel reason.
type CancelReason uint8

// CancelReason values
//   - 1: `ContextDeadlineReached`
//   - 2: `RateLimited`
//   - 3: `Cancelled`
const (
	// ContextDeadlineReached means the context is past its deadline.
	ContextDeadlineReached = CancelReason(1)
	// RateLimited means the number of concurrent tasks per second exceeded the maximum allowed.
	RateLimited = CancelReason(2)
	// Cancelled means `CancelTask` was invked and the `Task` was cancelled.
	Cancelled = CancelReason(3)
)

// TaskFunc signature of `Task` function
type TaskFunc func() interface{}

// Task represents a function that can be executed by the task manager
type Task struct {
	ID           uuid.UUID          `json:"id"`            // ID is the id of the task
	Priority     int                `json:"priority"`      // Priority is the priority of the task
	Fn           TaskFunc           `json:"-"`             // Fn is the function that will be executed by the task
	Ctx          context.Context    `json:"context"`       // Ctx is the context of the task
	Cancel       context.CancelFunc `json:"-"`             // Cancel is the cancel function of the task
	Error        atomic.Value       `json:"error"`         // Error is the error of the task
	Started      atomic.Int64       `json:"started"`       // Started is the time the task started
	Completed    atomic.Int64       `json:"completed"`     // Completed is the time the task completed
	Cancelled    atomic.Int64       `json:"cancelled"`     // Cancelled is the time the task was cancelled
	CancelReason CancelReason       `json:"cancel_reason"` // CancelReason is the reason the task was cancelled
	index        int                `json:"-"`             // The index of the task in the heap.
}

// IsValid returns an error if the task is invalid
func (t *Task) IsValid() (err error) {
	if t.ID == uuid.Nil {
		err = ErrInvalidTaskID
		t.Error.Store(err.Error())
		return
	}
	if t.Fn == nil {
		err = ErrInvalidTaskFunc
		t.Error.Store(err.Error())
		return
	}
	return
}

// setStarted handles the start of a task by setting the start time
func (t *Task) setStarted() {
	t.Started.Store(time.Now().UnixNano())
}

// setCompleted handles the finish of a task by setting the finish time
func (t *Task) setCompleted() {
	t.Completed.Store(time.Now().UnixNano())
}

// setCancelled handles the cancellation of a task by setting the cancellation time
func (t *Task) setCancelled() {
	t.Cancelled.Store(time.Now().UnixNano())
}

// taskHeap is a heap of tasks that can be sorted by priority
type taskHeap []Task

// Len returns the length of the heap of tasks
func (h taskHeap) Len() int { return len(h) }

// Less returns true if the priority of the first task is less than the second task
func (h taskHeap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return h[i].Priority > h[j].Priority
}

// Swap swaps the position of two tasks in the heap
func (h taskHeap) Swap(i, j int) {
	// This is a safety check to make sure we don't panic if the heap is empty
	if h.Len() <= i || h.Len() <= j || h.Len() <= 0 {
		return
	}

	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push adds a new task to the heap
func (h *taskHeap) Push(x interface{}) {
	n := len(*h)
	task := x.(Task)
	task.index = n
	*h = append(*h, task)
}

// Pop removes the last task from the heap and returns it
func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	task := old[n-1]
	// old[n-1] = nil  // avoid memory leak
	task.index = -1 // for safety
	*h = old[0 : n-1]
	return task
}
