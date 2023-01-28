package worker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Task represents a function that can be executed by the task manager
type Task struct {
	ID        uuid.UUID          `json:"id"`        // ID is the id of the task
	Priority  int                `json:"priority"`  // Priority is the priority of the task
	Fn        func() interface{} `json:"-"`         // Fn is the function that will be executed by the task
	Ctx       context.Context    `json:"context"`   // Ctx is the context of the task
	Cancel    context.CancelFunc `json:"-"`         // Cancel is the cancel function of the task
	Started   atomic.Int64       `json:"started"`   // Started is the time the task started
	Completed atomic.Int64       `json:"completed"` // Completed is the time the task completed
	Cancelled atomic.Int64       `json:"cancelled"` // Cancelled is the time the task was cancelled
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
func (h taskHeap) Less(i, j int) bool { return h[i].Priority > h[j].Priority }

// Swap swaps the position of two tasks in the heap
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds a new task to the heap
func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(Task))
}

// Pop removes the last task from the heap and returns it
func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
