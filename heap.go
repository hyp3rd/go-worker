package worker

import "sync"

// // taskHeap is a heap of tasks that implements the heap interface
// type taskHeap struct {
// 	tasks       []Task
// 	numActive   int
// 	mutex       sync.Mutex
// 	waitersLock sync.Mutex
// 	waiters     []*sync.Cond
// }

// func newTaskHeap() *taskHeap {
// 	th := &taskHeap{
// 		tasks:     make([]Task, 0),
// 		numActive: 0,
// 	}
// 	th.waitersLock = sync.Mutex{}
// 	th.mutex = sync.Mutex{}
// 	th.waiters = make([]*sync.Cond, 0)
// 	return th
// }

// // Len returns the number of tasks in the heap
// func (th *taskHeap) Len() int {
// 	th.mutex.Lock()
// 	defer th.mutex.Unlock()

// 	return len(th.tasks)
// }

// // Less returns whether the task with index i should sort before the task with index j in the heap
// func (th *taskHeap) Less(i, j int) bool {
// 	th.mutex.Lock()
// 	defer th.mutex.Unlock()

// 	// sort by priority first
// 	if th.tasks[i].Priority != th.tasks[j].Priority {
// 		return th.tasks[i].Priority < th.tasks[j].Priority
// 	}

// 	// if priorities are the same, sort by ID
// 	return th.tasks[i].ID.String() < th.tasks[j].ID.String()
// }

// // Swap swaps the tasks with indexes i and j in the heap
// func (th *taskHeap) Swap(i, j int) {
// 	th.mutex.Lock()
// 	defer th.mutex.Unlock()

// 	th.tasks[i], th.tasks[j] = th.tasks[j], th.tasks[i]
// 	th.tasks[i].index = i
// 	th.tasks[j].index = j
// }

// // Push adds a task to the heap
// func (th *taskHeap) Push(x interface{}) {
// 	task := x.(Task)

// 	// Lock the mutex to access the tasks slice
// 	th.mutex.Lock()
// 	defer th.mutex.Unlock()

// 	// Add the task to the tasks slice
// 	th.tasks = append(th.tasks, task)

// 	// If there are no waiters, there's nothing else to do
// 	if len(th.waiters) == 0 {
// 		return
// 	}

// 	// Wake up the first waiter
// 	waiter := th.waiters[0]
// 	th.waiters = th.waiters[1:]
// 	waiter.Signal()
// }

// // Pop removes and returns the highest priority task from the heap
// func (th *taskHeap) Pop() interface{} {
// 	th.mutex.Lock()
// 	defer th.mutex.Unlock()

// 	if len(th.tasks) == 0 {
// 		return nil
// 	}

// 	// swap the first and last elements
// 	n := len(th.tasks)
// 	task := th.tasks[0]
// 	th.tasks[0], th.tasks[n-1] = th.tasks[n-1], th.tasks[0]
// 	th.tasks = th.tasks[:n-1]

// 	// notify waiting goroutines that a new task is available
// 	th.notifyWaiters()

// 	return task
// }

// // notifyWaiters notifies waiting goroutines that a new task is available
// func (th *taskHeap) notifyWaiters() {
// 	th.waitersLock.Lock()
// 	defer th.waitersLock.Unlock()

// 	for _, waiter := range th.waiters {
// 		waiter.Signal()
// 	}
// 	th.waiters = []*sync.Cond{}
// }

// // Wait waits for the task heap to be non-empty and returns a channel that signals when a task is added to the heap.
// func (th *taskHeap) Wait() *sync.Cond {
// 	// create a new condition variable for the waiters
// 	waiter := sync.NewCond(&th.waitersLock)

// 	// add the condition variable to the list of waiters
// 	th.waitersLock.Lock()
// 	th.waiters = append(th.waiters, waiter)
// 	th.waitersLock.Unlock()

// 	return waiter
// }

// // Unlock releases the lock.
// func (th *taskHeap) Unlock() {
// 	th.mutex.Unlock()
// }

// // Lock acquires the lock.
// func (th *taskHeap) Lock() {
// 	th.mutex.Lock()
// }

// taskHeap is a heap of tasks that implements the heap interface
type taskHeap struct {
	tasks       []Task
	mutex       sync.Mutex
	waitersLock sync.RWMutex
	waiters     []*sync.Cond
}

func newTaskHeap() *taskHeap {
	return &taskHeap{
		tasks: make([]Task, 0),
	}
}

// Len returns the number of tasks in the heap
func (th *taskHeap) Len() int {
	th.mutex.Lock()
	defer th.mutex.Unlock()

	return len(th.tasks)
}

// Less returns whether the task with index i should sort before the task with index j in the heap
func (th *taskHeap) Less(i, j int) bool {
	th.mutex.Lock()
	defer th.mutex.Unlock()

	// sort by priority first
	if th.tasks[i].Priority != th.tasks[j].Priority {
		return th.tasks[i].Priority < th.tasks[j].Priority
	}

	// if priorities are the same, sort by ID
	return th.tasks[i].ID.String() < th.tasks[j].ID.String()
}

// Swap swaps the tasks with indexes i and j in the heap
func (th *taskHeap) Swap(i, j int) {
	th.mutex.Lock()
	defer th.mutex.Unlock()

	th.tasks[i], th.tasks[j] = th.tasks[j], th.tasks[i]
	th.tasks[i].index = i
	th.tasks[j].index = j
}

// Push adds a task to the heap
func (th *taskHeap) Push(x interface{}) {
	task := x.(Task)

	// Lock the mutex to access the tasks slice
	th.mutex.Lock()
	defer th.mutex.Unlock()

	// Add the task to the tasks slice
	th.tasks = append(th.tasks, task)

	// Wake up the first waiter
	if len(th.waiters) > 0 {
		waiter := th.waiters[0]
		th.waiters = th.waiters[1:]
		waiter.Signal()
	}
}

// Pop removes and returns the highest priority task from the heap
func (th *taskHeap) Pop() interface{} {
	th.mutex.Lock()
	defer th.mutex.Unlock()

	if len(th.tasks) == 0 {
		return nil
	}

	// swap the first and last elements
	n := len(th.tasks)
	task := th.tasks[0]
	th.tasks[0], th.tasks[n-1] = th.tasks[n-1], th.tasks[0]
	th.tasks = th.tasks[:n-1]

	// notify waiting goroutines that a new task is available
	th.notifyWaiters()

	return task
}

// notifyWaiters notifies waiting goroutines that a new task is available
func (th *taskHeap) notifyWaiters() {
	th.waitersLock.Lock()
	waiters := th.waiters
	th.waiters = nil
	th.waitersLock.Unlock()

	for _, waiter := range waiters {
		waiter.Signal()
	}
}

// Wait waits for the task heap to be non-empty and returns a channel that signals when a task is added to the heap.
func (th *taskHeap) Wait() *sync.Cond {
	// create a new condition variable for the waiters
	waiter := sync.NewCond(&th.waitersLock)

	// add the condition variable to the list of waiters
	th.waitersLock.Lock()
	th.waiters = append(th.waiters, waiter)
	th.waitersLock.Unlock()

	return waiter
}

// Unlock releases the lock.
func (th *taskHeap) Unlock() {
	th.mutex.Unlock()
}

// Lock acquires the lock.
func (th *taskHeap) Lock() {
	th.mutex.Lock()
}
