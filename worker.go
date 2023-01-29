package worker

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// TaskManager is a struct that manages a pool of goroutines that can execute tasks
type TaskManager struct {
	Registry sync.Map         // Registry is a map of registered tasks
	Results  chan interface{} // Results is the channel of results
	taskHeap taskHeap         // heap of tasks
	limiter  *rate.Limiter    // limiter is a rate limiter that limits the number of tasks that can be executed at once
	wg       sync.WaitGroup   // wg is a wait group that waits for all tasks to finish
	mutex    sync.RWMutex     // mutex protects the task handling
}

// NewTaskManager creates a new task manager
//   - `maxTasks` is the maximum number of tasks that can be executed at once, defaults to 1
//   - `tasksPerSecond` is the rate limit of tasks that can be executed per second, defaults to 1
func NewTaskManager(maxTasks int, tasksPerSecond float64) Service {
	if maxTasks <= 0 {
		maxTasks = 1
	}
	// avoid values that would lock the program
	if tasksPerSecond <= 0 {
		tasksPerSecond = 1
	}

	tm := &TaskManager{
		Registry: sync.Map{},
		Results:  make(chan interface{}, maxTasks),
		taskHeap: make(taskHeap, 0, maxTasks),
		limiter:  rate.NewLimiter(rate.Limit(tasksPerSecond), maxTasks),
	}
	// initialize the heap of tasks
	heap.Init(&tm.taskHeap)

	return tm
}

// RegisterTask registers a new task to the task manager
func (tm *TaskManager) RegisterTask(tasks ...Task) {
	for _, task := range tasks {
		tm.mutex.RLock()
		defer tm.mutex.RUnlock()
		if task.IsValid() != nil {
			tm.Results <- task
			continue
		}
		// add a wait group for the task
		tm.wg.Add(1)
		// create a context for the task and store it in the task
		task.Ctx, task.Cancel = context.WithCancel(context.Background())
		// store the task in the registry
		tm.Registry.Store(task.ID, task)
		// add the task to the heap
		heap.Push(&tm.taskHeap, task)
	}
}

// Start starts the task manager and its goroutines
//   - `numWorkers` is the number of workers to start, if not specified, the number of CPUs will be used
func (tm *TaskManager) Start(numWorkers int) {
	// if numWorkers is not specified, use the number of CPUs
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// start the workers
	for i := 0; i < numWorkers; i++ {
		// add a wait group for the worker
		tm.wg.Add(1)
		// start the worker
		go func(i int) {
			tm.worker(i)
			tm.wg.Done()
		}(i)
	}

	// close the results channel when all tasks are done
	go func() {
		tm.wg.Wait()
		close(tm.Results)
	}()
}

// Stop stops the task manager and its goroutines
func (tm *TaskManager) Stop() {
	// tm.wg.Wait()
	close(tm.Results)
}

// GetResults gets the results channel
func (tm *TaskManager) GetResults() <-chan interface{} {
	return tm.Results
}

// GetTask gets a task by its ID
func (tm *TaskManager) GetTask(id uuid.UUID) (task Task, ok bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	t, ok := tm.Registry.Load(id)
	if !ok {
		return
	}
	task, ok = t.(Task)
	return
}

// GetTasks gets all tasks
func (tm *TaskManager) GetTasks() []Task {
	tasks := make([]Task, 0)
	tm.Registry.Range(func(key, value interface{}) bool {
		task, ok := value.(*Task)
		if !ok {
			return true
		}
		tasks = append(tasks, *task)
		return true
	})
	return tasks
}

// CancelAll cancels all tasks
func (tm *TaskManager) CancelAll() {
	tm.Registry.Range(func(key, value interface{}) bool {
		task := value.(Task)
		// cancel the task
		tm.cancelTask(&task, Cancelled, true)

		return true
	})
	// wait for all tasks to finish
	tm.wg.Wait()
}

// CancelTask cancels a task by its ID
func (tm *TaskManager) CancelTask(id uuid.UUID) {
	// get the task
	task, ok := tm.GetTask(id)
	if !ok {
		return
	}
	// cancel the task
	tm.cancelTask(&task, Cancelled, true)
}

// ExecuteTask executes a task given its ID and returns the result
func (tm *TaskManager) ExecuteTask(id uuid.UUID) (interface{}, error) {
	// get the task
	task, ok := tm.GetTask(id)
	if !ok {
		// task not found
		return nil, ErrTaskNotFound
	}
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	// execute the task
	tm.executeTask(&task)
	// drain the results channel and return the result
	res := <-tm.Results
	return res, nil
}

// worker is a goroutine that executes tasks
func (tm *TaskManager) worker(workerID int) {
	for {
		if tm.taskHeap.Len() == 0 {
			break
		}
		// pop the next task from the heap
		// newTask := heap.Pop(&tm.taskHeap).(Task)
		newTask := tm.taskHeap.Pop().(Task)

		// check if the task has been cancelled before starting it and if so, skip it and continue
		if newTask.Cancelled.Load() > 0 {
			continue
		}

		// wait for the task to be ready to execute
		if err := tm.limiter.Wait(newTask.Ctx); err != nil {
			// the task has been cancelled at this time
			tm.cancelTask(&newTask, RateLimited, err != context.Canceled)
			continue
		}

		// execute the task
		go tm.executeTask(&newTask)
	}
}

// executeTask executes a task
func (tm *TaskManager) executeTask(task *Task) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	defer tm.wg.Done()

	// reserve a token from the limiter
	r := tm.limiter.ReserveN(time.Now(), 1)
	if !r.OK() {
		// not allowed to execute the task yet
		return
	}

	// create a timer for the delay
	t := time.After(r.Delay())
	// wait for the task to be ready
	select {
	case <-t:
		// do nothing
	case <-task.Ctx.Done():
		return
	}

	// set the started time
	task.setStarted()
	// update the task in the registry
	tm.Registry.Store(task.ID, task)

	// execute the task
	result := task.Fn()

	// set the completed time
	task.setCompleted()
	// update the task in the registry
	tm.Registry.Store(task.ID, task)
	// send the result to the results channel
	tm.Results <- result
}

// cancelTask cancels a task
func (tm *TaskManager) cancelTask(task *Task, reason CancelReason, notifyWG bool) {
	if notifyWG {
		defer tm.wg.Done()
	}
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	task.Cancel()
	// set the cancelled time
	task.setCancelled()
	// set the cancel reason
	task.CancelReason = reason
	// update the task in the registry
	tm.Registry.Store(task.ID, task)
}
