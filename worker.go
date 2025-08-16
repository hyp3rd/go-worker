package worker

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"golang.org/x/time/rate"
)

type (
	ctxKeyTaskID    struct{}
	ctxKeyCancelled struct{}
)

const (
	// DefaultMaxTasks is the default maximum number of tasks that can be executed at once.
	DefaultMaxTasks = 10
	// DefaultTasksPerSecond is the default rate limit of tasks that can be executed per second.
	DefaultTasksPerSecond = 5
	// DefaultTimeout is the default timeout for tasks.
	DefaultTimeout = 5
	// DefaultRetryDelay is the default delay between retries.
	DefaultRetryDelay = 1
	// DefaultMaxRetries is the default maximum number of retries.
	DefaultMaxRetries = 3
)

// TaskManager is a struct that manages a pool of goroutines that can execute tasks.
//
//nolint:containedctx
type TaskManager struct {
	Registry           sync.Map              // Registry is a map of registered tasks
	Results            chan Result           // Results is the channel of results
	Tasks              chan *Task            // Tasks is the channel of tasks
	RegisterTaskErrors *ewrap.ErrorGroup     // RegisterTaskErrors groups the errors that occurred while registering tasks
	ExecuteTaskErrors  *ewrap.ErrorGroup     // ExecuteTaskErrors groups the errors that occurred while executing tasks
	Cancelled          chan *Task            // Cancelled is the channel of cancelled tasks
	Timeout            time.Duration         // Timeout is the default timeout for tasks
	MaxWorkers         int                   // MaxWorkers is the maximum number of workers that can be started
	MaxTasks           int                   // MaxTasks is the maximum number of tasks that can be executed at once
	RetryDelay         time.Duration         // RetryDelay is the delay between retries
	MaxRetries         int                   // MaxRetries is the maximum number of retries
	limiter            *rate.Limiter         // limiter is a rate limiter that limits the number of tasks that can be executed at once
	wg                 sync.WaitGroup        // wg is a wait group that waits for all tasks to finish
	cancel             chan struct{}         // This channel is used to signal all tasks to cancel
	mutex              sync.RWMutex          // mutex protects the task handling
	quit               chan struct{}         // quit is a channel to signal all goroutines to stop
	ctx                context.Context       // ctx is the context for the task manager
	scheduler          *priorityQueue[*Task] // scheduler is a heap of tasks that are scheduled to be executed
	workerQuit         []chan struct{}       // per-worker quit channels
	metrics            taskMetrics           // metrics holds counters for task states
	workerMutex        sync.Mutex            // protects workerQuit slice
}

// NewTaskManagerWithDefaults creates a new task manager with default values
//   - `maxWorkers`: `runtime.NumCPU()`
//   - `maxTasks`: 10
//   - `tasksPerSecond`: 5
//   - `timeout`: 5 minute
//   - `retryDelay`: 1 second
//   - `maxRetries`: 3
func NewTaskManagerWithDefaults(ctx context.Context) *TaskManager {
	return NewTaskManager(ctx,
		runtime.NumCPU(),
		DefaultMaxTasks,
		DefaultTasksPerSecond,
		DefaultTimeout,
		DefaultRetryDelay,
		DefaultMaxRetries,
	)
}

// NewTaskManager creates a new task manager
//   - `ctx` is the context for the task manager
//   - `maxWorkers` is the number of workers to start, if not specified, the number of CPUs will be used
//   - `maxTasks` is the maximum number of tasks that can be executed at once, defaults to 10
//   - `tasksPerSecond` is the rate limit of tasks that can be executed per second, defaults to 1
//   - `timeout` is the default timeout for tasks, defaults to 5 minute
//   - `retryDelay` is the default delay between retries, defaults to 1 second
//   - `maxRetries` is the default maximum number of retries, defaults to 3
func NewTaskManager(ctx context.Context, maxWorkers int, maxTasks int, tasksPerSecond float64, timeout time.Duration, retryDelay time.Duration, maxRetries int) *TaskManager {
	// avoid values that would lock the program
	maxWorkers = int(math.Max(float64(runtime.NumCPU()), float64(maxWorkers)))
	maxTasks = int(math.Max(DefaultMaxTasks, float64(maxTasks)))
	tasksPerSecond = math.Max(DefaultTasksPerSecond, tasksPerSecond)
	maxRetries = int(math.Max(DefaultMaxRetries, float64(maxRetries)))

	timeout = time.Duration(int64(math.Max(DefaultTimeout, float64(timeout))))
	retryDelay = time.Duration(int64(math.Max(DefaultRetryDelay, float64(retryDelay))))

	// create a channel for cancelled tasks
	cancelled := make(chan *Task, maxTasks)

	// create a context for the task manager that can be used to cancel all tasks
	ctx = context.WithValue(ctx, ctxKeyCancelled{}, cancelled)

	tm := &TaskManager{
		Registry:           sync.Map{},
		Results:            make(chan Result, maxTasks),
		Tasks:              make(chan *Task, maxTasks),
		RegisterTaskErrors: ewrap.NewErrorGroup(),
		ExecuteTaskErrors:  ewrap.NewErrorGroup(),
		Cancelled:          cancelled,
		Timeout:            timeout,
		MaxWorkers:         maxWorkers,
		MaxTasks:           maxTasks,
		RetryDelay:         retryDelay,
		MaxRetries:         maxRetries,
		limiter:            rate.NewLimiter(rate.Limit(tasksPerSecond), maxTasks),
		wg:                 sync.WaitGroup{},
		cancel:             make(chan struct{}), // Initialize the cancel channel
		mutex:              sync.RWMutex{},
		quit:               make(chan struct{}),
		ctx:                ctx,
		scheduler:          newTaskHeap(),
		workerQuit:         make([]chan struct{}, 0, maxWorkers),
		metrics:            taskMetrics{},
	}

	// start the workers
	tm.StartWorkers()

	return tm
}

// IsEmpty checks if the task scheduler queue is empty.
func (tm *TaskManager) IsEmpty() bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	return tm.scheduler.Len() == 0
}

// StartWorkers starts the task manager and its goroutines.
func (tm *TaskManager) StartWorkers() {
	tm.workerMutex.Lock()

	for range tm.MaxWorkers {
		tm.wg.Add(1)

		q := make(chan struct{})

		tm.workerQuit = append(tm.workerQuit, q)
		go tm.workerLoop(q)
	}

	tm.workerMutex.Unlock()

	// start the scheduler
	tm.wg.Add(1)

	go func() {
		defer tm.wg.Done()

		ticker := time.NewTicker(1 * time.Second)

		for {
			select {
			case <-tm.ctx.Done():
				ticker.Stop()

				return
			case <-ticker.C:
				tm.mutex.Lock()

				if tm.scheduler.Len() > 0 {
					t, _ := tm.scheduler.Pop()
					tm.Tasks <- t
				}

				tm.mutex.Unlock()
			}
		}
	}()
	// Start the task re-scheduler
	tm.wg.Add(1)

	go func() {
		defer tm.wg.Done()

		for {
			select {
			case <-tm.quit:
				return
			case task := <-tm.Cancelled:
				// re-add the task back to the scheduler if it is still active
				t, err := tm.GetTask(task.ID)
				if err == nil {
					err := t.ShouldSchedule()
					if err == nil {
						tm.mutex.Lock()
						tm.scheduler.Push(t)

						tm.mutex.Unlock()
					}
				}
			}
		}
	}()
}

// SetMaxWorkers adjusts the number of worker goroutines.
// Increasing the number spawns additional workers; decreasing signals workers to stop.
func (tm *TaskManager) SetMaxWorkers(maxWorkers int) {
	if maxWorkers < 0 {
		return
	}

	tm.workerMutex.Lock()
	defer tm.workerMutex.Unlock()

	current := len(tm.workerQuit)
	switch {
	case maxWorkers > current:
		for i := current; i < maxWorkers; i++ {
			tm.wg.Add(1)

			q := make(chan struct{})

			tm.workerQuit = append(tm.workerQuit, q)
			go tm.workerLoop(q)
		}
	case maxWorkers < current:
		for i := current - 1; i >= maxWorkers; i-- {
			close(tm.workerQuit[i])
			tm.workerQuit = tm.workerQuit[:i]
		}
	}

	tm.MaxWorkers = maxWorkers
}

// RegisterTask registers a new task to the task manager.
func (tm *TaskManager) RegisterTask(ctx context.Context, task *Task) error {
	// set the maximum retries and retry delay for the task
	if task.Retries == 0 || task.Retries > tm.MaxRetries {
		task.Retries = tm.MaxRetries
	}

	if task.RetryDelay == 0 {
		task.RetryDelay = tm.RetryDelay
	}

	if task.Ctx == nil {
		task.Ctx = ctx
	}

	// create a new context for this task
	task.Ctx, task.CancelFunc = context.WithCancel(context.WithValue(task.Ctx, ctxKeyTaskID{}, task.ID))

	// if the task is invalid, send it to the results channel
	err := tm.validateTask(task)
	if err != nil {
		return err
	}

	// Check if context is done before waiting on the limiter
	err = ctx.Err()
	if err != nil {
		tm.cancelTask(task, Cancelled, false)

		return ewrap.Wrapf(err, "failed to get context for task %v", task.ID)
	}

	err = tm.limiter.Wait(ctx)
	if err != nil {
		// the task has been cancelled at this time
		tm.cancelTask(task, RateLimited, !errors.Is(err, context.Canceled))

		return ewrap.Wrapf(err, "failed to wait for limiter for task %v", task.ID)
	}

	// store the task in the registry
	tm.Registry.Store(task.ID, task)

	tm.mutex.Lock()
	// add a wait group for the task
	tm.wg.Add(1)
	// add the task to the scheduler
	tm.scheduler.Push(task)
	// send the task to the Tasks channel

	tm.Tasks <- task

	tm.mutex.Unlock()
	tm.metrics.scheduled.Add(1)

	return nil
}

// RegisterTasks registers multiple tasks to the task manager at once.
func (tm *TaskManager) RegisterTasks(ctx context.Context, tasks ...*Task) {
	for _, task := range tasks {
		err := tm.RegisterTask(ctx, task)
		if err != nil {
			tm.RegisterTaskErrors.Add(err)
		}
	}
}

// Wait waits for all tasks to complete or for the timeout to elapse.
func (tm *TaskManager) Wait(timeout time.Duration) {
	done := make(chan struct{})

	go func() {
		tm.wg.Wait() // Wait for all tasks to be started
		close(done)
	}()

	select {
	case <-done:
		// All tasks have finished
	case <-time.After(timeout):
		// Timeout reached before all tasks finished
	case <-tm.ctx.Done():
		// Context cancelled before all tasks finished
	}

	// close the results and cancelled channels
	close(tm.Results)
	// close(tm.ctx.Value(ctxKeyCancelled{}).(chan *Task))
	close(tm.Cancelled)
}

// Stop stops the task manager and waits for all tasks to finish.
func (tm *TaskManager) Stop() {
	// Signal context cancellation
	<-tm.cancel
	close(tm.quit)
	// wait for all tasks to finish before closing the task manager
	tm.wg.Wait()
	// close the results and cancelled channels
	close(tm.Results)
	close(tm.Cancelled)

	// Close the tasks channel
	close(tm.Tasks)
}

// ExecuteTask executes a task given its ID and returns the result
//   - It gets the task by ID and locks the mutex to access the task data.
//   - If the task has already been started, it cancels it and returns an error.
//   - If the task is invalid, it sends it to the cancelled channel and returns an error.
//   - If the task is already running, it returns an error.
//   - It creates a new context for this task and waits for the result to be available and return it.
//   - It reserves a token from the limiter and waits for the task execution.
//   - If the token reservation fails, it waits for a delay and tries again.
//   - It executes the task and sends the result to the results channel.
//   - If the task execution fails, it retries the task up to max retries with a delay between retries.
//   - If the task fails with all retries exhausted, it cancels the task and returns an error.
func (tm *TaskManager) ExecuteTask(id uuid.UUID, timeout time.Duration) (any, error) {
	// defer tm.wg.Done()

	// get the task
	task, err := tm.GetTask(id)
	if err != nil {
		return nil, err
	}

	// validate the task
	err = tm.validateTask(task)
	if err != nil {
		tm.Cancelled <- task

		return nil, err
	}

	// check if the task is already running
	err = task.ShouldSchedule()
	if err != nil {
		return nil, err
	}

	// create a new context for this task
	taskCtx, cancel := context.WithTimeout(task.Ctx, tm.Timeout)
	defer cancel()

	// reserve a token from the limiter
	reserveToken := tm.limiter.Reserve()

	// if reservation is not okay, wait and retry
	if !reserveToken.OK() {
		task.setRateLimited()
		// not allowed to execute the task yet
		select {
		case <-time.After(reserveToken.Delay()):
		case <-tm.quit:
			tm.cancelTask(task, Cancelled, false)

			return nil, ErrTaskCancelled
		case <-taskCtx.Done():
			tm.cancelTask(task, Cancelled, false)

			return nil, ErrTaskCancelled
		}
	}

	// if reservation is okay, execute the task
	task.setStarted()
	result, err := task.Execute()
	task.setResult(result)

	// if task execution fails, cancel task
	if err != nil {
		task.setError(err)
		tm.cancelTask(task, Failed, false)

		return nil, err
	}

	// if task execution is successful, set task as completed and send result
	task.setCompleted()
	tm.metrics.completed.Add(1)

	tm.Results <- Result{
		Task:   task,
		Result: result,
		Error:  err,
	}

	return result, err
}

// CancelAll cancels all tasks.
func (tm *TaskManager) CancelAll() {
	tm.Registry.Range(func(key, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			return true
		}

		if task.Started.Load() <= 0 {
			// task has not been started yet, remove it from the scheduler
			tm.mutex.Lock()
			tm.scheduler.Remove(task.index)
			tm.mutex.Unlock()

			return true
		}
		// cancel the task
		tm.cancelTask(task, Cancelled, true)

		return true
	})

	// Notify that cancellation is done
	close(tm.cancel)
}

// CancelTask cancels a task by its ID.
func (tm *TaskManager) CancelTask(id uuid.UUID) {
	// get the task
	task, err := tm.GetTask(id)
	if err != nil {
		return
	}

	// cancel the task
	tm.cancelTask(task, Cancelled, true)
}

// GetCancelledTasks gets the cancelled tasks channel
// Example usage:
//
// get the cancelled tasks
// cancelledTasks := tm.GetCancelledTasks()
//
// select {
// case task := <-cancelledTasks:
//
//	fmt.Printf("Task %s was cancelled\n", task.ID.String())
//
// default:
//
//	fmt.Println("No tasks have been cancelled yet")
//	}
func (tm *TaskManager) GetCancelledTasks() <-chan *Task {
	results, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan *Task)
	if !ok {
		return nil
	}

	return results
}

// GetActiveTasks returns the number of active tasks.
func (tm *TaskManager) GetActiveTasks() int {
	return int(tm.limiter.Limit()) - tm.limiter.Burst()
}

// StreamResults streams the results channel.
func (tm *TaskManager) StreamResults() <-chan Result {
	return tm.Results
}

// GetResults gets the results channel.
func (tm *TaskManager) GetResults() []Result {
	results := make([]Result, 0)

	// Create a done channel to signal when all tasks have finished
	done := make(chan struct{})

	// Start a goroutine to read from the Results channel
	go func() {
		for result := range tm.Results {
			results = append(results, result)
		}

		close(done)
	}()

	// Wait for all tasks to finish
	tm.Wait(tm.Timeout)
	// Wait for the results goroutine to finish
	<-done

	return results
}

// GetTask gets a task by its ID.
func (tm *TaskManager) GetTask(id uuid.UUID) (*Task, error) {
	task, ok := tm.Registry.Load(id)
	if !ok {
		return nil, ewrap.Newf("task with ID %v not found", id)
	}

	if task == nil {
		return nil, ewrap.Newf("task with ID %v not found", id)
	}

	res, ok := task.(*Task)
	if !ok {
		return nil, ewrap.Newf("failed to get task with ID %v", id)
	}

	return res, nil
}

// GetTasks gets all tasks.
func (tm *TaskManager) GetTasks() []*Task {
	tasks := make([]*Task, 0, tm.MaxTasks)
	tm.Registry.Range(func(key, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			return false
		}

		tasks = append(tasks, task)

		return true
	})

	return tasks
}

// validateTask validates a task and sends it to the cancelled channel if it is invalid.
func (tm *TaskManager) validateTask(task *Task) error {
	// if the task is invalid, send it to the cancelled channel
	err := task.IsValid()
	if err != nil {
		cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan *Task)
		if !ok {
			task.setCancelled()
			task.setError(err)

			return ErrTaskCancelled
		}

		select {
		case cancelled <- task:
			// task sent successfully
		default:
			// channel buffer is full, task not sent
		}

		return err
	}

	return nil
}

// workerLoop executes tasks until signalled to stop.
func (tm *TaskManager) workerLoop(stop chan struct{}) {
	defer tm.wg.Done()

	for {
		select {
		case task, ok := <-tm.Tasks:
			if (!ok || task == nil || task.ID == uuid.Nil) && tm.IsEmpty() {
				return
			}

			_, err := tm.ExecuteTask(task.ID, tm.Timeout)
			if err != nil {
				tm.ExecuteTaskErrors.Add(err)
			}
		case <-tm.quit:
			return
		case <-stop:
			return
		}
	}
}

// retryTask retries a task with exponential backoff, respecting task and manager context cancellation.
func (tm *TaskManager) retryTask(task *Task) {
	timer := time.NewTimer(task.RetryDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		tm.mutex.Lock()

		err := tm.RegisterTask(task.Ctx, task)
		if err != nil {
			tm.RegisterTaskErrors.Add(err)
		}

		tm.mutex.Unlock()
	case <-task.Ctx.Done():
		return
	case <-tm.quit:
		return
	}
}

// cancelTask cancels a task.
func (tm *TaskManager) cancelTask(task *Task, status TaskStatus, notifyWG bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	task.CancelFunc()
	// set the cancelled time
	task.setCancelled()
	// set the task status
	task.Status = status

	// wait for the task to be cancelled
	task.WaitCancelled()

	if notifyWG || status == Cancelled {
		tm.wg.Done()
	}

	// update the task in the registry
	tm.Registry.Store(task.ID, task)

	//nolint:exhaustive // we handle here only cancelled or failed cases.
	switch status {
	case Cancelled:
		tm.metrics.cancelled.Add(1)
	case Failed:
		tm.metrics.failed.Add(1)
	}

	// send the task to the cancelled channel
	cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan *Task)

	if ok {
		select {
		case cancelled <- task:
		case <-tm.quit:
			return
		default:
		}
	}

	// if the task has retries remaining and status is Failed, add it back to the queue with a delay
	if task.Retries > 0 && status != Cancelled {
		task.setQueued()
		task.Retries--
		task.RetryDelay *= 2
		tm.retryTask(task)
	} else if status == Failed {
		task.setFailed(ewrap.Newf("task ID %v failed after reaching the maximum number of retries %v", task.ID, tm.MaxTasks))
	}
}
