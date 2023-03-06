package worker

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type (
	ctxKeyTaskID    struct{}
	ctxKeyCancelled struct{}
)

const (
	// DefaultMaxTasks is the default maximum number of tasks that can be executed at once
	DefaultMaxTasks = 10
	// DefaultTasksPerSecond is the default rate limit of tasks that can be executed per second
	DefaultTasksPerSecond = 1
	// DefaultTimeout is the default timeout for tasks
	DefaultTimeout = 5
	// DefaultRetryDelay is the default delay between retries
	DefaultRetryDelay = 1
	// DefaultMaxRetries is the default maximum number of retries
	DefaultMaxRetries = 3
)

// TaskManager is a struct that manages a pool of goroutines that can execute tasks
type TaskManager struct {
	Registry   sync.Map        // Registry is a map of registered tasks
	Results    chan Result     // Results is the channel of results
	Tasks      chan Task       // Tasks is the channel of tasks
	Cancelled  chan Task       // Cancelled is the channel of cancelled tasks
	Timeout    time.Duration   // Timeout is the default timeout for tasks
	MaxWorkers int             // MaxWorkers is the maximum number of workers that can be started
	MaxTasks   int             // MaxTasks is the maximum number of tasks that can be executed at once
	RetryDelay time.Duration   // RetryDelay is the delay between retries
	MaxRetries int             // MaxRetries is the maximum number of retries
	limiter    *rate.Limiter   // limiter is a rate limiter that limits the number of tasks that can be executed at once
	wg         sync.WaitGroup  // wg is a wait group that waits for all tasks to finish
	mutex      sync.RWMutex    // mutex protects the task handling
	quit       chan struct{}   // quit is a channel to signal all goroutines to stop
	ctx        context.Context // ctx is the context for the task manager
	scheduler  *taskHeap       // scheduler is a heap of tasks that are scheduled to be executed
}

// NewTaskManager creates a new task manager
//   - `maxWorkers` is the number of workers to start, if not specified, the number of CPUs will be used
//   - `maxTasks` is the maximum number of tasks that can be executed at once, defaults to 10
//   - `tasksPerSecond` is the rate limit of tasks that can be executed per second, defaults to 1
//   - `timeout` is the default timeout for tasks, defaults to 5 minute
//   - `retryDelay` is the default delay between retries, defaults to 1 second
//   - `maxRetries` is the default maximum number of retries, defaults to 3
func NewTaskManager(maxWorkers int, maxTasks int, tasksPerSecond float64, timeout time.Duration, retryDelay time.Duration, maxRetries int) *TaskManager {
	// avoid values that would lock the program
	maxWorkers = int(math.Max(float64(runtime.NumCPU()), float64(maxWorkers)))
	maxTasks = int(math.Max(DefaultMaxTasks, float64(maxTasks)))
	tasksPerSecond = math.Max(DefaultTasksPerSecond, tasksPerSecond)
	maxRetries = int(math.Max(DefaultMaxRetries, float64(maxRetries)))

	timeout = time.Duration(int64(math.Max(DefaultTimeout, float64(timeout))))
	retryDelay = time.Duration(int64(math.Max(DefaultRetryDelay, float64(retryDelay))))

	// create a channel for cancelled tasks
	cancelled := make(chan Task, maxTasks)

	// create a context for the task manager that can be used to cancel all tasks
	ctx := context.WithValue(context.Background(), ctxKeyCancelled{}, cancelled)

	tm := &TaskManager{
		Registry:   sync.Map{},
		Results:    make(chan Result, maxTasks),
		Tasks:      make(chan Task),
		Cancelled:  cancelled,
		Timeout:    timeout,
		MaxWorkers: maxWorkers,
		MaxTasks:   maxTasks,
		RetryDelay: retryDelay,
		MaxRetries: maxRetries,
		limiter:    rate.NewLimiter(rate.Limit(tasksPerSecond), maxTasks),
		wg:         sync.WaitGroup{},
		mutex:      sync.RWMutex{},
		quit:       make(chan struct{}),
		ctx:        ctx,
		scheduler:  newTaskHeap(),
	}

	// initialize the heap of tasks
	heap.Init(tm.scheduler)

	// start the workers
	tm.StartWorkers()

	return tm
}

// StartWorkers starts the task manager and its goroutines
func (tm *TaskManager) StartWorkers() {
	// start the workers
	for i := 0; i < tm.MaxWorkers; i++ {
		// add a wait group for the worker
		tm.wg.Add(1)
		go func() {
			defer tm.wg.Done()
			for {
				select {
				case <-tm.quit:
					return
				// case t := <-tm.Tasks:
				case t := <-tm.Tasks:
					// safety check
					if tm.scheduler.Len() == 0 {
						if t.ShouldExecute() == nil {
							// add the task back to the scheduler
							heap.Push(tm.scheduler, t)
						}
						break
					}

					tm.mutex.Lock()
					// pop the next task from the heap
					task, ok := heap.Pop(tm.scheduler).(Task)
					tm.mutex.Unlock()
					// safety check
					if !ok {
						return
					}

					// check if the task has been cancelled before starting it and if so, skip it and continue
					select {
					case <-task.Ctx.Done():
						tm.cancelTask(&task, Cancelled, false)
						continue
					default:
					}

					// check if the task has been cancelled before starting it and if so, skip it and continue
					if task.ShouldExecute() != nil {
						continue
					}

					// wait for the task to be ready to execute
					waitCtx, waitCancel := context.WithTimeout(task.Ctx, tm.Timeout)
					if err := tm.limiter.Wait(waitCtx); err != nil {
						// the task has been cancelled at this time
						tm.cancelTask(&task, RateLimited, err != context.Canceled)
						waitCancel()
						continue
					}
					waitCancel()

					// execute the task
					tm.ExecuteTask(task.ID, tm.Timeout)

					// check if all tasks have been done
					if tm.limiter.Allow() {
						select {
						case <-tm.Tasks:
							// there is still a task in the queue, continue processing
							continue
						default:
							// all tasks have been done, close the channels
							tm.Wait(tm.Timeout)
							return

						}
					}

				case <-time.After(time.Millisecond * 10):
					// wait for a short duration to avoid busy waiting
					// check if the task manager has been closed
					select {
					case <-tm.quit:
						return
					default:
						continue
					}
				}
			}
		}()
	}
}

// RegisterTask registers a new task to the task manager
func (tm *TaskManager) RegisterTask(ctx context.Context, task Task) error {
	// set the maximum retries and retry delay for the task
	task.Retries = tm.MaxRetries
	task.RetryDelay = tm.RetryDelay

	defer func() {
		// recover from any panics and send the task to the `Cancelled` channel
		if r := recover(); r != nil {
			cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan Task)
			if ok {
				select {
				case cancelled <- task:
					// task sent successfully
					return
				default:
					// channel buffer is full, task not sent
				}
			}
		}
	}()

	// if the task is invalid, send it to the results channel
	err := task.IsValid()
	if err != nil {
		cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan Task)
		if !ok {
			return err
		}
		select {
		case cancelled <- task:
			// task sent successfully
		default:
			// channel buffer is full, task not sent
		}
		return err
	}

	// add a wait group for the task
	tm.wg.Add(1)

	// create a new context for this task
	task.Ctx, task.CancelFunc = context.WithCancel(context.WithValue(ctx, ctxKeyTaskID{}, task.ID))
	defer func() {
		if ctx.Err() != nil {
			tm.cancelTask(&task, Cancelled, false)
		}
	}()
	if err := tm.limiter.Wait(ctx); err != nil {
		// the task has been cancelled at this time
		tm.cancelTask(&task, RateLimited, err != context.Canceled)
		return err
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// store the task in the registry
	tm.Registry.Store(task.ID, &task)

	// add the task to the scheduler
	heap.Push(tm.scheduler, task)

	// send the task to the NewTasks channel
	tm.Tasks <- task

	return nil
}

// RegisterTasks registers multiple tasks to the task manager at once
func (tm *TaskManager) RegisterTasks(ctx context.Context, tasks ...Task) {
	for _, task := range tasks {
		tm.RegisterTask(ctx, task)
	}
}

// Wait waits for all tasks to complete or for the timeout to elapse
func (tm *TaskManager) Wait(timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// flag to indicate if any tasks have been executed
	executed := false

	for {
		select {
		case <-tm.quit:
			// task manager has been closed, cancel all tasks
			tm.CancelAll()
			close(tm.Results)
			close(tm.ctx.Value(ctxKeyCancelled{}).(chan Task))

		case <-timer.C:
			// timeout reached, cancel all tasks
			tm.CancelAll()
		default:
			// check if any tasks have been executed
			if !executed {
				// no tasks have been executed, return immediately
				return
			}
			// wait for all tasks to finish
			// tm.scheduler.Wait()
			tm.wg.Wait()
			// close the results and cancelled channels
			close(tm.Results)
			close(tm.ctx.Value(ctxKeyCancelled{}).(chan Task))
		}
	}
}

// Close stops the task manager and waits for all tasks to finish
func (tm *TaskManager) Close() {
	// close the tasks channel
	defer close(tm.Tasks)
	// close the quit channel
	defer close(tm.quit)
	// wait for all tasks to finish before closing the task manager
	tm.Wait(tm.Timeout)
}

// CancelAllAndWait cancels all tasks and waits for them to finish
func (tm *TaskManager) CancelAllAndWait() {
	tm.Registry.Range(func(key, value interface{}) bool {
		task := value.(Task)
		// cancel the task
		tm.cancelTask(&task, Cancelled, true)

		return true
	})

	// wait for all tasks to be cancelled
	tm.Registry.Range(func(key, value interface{}) bool {
		task := value.(Task)
		tm.mutex.Lock()
		defer tm.mutex.Unlock()
		task.WaitCancelled()
		return true
	})

	// wait for all tasks to finish
	tm.wg.Wait()
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
	task, err := tm.GetTask(id)
	if err != nil {
		return
	}

	// cancel the task
	tm.cancelTask(task, Cancelled, true)
}

// GetActiveTasks returns the number of active tasks
func (tm *TaskManager) GetActiveTasks() int {
	return int(tm.limiter.Limit()) - tm.limiter.Burst()
}

// GetResults gets the results channel
func (tm *TaskManager) GetResults() <-chan Result {
	return tm.Results
}

// GetCancelled gets the cancelled tasks channel
func (tm *TaskManager) GetCancelled() <-chan Task {
	return tm.ctx.Value(ctxKeyCancelled{}).(chan Task)
}

// GetTask gets a task by its ID
func (tm *TaskManager) GetTask(id uuid.UUID) (task *Task, err error) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	t, ok := tm.Registry.Load(id)
	if !ok {
		return nil, fmt.Errorf("task with ID %v not found", id)
	}

	if t == nil {
		return nil, fmt.Errorf("task with ID %v not found", id)
	}

	task, ok = t.(*Task)
	if !ok {
		return nil, fmt.Errorf("failed to get task with ID %v", id)
	}
	return
}

// GetTasks gets all tasks
func (tm *TaskManager) GetTasks() []Task {
	tasks := make([]Task, 0, tm.MaxTasks)
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
func (tm *TaskManager) ExecuteTask(id uuid.UUID, timeout time.Duration) (interface{}, error) {
	defer tm.wg.Done()
	// get the task
	task, err := tm.GetTask(id)
	if err != nil {
		return nil, err
	}

	// Lock the mutex to access the task data
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	// check if the context has been cancelled before checking if the task is already running
	select {
	case <-task.Ctx.Done():
		tm.cancelTask(task, Cancelled, false)
		return nil, ErrTaskAlreadyStarted
	default:
	}

	// if the task is invalid, send it to the results channel
	err = task.IsValid()
	if err != nil {
		cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan Task)
		if !ok {
			return nil, ErrTaskCancelled
		}
		select {
		case cancelled <- *task:
			// task sent successfully
		default:
			// channel buffer is full, task not sent
		}
		return nil, err
	}

	// check if the task is already running
	err = task.ShouldExecute()
	if err != nil {
		return nil, err
	}

	// create a new context for this task
	ctx, cancel := context.WithTimeout(context.WithValue(task.Ctx, ctxKeyTaskID{}, task.ID), tm.Timeout)
	defer cancel()

	// wait for the result to be available and return it
	for {
		select {
		case res := <-tm.Results:
			if res.Task.ID == task.ID {
				return res.Result, nil
			}
		case cancelledTask := <-tm.GetCancelled():
			if cancelledTask.ID == task.ID {
				// the task was cancelled before it could complete
				return nil, ErrTaskCancelled
			}
		case <-ctx.Done():
			// the task has timed out
			tm.CancelTask(task.ID)
			return nil, ErrTaskTimeout
		case <-time.After(timeout):
			return nil, ErrTaskTimeout
		default:
			// execute the task
			// reserve a token from the limiter
			r := tm.limiter.Reserve()

			if !r.OK() {
				// not allowed to execute the task yet
				waitTime := r.Delay()
				select {
				case <-tm.quit:
					// the task manager has been closed
					tm.cancelTask(task, Cancelled, false)
					return nil, ErrTaskCancelled
				case <-task.Ctx.Done():
					// the task has been cancelled
					tm.cancelTask(task, Cancelled, false)
					return nil, ErrTaskCancelled
				case <-time.After(waitTime):
					// continue with the task execution
					continue
				}
			}

			select {
			case <-tm.quit:
				// the task manager has been closed
				tm.cancelTask(task, Cancelled, false)
				return nil, ErrTaskCancelled
			case <-task.Ctx.Done():
				// the task has been cancelled
				tm.cancelTask(task, Cancelled, false)
				return nil, ErrTaskCancelled
			default:
				task.setStarted()

				// execute the task
				result, err := task.Fn()
				if err != nil {
					// task failed, retry up to max retries with delay between retries
					if task.Retries > 0 {
						task.Retries--
						tm.retryTask(task)
						return nil, err
					}

					// task failed, no more retries
					tm.cancelTask(task, Failed, false)
					return nil, err
				}
				// task completed successfully
				task.setCompleted()
				// send the result to the results channel
				tm.Results <- Result{
					Task:   task,
					Result: result,
					Error:  err,
				}

				return result, nil
			}
		}
	}
}

// retryTask retries a task up to its maximum number of retries with a delay between retries
func (tm *TaskManager) retryTask(task *Task) {
	task.setRetryDelay(tm.RetryDelay)

	// wait for the retry delay to pass
	timer := time.NewTimer(tm.RetryDelay)
	defer timer.Stop()
	<-timer.C

	// re-enqueue the task
	tm.RegisterTask(task.Ctx, *task)
}

// cancelTask cancels a task
func (tm *TaskManager) cancelTask(task *Task, status TaskStatus, notifyWG bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	task.CancelFunc()
	// set the cancelled time
	task.setCancelled()
	// set the task status
	task.Status = status

	if notifyWG || status == Cancelled {
		tm.wg.Done()
	}
	// heap.Remove(tm.scheduler, task.index)

	// update the task in the registry
	tm.Registry.Store(task.ID, task)

	// send the task to the cancelled channel
	cancelled, ok := tm.ctx.Value(ctxKeyCancelled{}).(chan Task)

	if !ok {
		return
	}
	select {
	case cancelled <- *task:
		// task sent successfully
	default:
		// channel buffer is full, task not sent
	}

	// if the task has retries remaining, add it back to the queue with a delay
	if task.Retries > 0 && status != Cancelled {
		task.Retries--
		time.AfterFunc(task.RetryDelay, func() {
			tm.RegisterTask(tm.ctx, *task)
		})
	}
}
