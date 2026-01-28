package worker

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"golang.org/x/time/rate"
)

const (
	// DefaultMaxTasks is the default maximum number of tasks that can be executed at once.
	DefaultMaxTasks = 10
	// DefaultTasksPerSecond is the default rate limit of tasks that can be executed per second.
	DefaultTasksPerSecond = 5
	// DefaultTimeout is the default timeout for tasks.
	DefaultTimeout = 5 * time.Minute
	// DefaultRetryDelay is the default delay between retries.
	DefaultRetryDelay = 1 * time.Second
	// DefaultMaxRetries is the default maximum number of retries.
	DefaultMaxRetries = 3
	// ErrMsgContextDone is the error message used when the context is done.
	ErrMsgContextDone = "context done"
)

// TaskManager is a struct that manages a pool of goroutines that can execute tasks.
//
//nolint:containedctx
type TaskManager struct {
	registryMu sync.RWMutex
	registry   map[uuid.UUID]*Task

	queueMu   sync.Mutex
	queueCond *sync.Cond
	scheduler *priorityQueue[*Task]

	jobs chan *Task

	limiter *rate.Limiter

	wg       sync.WaitGroup // tracks tasks
	workerWg sync.WaitGroup // tracks workers/scheduler/retry goroutines

	ctx    context.Context
	cancel context.CancelFunc

	maxWorkers int
	maxTasks   int
	timeout    time.Duration
	retryDelay time.Duration
	maxRetries int

	workerMutex sync.Mutex
	workerQuit  []chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	accepting   atomic.Bool

	metrics taskMetrics
	results *resultBroadcaster
	hooks   atomic.Pointer[TaskHooks]
	tracer  atomic.Pointer[tracerHolder]
	otel    atomic.Pointer[otelMetrics]

	durableBackend      DurableBackend
	durableHandlers     map[string]DurableHandlerSpec
	durableCodec        DurableCodec
	durableLease        time.Duration
	durablePollInterval time.Duration
	durableBatchSize    int
	durableEnabled      bool

	retentionMu        sync.RWMutex
	retention          retentionConfig
	retentionLastCheck atomic.Int64
	retentionPrune     atomic.Bool
	retentionOnce      sync.Once
}

// NewTaskManagerWithDefaults creates a new task manager with default values.
//   - maxWorkers: runtime.NumCPU()
//   - maxTasks: 10
//   - tasksPerSecond: 5
//   - timeout: 5 minutes
//   - retryDelay: 1 second
//   - maxRetries: 3
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

func limiterBurst(maxWorkers, maxTasks int) int {
	burst := min(maxWorkers, maxTasks)
	if burst <= 0 {
		return 1
	}

	return burst
}

// NewTaskManagerWithOptions creates a new task manager using functional options.
func NewTaskManagerWithOptions(ctx context.Context, opts ...TaskManagerOption) *TaskManager {
	if ctx == nil {
		panic("nil context")
	}

	cfg := defaultTaskManagerConfig()

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return newTaskManagerFromConfig(ctx, cfg)
}

// NewTaskManager creates a new task manager.
//   - ctx is the context for the task manager
//   - maxWorkers is the number of workers to start, if <=0, the number of CPUs will be used
//   - maxTasks is the maximum number of tasks that can be queued at once, defaults to 10
//   - tasksPerSecond is the rate limit of tasks that can be executed per second; <=0 disables rate limiting
//     The limiter uses a burst size of min(maxWorkers, maxTasks) for deterministic throttling.
//   - timeout is the default timeout for tasks, defaults to 5 minutes
//   - retryDelay is the default delay between retries, defaults to 1 second
//   - maxRetries is the default maximum number of retries, defaults to 3 (0 disables retries)
func NewTaskManager(
	ctx context.Context,
	maxWorkers, maxTasks int,
	tasksPerSecond float64,
	timeout, retryDelay time.Duration,
	maxRetries int,
) *TaskManager {
	cfg := defaultTaskManagerConfig()
	cfg.maxWorkers = maxWorkers
	cfg.maxTasks = maxTasks
	cfg.tasksPerSecond = tasksPerSecond
	cfg.timeout = timeout
	cfg.retryDelay = retryDelay
	cfg.maxRetries = maxRetries

	return newTaskManagerFromConfig(ctx, cfg)
}

func newTaskManagerFromConfig(ctx context.Context, cfg taskManagerConfig) *TaskManager {
	if ctx == nil {
		panic("nil context")
	}

	ctx, cancel := context.WithCancel(ctx)

	maxWorkers := cfg.maxWorkers
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}

	maxTasks := cfg.maxTasks
	if maxTasks <= 0 {
		maxTasks = DefaultMaxTasks
	}

	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	retryDelay := cfg.retryDelay
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}

	maxRetries := cfg.maxRetries
	if maxRetries < 0 {
		maxRetries = DefaultMaxRetries
	}

	burst := limiterBurst(maxWorkers, maxTasks)

	var limiter *rate.Limiter
	if cfg.tasksPerSecond <= 0 {
		limiter = rate.NewLimiter(rate.Inf, burst)
	} else {
		limiter = rate.NewLimiter(rate.Limit(cfg.tasksPerSecond), burst)
	}

	durableEnabled := cfg.durableBackend != nil
	if cfg.durableCodec == nil {
		cfg.durableCodec = ProtoDurableCodec{}
	}

	tm := &TaskManager{
		registry:            make(map[uuid.UUID]*Task),
		scheduler:           newTaskHeap(),
		jobs:                make(chan *Task, maxWorkers),
		limiter:             limiter,
		ctx:                 ctx,
		cancel:              cancel,
		maxWorkers:          maxWorkers,
		maxTasks:            maxTasks,
		timeout:             timeout,
		retryDelay:          retryDelay,
		maxRetries:          maxRetries,
		workerQuit:          make([]chan struct{}, 0, maxWorkers),
		results:             newResultBroadcaster(maxTasks),
		durableBackend:      cfg.durableBackend,
		durableHandlers:     cfg.durableHandlers,
		durableCodec:        cfg.durableCodec,
		durableLease:        cfg.durableLease,
		durablePollInterval: cfg.durablePollInterval,
		durableBatchSize:    cfg.durableBatchSize,
		durableEnabled:      durableEnabled,
	}

	tm.queueCond = sync.NewCond(&tm.queueMu)
	tm.accepting.Store(true)

	// start the workers
	tm.StartWorkers(ctx)

	return tm
}

// IsEmpty checks if the task scheduler queue is empty.
func (tm *TaskManager) IsEmpty() bool {
	tm.queueMu.Lock()
	defer tm.queueMu.Unlock()

	return tm.scheduler.Len() == 0
}

// StartWorkers starts the task manager's workers and scheduler (idempotent).
func (tm *TaskManager) StartWorkers(ctx context.Context) {
	if ctx == nil {
		panic("nil context")
	}

	if tm.cancel == nil {
		panic("task manager not initialized")
	}

	workerCtx := ctx

	tm.startOnce.Do(func() {
		tm.workerMutex.Lock()

		for range tm.maxWorkers {
			tm.spawnWorker(workerCtx)
		}

		tm.workerMutex.Unlock()

		tm.workerWg.Add(1)

		if tm.durableEnabled {
			go tm.durableLoop(workerCtx)
		} else {
			go tm.schedulerLoop(workerCtx)
		}
	})
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
			tm.spawnWorker(tm.ctx)
		}
	case maxWorkers < current:
		for i := current - 1; i >= maxWorkers; i-- {
			close(tm.workerQuit[i])
			tm.workerQuit = tm.workerQuit[:i]
		}
	default:
		// no change
	}

	tm.maxWorkers = maxWorkers
}

// RegisterTask registers a new task to the task manager.
func (tm *TaskManager) RegisterTask(ctx context.Context, task *Task) error {
	err := tm.validateRegisterArgs(ctx, task)
	if err != nil {
		return err
	}

	err = tm.prepareTaskForRegister(ctx, task)
	if err != nil {
		return err
	}

	err = tm.storeTask(task)
	if err != nil {
		return err
	}

	tm.wg.Add(1)

	err = tm.enqueue(task)
	if err != nil {
		tm.registryMu.Lock()
		delete(tm.registry, task.ID)
		tm.registryMu.Unlock()

		tm.wg.Done()

		return err
	}

	task.setQueued()
	tm.metrics.scheduled.Add(1)
	tm.hookQueued(task)

	return nil
}

// RegisterTasks registers multiple tasks to the task manager at once.
func (tm *TaskManager) RegisterTasks(ctx context.Context, tasks ...*Task) error {
	var joined error

	for _, task := range tasks {
		err := tm.RegisterTask(ctx, task)
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}

	return joined
}

// Wait waits for all tasks to complete or context cancellation.
func (tm *TaskManager) Wait(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	done := make(chan struct{})

	go func() {
		tm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ewrap.Wrap(ctx.Err(), ErrMsgContextDone)
	case <-tm.ctx.Done():
		return ewrap.Wrap(tm.ctx.Err(), "task manager context done")
	}
}

// StopGraceful stops accepting new tasks and waits for completion before stopping workers.
func (tm *TaskManager) StopGraceful(ctx context.Context) error {
	tm.accepting.Store(false)

	err := tm.Wait(ctx)
	if err != nil {
		return err
	}

	tm.StopNow()

	return tm.waitWorkers(ctx)
}

// StopNow cancels running tasks and stops workers immediately.
func (tm *TaskManager) StopNow() {
	tm.stopOnce.Do(func() {
		tm.accepting.Store(false)
		tm.cancel()
		tm.queueCond.Broadcast()
		tm.cancelAllInternal()
		tm.results.Close()
	})
}

// CancelAll cancels all tasks.
func (tm *TaskManager) CancelAll() {
	tm.cancelAllInternal()
}

// CancelTask cancels a task by its ID.
func (tm *TaskManager) CancelTask(id uuid.UUID) error {
	task, err := tm.GetTask(id)
	if err != nil {
		return err
	}

	if task.CancelFunc != nil {
		task.CancelFunc()
	}

	status := task.Status()

	// If queued or in backoff, remove and finish immediately.
	if status == Queued || status == RateLimited {
		tm.queueMu.Lock()

		if task.index >= 0 {
			tm.scheduler.Remove(task.index)
		}

		tm.queueMu.Unlock()

		tm.finishTask(task, Cancelled, nil, ErrTaskCancelled)
	}

	return nil
}

// GetActiveTasks returns the number of running tasks.
func (tm *TaskManager) GetActiveTasks() int {
	return int(tm.metrics.running.Load())
}

// GetResults returns a results channel (compatibility shim for legacy API).
// Use SubscribeResults for explicit unsubscription and buffer control.
func (tm *TaskManager) GetResults() <-chan Result {
	results, unsubscribe := tm.SubscribeResults(DefaultMaxTasks)
	if tm.ctx == nil {
		return results
	}

	go func() {
		<-tm.ctx.Done()
		unsubscribe()
	}()

	return results
}

// SubscribeResults returns a results channel and an unsubscribe function.
func (tm *TaskManager) SubscribeResults(buffer int) (<-chan Result, func()) {
	return tm.results.Subscribe(buffer)
}

// SetResultsDropPolicy configures how full subscriber buffers are handled.
func (tm *TaskManager) SetResultsDropPolicy(policy ResultDropPolicy) {
	tm.results.SetDropPolicy(policy)
}

// ExecuteTask executes a task given its ID and returns the result.
func (tm *TaskManager) ExecuteTask(ctx context.Context, id uuid.UUID, timeout time.Duration) (any, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	if tm.durableEnabled {
		return nil, ewrap.New("ExecuteTask is not supported when durable backend is enabled")
	}

	err := ctx.Err()
	if err != nil {
		return nil, ewrap.Wrap(err, ErrMsgContextDone)
	}

	task, err := tm.GetTask(id)
	if err != nil {
		return nil, err
	}

	removed := tm.removeFromQueue(task)

	err = tm.waitForPermit(ctx)
	if err != nil {
		tm.restoreQueued(task, removed)

		return nil, err
	}

	return tm.runTask(ctx, task, timeout)
}

// GetTask gets a task by its ID.
func (tm *TaskManager) GetTask(id uuid.UUID) (*Task, error) {
	tm.registryMu.RLock()
	task, ok := tm.registry[id]
	tm.registryMu.RUnlock()

	if !ok || task == nil {
		return nil, ewrap.Wrapf(ErrTaskNotFound, "task with ID %v not found", id)
	}

	return task, nil
}

// GetTasks gets all tasks.
func (tm *TaskManager) GetTasks() []*Task {
	tm.registryMu.RLock()
	defer tm.registryMu.RUnlock()

	tasks := make([]*Task, 0, len(tm.registry))
	for _, task := range tm.registry {
		tasks = append(tasks, task)
	}

	return tasks
}

func (tm *TaskManager) waitWorkers(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	done := make(chan struct{})

	go func() {
		tm.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ewrap.Wrap(ctx.Err(), ErrMsgContextDone)
	}
}

func (tm *TaskManager) queueDepth() int {
	tm.queueMu.Lock()
	defer tm.queueMu.Unlock()

	return tm.scheduler.Len()
}

func (tm *TaskManager) waitForPermit(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	if tm.limiter == nil {
		return nil
	}

	err := tm.limiter.Wait(ctx)
	if err != nil {
		return ewrap.Wrap(err, ErrMsgContextDone)
	}

	return nil
}

func (tm *TaskManager) removeFromQueue(task *Task) bool {
	if task == nil {
		return false
	}

	tm.queueMu.Lock()
	defer tm.queueMu.Unlock()

	if task.index < 0 {
		return false
	}

	tm.scheduler.Remove(task.index)

	return true
}

func (tm *TaskManager) restoreQueued(task *Task, removed bool) {
	if !removed || task == nil {
		return
	}

	if task.ShouldSchedule() != nil {
		return
	}

	err := tm.enqueue(task)
	if err != nil {
		tm.finishTask(task, Failed, nil, err)

		return
	}

	task.setQueued()
}

func (tm *TaskManager) spawnWorker(ctx context.Context) {
	q := make(chan struct{})
	tm.workerQuit = append(tm.workerQuit, q)
	tm.workerWg.Add(1)

	go tm.workerLoop(ctx, q)
}

func (tm *TaskManager) validateRegisterArgs(ctx context.Context, task *Task) error {
	if task == nil {
		return ewrap.New("task is nil")
	}

	if tm.durableEnabled {
		return ewrap.New("durable backend enabled; use RegisterDurableTask")
	}

	if !tm.accepting.Load() {
		return ewrap.New("task manager is not accepting new tasks")
	}

	if ctx == nil {
		return ErrInvalidTaskContext
	}

	err := ctx.Err()
	if err != nil {
		return ewrap.Wrap(err, ErrMsgContextDone)
	}

	if task.Ctx != nil {
		err := task.Ctx.Err()
		if err != nil {
			return ewrap.Wrap(err, ErrMsgContextDone)
		}
	}

	return nil
}

func (tm *TaskManager) prepareTaskForRegister(ctx context.Context, task *Task) error {
	task.initRetryState(tm.retryDelay, tm.maxRetries)

	taskCtx, cancel := mergeContext(ctx, task.Ctx)
	task.Ctx = taskCtx
	task.CancelFunc = cancel

	if tm.ctx != nil {
		go func() {
			select {
			case <-tm.ctx.Done():
				cancel()
			case <-taskCtx.Done():
				return
			}
		}()
	}

	err := task.IsValid()
	if err != nil {
		task.markTerminal(Invalid, nil, err)

		return err
	}

	return nil
}

func (tm *TaskManager) storeTask(task *Task) error {
	tm.registryMu.Lock()
	defer tm.registryMu.Unlock()

	if _, exists := tm.registry[task.ID]; exists {
		return ewrap.Newf("task %s already exists", task.ID)
	}

	tm.registry[task.ID] = task

	return nil
}

func (tm *TaskManager) cancelAllInternal() {
	tasks := tm.GetTasks()
	for _, task := range tasks {
		err := tm.CancelTask(task.ID)
		if err != nil {
			// handle error if needed
			tm.metrics.failed.Add(1)

			continue
		}
	}
}

func (tm *TaskManager) enqueue(task *Task) error {
	tm.queueMu.Lock()
	defer tm.queueMu.Unlock()

	if tm.scheduler.Len() >= tm.maxTasks {
		return ewrap.Newf("queue is full (max %d)", tm.maxTasks)
	}

	tm.scheduler.Push(task)
	tm.queueCond.Signal()

	return nil
}

func (tm *TaskManager) schedulerLoop(ctx context.Context) {
	defer tm.workerWg.Done()

	for {
		task := tm.nextTask(ctx)
		if task == nil {
			return
		}

		if task.isCancelled() {
			tm.finishTask(task, Cancelled, nil, ErrTaskCancelled)

			continue
		}

		err := tm.waitForPermit(ctx)
		if err != nil {
			tm.finishTask(task, Cancelled, nil, err)

			continue
		}

		if task.isCancelled() {
			tm.finishTask(task, Cancelled, nil, ErrTaskCancelled)

			continue
		}

		select {
		case tm.jobs <- task:
		case <-ctx.Done():
			tm.finishTask(task, Cancelled, nil, ErrTaskCancelled)

			return
		}
	}
}

func (tm *TaskManager) nextTask(ctx context.Context) *Task {
	tm.queueMu.Lock()
	defer tm.queueMu.Unlock()

	for tm.scheduler.Len() == 0 && ctx.Err() == nil {
		tm.queueCond.Wait()
	}

	if ctx.Err() != nil {
		return nil
	}

	task, ok := tm.scheduler.Pop()
	if !ok {
		return nil
	}

	return task
}

func (tm *TaskManager) workerLoop(ctx context.Context, stop chan struct{}) {
	defer tm.workerWg.Done()

	for {
		select {
		case task := <-tm.jobs:
			if task == nil {
				continue
			}

			_, err := tm.runTask(ctx, task, 0)
			if err != nil {
				// handle error if needed
				tm.metrics.failed.Add(1)

				continue
			}
		case <-ctx.Done():
			return
		case <-stop:
			return
		}
	}
}

func (tm *TaskManager) runTask(ctx context.Context, task *Task, timeout time.Duration) (any, error) {
	if task == nil {
		return nil, ewrap.New("task is nil")
	}

	if task.durableLease != nil {
		return tm.runDurableTask(ctx, task, timeout)
	}

	if !task.markRunning() {
		return nil, ErrTaskAlreadyStarted
	}

	tm.hookStart(task)

	tm.metrics.running.Add(1)
	defer tm.metrics.running.Add(1 * -1)

	execCtx, cancel := tm.taskContext(ctx, task, timeout)
	defer cancel()

	var (
		result any
		err    error
	)

	execCtx, span := tm.startSpan(execCtx, task)

	defer func() {
		tm.endSpan(span, err)
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = ewrap.Newf("task panic: %v", r)
			}
		}()

		result, err = task.Execute(execCtx)
	}()

	if err == nil {
		tm.finishTask(task, Completed, result, nil)

		return result, nil
	}

	if errors.Is(execCtx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		tm.finishTask(task, Cancelled, result, ErrTaskCancelled)

		return result, err
	}

	if errors.Is(execCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		if tm.scheduleRetry(task) {
			return result, err
		}

		tm.finishTask(task, ContextDeadlineReached, result, err)

		return result, err
	}

	if tm.scheduleRetry(task) {
		return result, err
	}

	tm.finishTask(task, Failed, result, err)

	return result, err
}

func mergeContext(parent, other context.Context) (context.Context, context.CancelFunc) {
	base := parent
	baseCancel := func() {}

	if other != nil {
		if dl, ok := other.Deadline(); ok {
			if parentDl, ok := parent.Deadline(); !ok || dl.Before(parentDl) {
				base, baseCancel = context.WithDeadline(parent, dl)
			}
		}
	}

	merged, cancel := context.WithCancel(base)

	if other != nil {
		go func() {
			select {
			case <-other.Done():
				cancel()
			case <-merged.Done():
			}
		}()
	}

	return merged, func() {
		baseCancel()
		cancel()
	}
}

func (tm *TaskManager) taskContext(parent context.Context, task *Task, timeout time.Duration) (context.Context, context.CancelFunc) {
	base := task.Ctx
	if base == nil {
		base = parent
	}

	if base == nil {
		base = tm.ctx
	}

	if timeout > 0 {
		return context.WithTimeout(base, timeout)
	}

	if tm.timeout > 0 {
		if _, ok := base.Deadline(); !ok {
			return context.WithTimeout(base, tm.timeout)
		}
	}

	return base, func() {}
}

func (tm *TaskManager) scheduleRetry(task *Task) bool {
	if task.isCancelled() {
		return false
	}

	delay, attempt, ok := task.nextRetryDelay()
	if !ok {
		return false
	}

	task.setRateLimited()
	tm.metrics.retried.Add(1)
	tm.hookRetry(task, delay, attempt)

	tm.workerWg.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			if tm.ctx.Err() != nil || task.isCancelled() {
				return
			}

			err := tm.enqueue(task)
			if err != nil {
				tm.finishTask(task, Failed, nil, err)

				return
			}

			task.setQueued()
		case <-tm.ctx.Done():
			return
		case <-task.Ctx.Done():
			return
		}
	})

	return true
}

func (tm *TaskManager) incrementMetric(status TaskStatus) {
	//nolint:exhaustive
	switch status {
	case Completed:
		tm.metrics.completed.Add(1)
	case Cancelled:
		tm.metrics.cancelled.Add(1)
	case Failed, ContextDeadlineReached, Invalid:
		tm.metrics.failed.Add(1)
	default:
		return
	}
}

func (tm *TaskManager) finishTask(task *Task, status TaskStatus, result any, err error) {
	if task == nil {
		return
	}

	if !task.markTerminal(status, result, err) {
		return
	}

	task.doneOnce.Do(func() {
		if !task.skipWg {
			tm.wg.Done()
		}

		tm.incrementMetric(status)
		tm.recordLatency(task)
		tm.hookFinish(task, status, result, err)

		tm.results.Publish(Result{Task: task, Result: result, Error: err})
		tm.maybePruneRegistry()
	})
}

func (tm *TaskManager) recordLatency(task *Task) {
	if task == nil {
		return
	}

	latency, ok := task.latency()
	if !ok {
		return
	}

	latencyNs := latency.Nanoseconds()

	tm.metrics.latencyCount.Add(1)
	tm.metrics.latencyTotalNs.Add(latencyNs)

	for {
		current := tm.metrics.latencyMaxNs.Load()
		if latencyNs <= current {
			tm.recordOTelLatency(latency)

			return
		}

		if tm.metrics.latencyMaxNs.CompareAndSwap(current, latencyNs) {
			tm.recordOTelLatency(latency)

			return
		}
	}
}
