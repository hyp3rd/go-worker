package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
)

const errMsgDurableHandlerMissing = "durable handler not registered"

// RegisterDurableTask registers a durable task into the configured backend.
func (tm *TaskManager) RegisterDurableTask(ctx context.Context, task DurableTask) error {
	if !tm.durableEnabled || tm.durableBackend == nil {
		return ewrap.New("durable backend not configured")
	}

	prepared, err := tm.prepareDurableTask(ctx, task)
	if err != nil {
		return err
	}

	return tm.durableBackend.Enqueue(ctx, prepared)
}

// RegisterDurableTasks registers multiple durable tasks.
func (tm *TaskManager) RegisterDurableTasks(ctx context.Context, tasks ...DurableTask) error {
	var joined error

	for _, task := range tasks {
		err := tm.RegisterDurableTask(ctx, task)
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}

	return joined
}

func (tm *TaskManager) prepareDurableTask(ctx context.Context, task DurableTask) (DurableTask, error) {
	if ctx == nil {
		return DurableTask{}, ErrInvalidTaskContext
	}

	if task.ID == uuid.Nil {
		task.ID = uuid.New()
	}

	if task.Handler == "" {
		return DurableTask{}, ewrap.New("durable task handler is required")
	}

	spec, err := tm.durableHandlerSpec(task.Handler)
	if err != nil {
		return DurableTask{}, err
	}

	err = tm.ensureDurablePayload(&task)
	if err != nil {
		return DurableTask{}, err
	}

	tm.normalizeDurableConfig(&task)

	err = tm.ensureDurableMessage(&task, spec)
	if err != nil {
		return DurableTask{}, err
	}

	return task, nil
}

func (tm *TaskManager) normalizeDurableConfig(task *DurableTask) {
	if task.RetryDelay <= 0 {
		task.RetryDelay = tm.retryDelay
	}

	if task.Retries < 0 {
		task.Retries = 0
	}

	if task.Retries > tm.maxRetries {
		task.Retries = tm.maxRetries
	}

	if task.Priority == 0 {
		task.Priority = 1
	}

	if task.Queue == "" {
		task.Queue = tm.defaultQueue
	}

	if task.Weight <= 0 {
		task.Weight = DefaultTaskWeight
	}
}

func (tm *TaskManager) durableHandlerSpec(handler string) (DurableHandlerSpec, error) {
	spec, ok := tm.durableHandlers[handler]
	if !ok {
		return DurableHandlerSpec{}, ewrap.Wrapf(ewrap.New(errMsgDurableHandlerMissing), "handler %q", handler)
	}

	return spec, nil
}

func (tm *TaskManager) ensureDurablePayload(task *DurableTask) error {
	if task.Payload != nil {
		return nil
	}

	if task.Message == nil {
		return ewrap.New("durable task payload or message is required")
	}

	payload, err := tm.durableCodec.Marshal(task.Message)
	if err != nil {
		return ewrap.Wrap(err, "marshal durable payload")
	}

	task.Payload = payload

	return nil
}

func (tm *TaskManager) ensureDurableMessage(task *DurableTask, spec DurableHandlerSpec) error {
	if task.Message != nil || task.Payload == nil {
		return nil
	}

	msg := spec.Make()
	if msg == nil {
		return ewrap.New("durable handler payload maker is nil")
	}

	err := tm.durableCodec.Unmarshal(task.Payload, msg)
	if err != nil {
		return ewrap.Wrap(err, "durable payload validation")
	}

	task.Message = msg

	return nil
}

func (tm *TaskManager) durableLoop(ctx context.Context) {
	defer tm.workerWg.Done()

	poll := tm.durablePollInterval
	if poll <= 0 {
		poll = defaultDurablePollInterval
	}

	batch := tm.durableBatchSize
	if batch <= 0 {
		batch = 1
	}

	for {
		if ctx.Err() != nil {
			return
		}

		if !tm.dequeueDurableBatch(ctx, batch, poll) {
			return
		}
	}
}

func (tm *TaskManager) taskFromLease(ctx context.Context, lease DurableTaskLease) (*Task, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	task := lease.Task
	if task.ID == uuid.Nil {
		return nil, ewrap.New("durable task missing id")
	}

	spec, ok := tm.durableHandlers[task.Handler]
	if !ok {
		return nil, ewrap.Wrapf(ewrap.New(errMsgDurableHandlerMissing), "handler %q", task.Handler)
	}

	msg := spec.Make()
	if msg == nil {
		return nil, ewrap.New("durable handler payload maker is nil")
	}

	err := tm.durableCodec.Unmarshal(task.Payload, msg)
	if err != nil {
		return nil, ewrap.Wrap(err, "unmarshal durable payload")
	}

	if task.RetryDelay <= 0 {
		task.RetryDelay = tm.retryDelay
	}

	if task.Retries < 0 {
		task.Retries = 0
	}

	exec := func(ctx context.Context, _ ...any) (any, error) {
		return spec.Fn(ctx, msg)
	}

	tmTask, err := NewTask(ctx, exec)
	if err != nil {
		return nil, err
	}

	tmTask.ID = task.ID
	tmTask.Name = task.Handler
	tmTask.Priority = task.Priority
	tmTask.Retries = task.Retries
	tmTask.RetryDelay = task.RetryDelay

	tmTask.Queue = task.Queue
	if tmTask.Queue == "" {
		tmTask.Queue = tm.defaultQueue
	}

	tmTask.Weight = task.Weight
	if tmTask.Weight <= 0 {
		tmTask.Weight = DefaultTaskWeight
	}

	tmTask.durableLease = &lease

	tm.registryMu.Lock()
	tm.registry[tmTask.ID] = tmTask
	tm.registryMu.Unlock()

	return tmTask, nil
}

func (tm *TaskManager) dequeueDurableBatch(ctx context.Context, batch int, poll time.Duration) bool {
	leases, err := tm.durableBackend.Dequeue(ctx, batch, tm.durableLease)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}

		time.Sleep(poll)

		return true
	}

	if len(leases) == 0 {
		time.Sleep(poll)

		return true
	}

	for _, lease := range leases {
		if !tm.enqueueDurableLease(ctx, lease) {
			return false
		}
	}

	return true
}

func (tm *TaskManager) enqueueDurableLease(ctx context.Context, lease DurableTaskLease) bool {
	task, err := tm.taskFromLease(ctx, lease)
	if err != nil {
		failErr := tm.durableBackend.Fail(ctx, lease, err)
		tm.noteDurableBackendErr(ctx, failErr)

		return true
	}

	tm.wg.Add(1)

	task.skipWg = true

	select {
	case tm.jobs <- task:
		return true
	case <-ctx.Done():
		tm.wg.Done()

		return false
	}
}

func (*TaskManager) durableRetryDelay(base time.Duration, attempt int, id uuid.UUID) time.Duration {
	if base <= 0 {
		base = DefaultRetryDelay
	}

	if attempt <= 0 {
		attempt = 1
	}

	delay := base
	for i := 1; i < attempt; i++ {
		next := delay * 2
		if next > time.Minute {
			delay = time.Minute

			break
		}

		delay = next
	}

	return retryJitterDelay(delay, id, attempt)
}

func (*TaskManager) shouldRetryDurable(lease DurableTaskLease) bool {
	if lease.MaxRetries < 0 {
		return false
	}

	return lease.Attempts <= lease.MaxRetries
}

func (tm *TaskManager) durableLeaseRenewalConfig() (interval, leaseDuration time.Duration) {
	if tm.durableBackend == nil {
		return 0, 0
	}

	interval = tm.durableLeaseRenewal
	if interval <= 0 {
		return 0, 0
	}

	leaseDuration = tm.durableLease
	if leaseDuration <= 0 {
		leaseDuration = defaultDurableLease
	}

	if interval >= leaseDuration {
		interval = leaseDuration / 2
	}

	if interval <= 0 {
		return 0, 0
	}

	return interval, leaseDuration
}

func (tm *TaskManager) startDurableLeaseRenewal(
	ctx context.Context,
	execCtx context.Context,
	lease DurableTaskLease,
) func() {
	interval, leaseDuration := tm.durableLeaseRenewalConfig()
	if interval <= 0 {
		return func() {}
	}

	baseCtx := execCtx
	if baseCtx == nil {
		baseCtx = ctx
	}

	if baseCtx == nil {
		return func() {}
	}

	renewCtx := baseCtx

	renewCancel := func() {}
	if ctx != nil && ctx != baseCtx {
		renewCtx, renewCancel = mergeContext(baseCtx, ctx)
	}

	done := make(chan struct{})

	tm.workerWg.Go(func() {
		tm.runDurableLeaseRenewalLoop(renewCtx, lease, leaseDuration, interval, done)
	})

	return func() {
		close(done)
		renewCancel()
	}
}

func (tm *TaskManager) runDurableLeaseRenewalLoop(
	renewCtx context.Context,
	lease DurableTaskLease,
	leaseDuration time.Duration,
	interval time.Duration,
	done <-chan struct{},
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-renewCtx.Done():
			return
		case <-ticker.C:
			if tm.handleDurableLeaseRenewal(renewCtx, lease, leaseDuration) {
				return
			}
		}
	}
}

func (tm *TaskManager) handleDurableLeaseRenewal(
	ctx context.Context,
	lease DurableTaskLease,
	leaseDuration time.Duration,
) bool {
	err := tm.durableBackend.Extend(ctx, lease, leaseDuration)
	if err == nil {
		return false
	}

	if errors.Is(err, ErrDurableLeaseNotFound) {
		return true
	}

	tm.noteDurableBackendErr(ctx, err)

	return false
}

func (tm *TaskManager) runDurableTask(ctx context.Context, task *Task, timeout time.Duration) (any, error) {
	defer tm.wg.Done()

	if task == nil {
		return nil, ewrap.New("task is nil")
	}

	lease := task.durableLease
	if lease == nil {
		return nil, ewrap.New("durable lease missing")
	}

	if !task.markRunning() {
		err := tm.durableBackend.Fail(ctx, *lease, ErrTaskAlreadyStarted)
		if err != nil && ctx.Err() != nil {
			tm.metrics.failed.Add(1)
		}

		return nil, ErrTaskAlreadyStarted
	}

	tm.hookStart(task)

	tm.metrics.running.Add(1)
	defer tm.metrics.running.Add(1 * -1)

	execCtx, cancel := tm.taskContext(ctx, task, timeout)
	defer cancel()

	stopRenew := tm.startDurableLeaseRenewal(ctx, execCtx, *lease)
	defer stopRenew()

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

		tm.ackDurable(ctx, *lease)

		return result, nil
	}

	if errors.Is(execCtx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		tm.finishTask(task, Cancelled, result, ErrTaskCancelled)

		tm.failDurable(ctx, *lease, ErrTaskCancelled)

		return result, ErrTaskCancelled
	}

	if errors.Is(execCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return tm.handleDurableRetryOrFail(ctx, task, *lease, result, err, ContextDeadlineReached)
	}

	return tm.handleDurableRetryOrFail(ctx, task, *lease, result, err, Failed)
}

func (tm *TaskManager) handleDurableRetryOrFail(
	ctx context.Context,
	task *Task,
	lease DurableTaskLease,
	result any,
	err error,
	terminalStatus TaskStatus,
) (any, error) {
	if tm.shouldRetryDurable(lease) {
		delay := tm.durableRetryDelay(task.RetryDelay, lease.Attempts, task.ID)
		tm.metrics.retried.Add(1)
		tm.hookRetry(task, delay, lease.Attempts)

		nackErr := tm.durableBackend.Nack(ctx, lease, delay)
		tm.noteDurableBackendErr(ctx, nackErr)

		return result, nackErr
	}

	tm.finishTask(task, terminalStatus, result, err)
	tm.failDurable(ctx, lease, err)

	return result, err
}

func (tm *TaskManager) ackDurable(ctx context.Context, lease DurableTaskLease) {
	err := tm.durableBackend.Ack(ctx, lease)
	tm.noteDurableBackendErr(ctx, err)
}

func (tm *TaskManager) failDurable(ctx context.Context, lease DurableTaskLease, err error) {
	backendErr := tm.durableBackend.Fail(ctx, lease, err)
	tm.noteDurableBackendErr(ctx, backendErr)
}

func (tm *TaskManager) noteDurableBackendErr(ctx context.Context, err error) {
	if err != nil && ctx.Err() != nil {
		tm.metrics.failed.Add(1)
	}
}
