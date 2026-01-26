package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers               = 4
	maxTasks                 = 10
	tasksPerSecond           = 5
	tasksPerSecondHigh       = 1000
	maxRetries               = 3
	taskName                 = "task"
	errMsgFailedRegisterTask = "RegisterTask returned error: %v"
	retentionPollInterval    = 10 * time.Millisecond
	retentionTimeout         = time.Second
	retentionTTL             = 50 * time.Millisecond
	retentionCleanupInterval = 10 * time.Millisecond
	retentionLoadTasks       = 30
	retentionLoadMaxEntries  = 5
	retentionLoadMaxTasks    = 50
	metricsWaitTimeout       = time.Second
	metricsTaskSleep         = 10 * time.Millisecond
	resultsWaitTimeout       = time.Second
	cancelRetryDelay         = 50 * time.Millisecond
	cancelWaitTimeout        = time.Second
	fanOutWaitTimeout        = time.Second
)

func TestTaskManager_NewTaskManager(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	if tm == nil {
		t.Fatal("Task manager is nil")
	}
}

func TestTaskManager_RegisterTask(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return nil, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	tk, err := tm.GetTask(task.ID)
	if err != nil {
		t.Fatal("Task was not found in the registry")
	}

	if tk.Ctx == nil {
		t.Fatal("Task context is nil")
	}

	if tk.CancelFunc == nil {
		t.Fatal("Task cancel function is nil")
	}
}

func TestTaskManager_StreamResults(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	res := <-results
	if res.Task == nil {
		t.Fatal("results channel is nil")
	}
}

func TestTaskManager_GetTask(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	tk, err := tm.GetTask(task.ID)
	if err != nil {
		t.Fatal("Task was not found in the registry")
	}

	if tk.ID != task.ID {
		t.Fatal("Task ID does not match")
	}
}

func TestTaskManager_ExecuteTask(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	res, err := tm.ExecuteTask(context.TODO(), task.ID, time.Second*10)
	if err != nil {
		t.Fatal("Task execution failed")
	}

	if res == nil {
		t.Fatal("Task result is nil")
	}
}

func TestTaskManager_RetentionMaxEntries(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	tm.SetRetentionPolicy(worker.RetentionPolicy{MaxEntries: 1})

	taskA := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	taskB := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTasks(context.TODO(), taskA, taskB)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), retentionTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	deadline := time.Now().Add(retentionTimeout)
	for time.Now().Before(deadline) {
		if len(tm.GetTasks()) <= 1 {
			return
		}

		time.Sleep(retentionPollInterval)
	}

	t.Fatalf("expected registry retention to prune to 1 entry, got %d", len(tm.GetTasks()))
}

func TestTaskManager_RetentionTTL(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	tm.SetRetentionPolicy(worker.RetentionPolicy{
		TTL:             retentionTTL,
		CleanupInterval: retentionCleanupInterval,
	})

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return taskName, nil },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), retentionTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	deadline := time.Now().Add(retentionTimeout)
	for time.Now().Before(deadline) {
		if len(tm.GetTasks()) == 0 {
			return
		}

		time.Sleep(retentionPollInterval)
	}

	t.Fatalf("expected registry retention to prune by TTL, got %d entries", len(tm.GetTasks()))
}

func TestTaskManager_RetentionMaxEntriesUnderLoad(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(
		context.TODO(),
		maxWorkers,
		retentionLoadMaxTasks,
		tasksPerSecondHigh,
		time.Second*30,
		time.Second*30,
		maxRetries,
	)
	tm.SetRetentionPolicy(worker.RetentionPolicy{MaxEntries: retentionLoadMaxEntries})

	tasks := make([]*worker.Task, 0, retentionLoadTasks)
	for range retentionLoadTasks {
		tasks = append(tasks, &worker.Task{
			ID:       uuid.New(),
			Execute:  func(_ context.Context, _ ...any) (any, error) { return taskName, nil },
			Priority: 10,
			Ctx:      context.Background(),
		})
	}

	err := tm.RegisterTasks(context.TODO(), tasks...)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), retentionTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	deadline := time.Now().Add(retentionTimeout)
	for time.Now().Before(deadline) {
		remaining := tm.GetTasks()
		if len(remaining) <= retentionLoadMaxEntries {
			for _, task := range remaining {
				status := task.Status()
				//nolint:exhaustive
				switch status {
				case worker.Completed, worker.Failed, worker.Cancelled, worker.Invalid, worker.ContextDeadlineReached:
					continue
				default:
					t.Fatalf("expected terminal task status, got %v", status)
				}
			}

			return
		}

		time.Sleep(retentionPollInterval)
	}

	t.Fatalf("expected registry retention to prune to %d entries, got %d", retentionLoadMaxEntries, len(tm.GetTasks()))
}

func TestTaskManager_MetricsLatency(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	task := &worker.Task{
		ID: uuid.New(),
		Execute: func(_ context.Context, _ ...any) (any, error) {
			time.Sleep(metricsTaskSleep)

			return taskName, nil
		},
		Priority: 10,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), metricsWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	metrics := tm.GetMetrics()
	if metrics.TaskLatencyCount != 1 {
		t.Fatalf("expected latency count 1, got %d", metrics.TaskLatencyCount)
	}

	if metrics.TaskLatencyMax <= 0 {
		t.Fatalf("expected positive latency max, got %v", metrics.TaskLatencyMax)
	}

	if metrics.QueueDepth != 0 {
		t.Fatalf("expected queue depth 0, got %d", metrics.QueueDepth)
	}
}

func TestTaskManager_ResultDropOldest(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), 1, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	tm.SetResultsDropPolicy(worker.DropOldest)

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	taskOne := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return "first", nil },
		Priority: 1,
		Ctx:      context.Background(),
		Name:     "task-one",
	}

	taskTwo := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return "second", nil },
		Priority: 2,
		Ctx:      context.Background(),
		Name:     "task-two",
	}

	err := tm.RegisterTasks(context.TODO(), taskOne, taskTwo)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), resultsWaitTimeout)
	defer cancelWait()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	var lastID uuid.UUID

	deadline := time.After(resultsWaitTimeout)

	for {
		select {
		case res := <-results:
			if res.Task == nil {
				t.Fatal("expected result task")
			}

			lastID = res.Task.ID
			if lastID == taskTwo.ID {
				return
			}
		case <-deadline:
			if lastID == taskTwo.ID {
				return
			}

			t.Fatalf("expected latest task result, got %v", lastID)
		}
	}
}

func TestTaskManager_ResultFanOut(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), 1, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	resultsOne, cancelOne := tm.SubscribeResults(1)
	defer cancelOne()

	resultsTwo, cancelTwo := tm.SubscribeResults(1)
	defer cancelTwo()

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return "fanout", nil },
		Priority: 1,
		Ctx:      context.Background(),
		Name:     "fanout-task",
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), fanOutWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	readResult := func(ch <-chan worker.Result) worker.Result {
		select {
		case res := <-ch:
			return res
		case <-time.After(fanOutWaitTimeout):
			t.Fatal("timed out waiting for result")
		}

		return worker.Result{}
	}

	resOne := readResult(resultsOne)
	resTwo := readResult(resultsTwo)

	if resOne.Task == nil || resTwo.Task == nil {
		t.Fatal("expected results on both subscribers")
	}

	if resOne.Task.ID != task.ID {
		t.Fatalf("expected task ID %v on subscriber one, got %v", task.ID, resOne.Task.ID)
	}

	if resTwo.Task.ID != task.ID {
		t.Fatalf("expected task ID %v on subscriber two, got %v", task.ID, resTwo.Task.ID)
	}
}

func TestTaskManager_CancelBackoffTask(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), 1, maxTasks, tasksPerSecondHigh, time.Second*30, cancelRetryDelay, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return nil, ewrap.New("retry") },
		Priority: 1,
		Ctx:      context.Background(),
		Retries:  1,
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	deadline := time.Now().Add(cancelWaitTimeout)
	for time.Now().Before(deadline) {
		if task.Status() == worker.RateLimited {
			break
		}

		time.Sleep(retentionPollInterval)
	}

	err = tm.CancelTask(task.ID)
	if err != nil {
		t.Fatalf("CancelTask returned error: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), cancelWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}

	if task.Status() != worker.Cancelled {
		t.Fatalf("expected task status Cancelled, got %v", task.Status())
	}
}
