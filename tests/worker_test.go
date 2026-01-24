package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers               = 4
	maxTasks                 = 10
	tasksPerSecond           = 5
	maxRetries               = 3
	taskName                 = "task"
	errMsgFailedRegisterTask = "RegisterTask returned error: %v"
	retentionPollInterval    = 10 * time.Millisecond
	retentionTimeout         = time.Second
	retentionTTL             = 50 * time.Millisecond
	retentionCleanupInterval = 10 * time.Millisecond
	metricsWaitTimeout       = time.Second
	metricsTaskSleep         = 10 * time.Millisecond
	resultsWaitTimeout       = time.Second
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

	first := <-results
	if first.Task == nil {
		t.Fatal("expected result task")
	}

	select {
	case res := <-results:
		if res.Task == nil {
			t.Fatal("expected result task")
		}

		if res.Task.ID != taskTwo.ID {
			t.Fatalf("expected latest task result, got %v", res.Task.ID)
		}

		return
	default:
	}

	res := first
	if res.Task == nil {
		t.Fatal("expected result task")
	}

	if res.Task.ID == taskTwo.ID {
		return
	}

	t.Fatalf("expected latest task result, got %v", res.Task.ID)
}
