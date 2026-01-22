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

func TestTaskManager_Start(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	res := <-tm.StreamResults()
	if res.Task == nil {
		t.Fatal("Task result was not added to the results channel")
	}
}

func TestTaskManager_StreamResults(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (val any, err error) { return taskName, err },
		Priority: 10,
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	results := <-tm.StreamResults()
	if results.Task == nil {
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
