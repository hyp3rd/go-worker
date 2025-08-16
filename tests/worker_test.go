package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hyp3rd/go-worker"
)

func TestTaskManager_NewTaskManager(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	if tm == nil {
		t.Fatalf("Task manager is nil")
	}
}

func TestTaskManager_RegisterTask(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return nil, err },
		Priority: 10,
	}

	if err := tm.RegisterTask(context.TODO(), task); err != nil {
		t.Fatalf("RegisterTask returned error: %v", err)
	}

	tk, err := tm.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Task was not found in the registry")
	}
	if tk.Ctx == nil {
		t.Fatalf("Task context is nil")
	}
	if tk.CancelFunc == nil {
		t.Fatalf("Task cancel function is nil")
	}
}

func TestTaskManager_Start(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "task", err },
		Priority: 10,
	}
	if err := tm.RegisterTask(context.TODO(), task); err != nil {
		t.Fatalf("RegisterTask returned error: %v", err)
	}

	res := <-tm.StreamResults()
	if res.Task == nil {
		t.Fatalf("Task result was not added to the results channel")
	}
}

func TestTaskManager_StreamResults(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "task", err },
		Priority: 10,
	}
	if err := tm.RegisterTask(context.TODO(), task); err != nil {
		t.Fatalf("RegisterTask returned error: %v", err)
	}

	results := <-tm.StreamResults()
	if results.Task == nil {
		t.Fatalf("results channel is nil")
	}
}

func TestTaskManager_GetTask(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "task", err },
		Priority: 10,
	}
	if err := tm.RegisterTask(context.TODO(), task); err != nil {
		t.Fatalf("RegisterTask returned error: %v", err)
	}
	tk, err := tm.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Task was not found in the registry")
	}

	if tk.ID != task.ID {
		t.Fatalf("Task ID does not match")
	}
}

func TestTaskManager_ExecuteTask(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "task", err },
		Priority: 10,
	}
	if err := tm.RegisterTask(context.TODO(), task); err != nil {
		t.Fatalf("RegisterTask returned error: %v", err)
	}

	res, err := tm.ExecuteTask(context.TODO(), task.ID, time.Second*10)
	if err != nil {
		t.Fatalf("Task execution failed")
	}
	if res == nil {
		t.Fatalf("Task result is nil")
	}
}
