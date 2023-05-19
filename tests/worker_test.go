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
	task := worker.Task{
		ID:       uuid.New(),
		Execute:  func() (val interface{}, err error) { return nil, err },
		Priority: 10,
	}

	tm.RegisterTask(context.TODO(), task)

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
	task := worker.Task{
		ID:       uuid.New(),
		Execute:  func() (val interface{}, err error) { return "task", err },
		Priority: 10,
	}
	tm.RegisterTask(context.TODO(), task)

	for {
		select {
		case result, ok := <-tm.StreamResults():
			if !ok {
				return
			}
			t.Log(result.Task.ID.String(), result.Result)
			if result.Task == nil {
				t.Fatalf("Task result was not added to the results channel")
			}
		case cancelledTasks, ok := <-tm.GetCancelledTasks():
			if !ok {
				return
			}
			t.Log(cancelledTasks)
		default:
			return
		}
	}

}

func TestTaskManager_StreamResults(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := worker.Task{
		ID:       uuid.New(),
		Execute:  func() (val interface{}, err error) { return "task", err },
		Priority: 10,
	}
	tm.RegisterTask(context.TODO(), task)

	for {
		select {
		case result, ok := <-tm.StreamResults():
			if !ok {
				return
			}
			t.Log(result.Task.ID.String(), result.Result)
			if result.Task == nil {
				t.Fatalf("Task result was not added to the results channel")
			}
		case cancelledTasks, ok := <-tm.GetCancelledTasks():
			if !ok {
				return
			}
			t.Log(cancelledTasks)
		default:
			return
		}
	}
}

func TestTaskManager_GetTask(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	task := worker.Task{
		ID:       uuid.New(),
		Execute:  func() (val interface{}, err error) { return "task", err },
		Priority: 10,
	}
	tm.RegisterTask(context.TODO(), task)
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
	task := worker.Task{
		ID:       uuid.New(),
		Execute:  func() (val interface{}, err error) { return "task", err },
		Priority: 10,
	}
	tm.RegisterTask(context.TODO(), task)

	res, err := tm.ExecuteTask(task.ID, time.Second*10)
	if err != nil {
		t.Fatalf("Task execution failed")
	}
	if res == nil {
		t.Fatalf("Task result is nil")
	}
}
