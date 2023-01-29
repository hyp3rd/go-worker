package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/hyp3rd/go-worker"
)

func TestTaskManager_NewTaskManager(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	if tm == nil {
		t.Fatalf("Task manager is nil")
	}
}

func TestTaskManager_RegisterTask(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	task := worker.Task{
		ID:       uuid.New(),
		Fn:       func() interface{} { return nil },
		Priority: 10,
	}

	tm.RegisterTask(task)

	tk, ok := tm.GetTask(task.ID)
	if !ok {
		t.Fatalf("Task was not found in the registry")
	}
	if tk.Ctx == nil {
		t.Fatalf("Task context is nil")
	}
	if tk.Cancel == nil {
		t.Fatalf("Task cancel function is nil")
	}
}

func TestTaskManager_Start(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	task := worker.Task{
		ID:       uuid.New(),
		Fn:       func() interface{} { return "task" },
		Priority: 10,
	}
	tm.RegisterTask(task)
	tm.Start(2)
	res := <-tm.GetResults()
	if res == nil {
		t.Fatalf("Task result was not added to the results channel")
	}
}

func TestTaskManager_GetResults(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	task := worker.Task{
		ID:       uuid.New(),
		Fn:       func() interface{} { return "task" },
		Priority: 10,
	}
	tm.RegisterTask(task)
	tm.Start(2)
	results := <-tm.GetResults()
	if results == nil {
		t.Fatalf("results channel is nil")
	}
}

func TestTaskManager_GetTask(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	task := worker.Task{
		ID:       uuid.New(),
		Fn:       func() interface{} { return "task" },
		Priority: 10,
	}
	tm.RegisterTask(task)
	tk, ok := tm.GetTask(task.ID)
	if !ok {
		t.Fatalf("Task was not found in the registry")
	}

	if tk.ID != task.ID {
		t.Fatalf("Task ID does not match")
	}
}

func TestTaskManager_ExecuteTask(t *testing.T) {
	tm := worker.NewTaskManager(10, 10)
	task := worker.Task{
		ID:       uuid.New(),
		Fn:       func() interface{} { return "task" },
		Priority: 10,
	}
	tm.RegisterTask(task)
	tm.Start(2)
	res, err := tm.ExecuteTask(task.ID)
	if err != nil {
		t.Fatalf("Task execution failed")
	}
	if res == nil {
		t.Fatalf("Task result is nil")
	}
}
