package tests

import (
	"context"
	"testing"
	"time"

	worker "github.com/hyp3rd/go-worker"
)

func TestTaskManager_RegisterTaskAt(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManagerWithDefaults(context.Background())
	defer tm.StopNow()

	fired := make(chan struct{}, 1)

	task, err := worker.NewTask(context.Background(), func(_ context.Context, _ ...any) (any, error) {
		fired <- struct{}{}

		return "ok", nil
	})
	if err != nil {
		t.Fatalf("new task: %v", err)
	}

	runAt := time.Now().Add(100 * time.Millisecond)

	err = tm.RegisterTaskAt(context.Background(), task, runAt)
	if err != nil {
		t.Fatalf("register task: %v", err)
	}

	select {
	case <-fired:
		t.Fatal("task executed before scheduled time")
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case <-fired:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task did not execute after scheduled time")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestTaskManager_CancelScheduledTask(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManagerWithDefaults(context.Background())
	defer tm.StopNow()

	fired := make(chan struct{}, 1)

	task, err := worker.NewTask(context.Background(), func(_ context.Context, _ ...any) (any, error) {
		fired <- struct{}{}

		return "ok", nil
	})
	if err != nil {
		t.Fatalf("new task: %v", err)
	}

	err = tm.RegisterTaskAt(context.Background(), task, time.Now().Add(250*time.Millisecond))
	if err != nil {
		t.Fatalf("register task: %v", err)
	}

	err = tm.CancelTask(task.ID)
	if err != nil {
		t.Fatalf("cancel task: %v", err)
	}

	select {
	case <-fired:
		t.Fatal("cancelled task should not execute")
	case <-time.After(300 * time.Millisecond):
	}

	if task.Status() != worker.Cancelled {
		t.Fatalf("expected cancelled status, got %s", task.Status())
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
}
