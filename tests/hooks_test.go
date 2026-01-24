package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"

	"github.com/hyp3rd/go-worker"
)

const (
	hookWaitTimeout    = time.Second
	hookRetryDelay     = 10 * time.Millisecond
	hookRetryAttempts  = 1
	hookTasksPerSecond = 1000
)

func TestTaskManager_Hooks(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	var (
		queued   atomic.Int64
		started  atomic.Int64
		finished atomic.Int64
	)

	tm.SetHooks(worker.TaskHooks{
		OnQueued: func(_ *worker.Task) { queued.Add(1) },
		OnStart:  func(_ *worker.Task) { started.Add(1) },
		OnFinish: func(_ *worker.Task, _ worker.TaskStatus, _ any, _ error) { finished.Add(1) },
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

	waitCtx, cancel := context.WithTimeout(context.Background(), hookWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}

	if queued.Load() != 1 {
		t.Fatalf("expected queued hook 1, got %d", queued.Load())
	}

	if started.Load() != 1 {
		t.Fatalf("expected start hook 1, got %d", started.Load())
	}

	if finished.Load() != 1 {
		t.Fatalf("expected finish hook 1, got %d", finished.Load())
	}
}

func TestTaskManager_HookRetry(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, hookTasksPerSecond, time.Second*30, hookRetryDelay, maxRetries)

	var (
		retries  atomic.Int64
		attempts atomic.Int64
	)

	tm.SetHooks(worker.TaskHooks{
		OnRetry: func(_ *worker.Task, _ time.Duration, _ int) { retries.Add(1) },
	})

	task := &worker.Task{
		ID: uuid.New(),
		Execute: func(_ context.Context, _ ...any) (any, error) {
			if attempts.Add(1) == hookRetryAttempts {
				return nil, ewrap.New("retry")
			}

			return taskName, nil
		},
		Priority: 10,
		Ctx:      context.Background(),
		Retries:  hookRetryAttempts,
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), hookWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}

	if retries.Load() != 1 {
		t.Fatalf("expected retry hook 1, got %d", retries.Load())
	}
}
