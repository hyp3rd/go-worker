package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"

	"github.com/hyp3rd/go-worker"
)

const tracerWaitTimeout = time.Second

type testSpan struct {
	ch chan error
}

func (s testSpan) End(err error) {
	select {
	case s.ch <- err:
	default:
	}
}

type testTracer struct {
	ch chan error
}

func (t testTracer) Start(ctx context.Context, _ *worker.Task) (context.Context, worker.TaskSpan) {
	return ctx, t.span()
}

func (t testTracer) span() testSpan {
	return testSpan(t)
}

func TestTaskManager_TracerEnd(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), 1, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	errCh := make(chan error, 1)
	tm.SetTracer(testTracer{ch: errCh})

	taskErr := ewrap.New("trace error")
	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return nil, taskErr },
		Priority: 1,
		Ctx:      context.Background(),
	}

	err := tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), tracerWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	select {
	case got := <-errCh:
		if !errors.Is(got, taskErr) {
			t.Fatalf("expected tracer error %v, got %v", taskErr, got)
		}
	case <-time.After(tracerWaitTimeout):
		t.Fatal("timed out waiting for tracer end")
	}
}
