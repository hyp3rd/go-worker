package tests

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func TestCronTaskRuns(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithCronLocation(time.UTC),
	)

	done := make(chan struct{})

	err := tm.RegisterCronTask(context.Background(), "tick", "*/1 * * * * *", func(ctx context.Context) (*worker.Task, error) {
		return worker.NewTask(ctx, func(context.Context, ...any) (any, error) {
			select {
			case <-done:
			default:
				close(done)
			}

			return "ok", nil
		})
	})
	if err != nil {
		t.Fatalf("register cron: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cron task")
	}

	tm.StopNow()
}

func TestDurableCronTaskRuns(t *testing.T) {
	t.Parallel()

	backend := &cronDurableBackend{enqueueCh: make(chan worker.DurableTask, 1)}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(map[string]worker.DurableHandlerSpec{
			"cron_handler": {
				Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
				Fn: func(context.Context, proto.Message) (any, error) {
					return struct{}{}, nil
				},
			},
		}),
		worker.WithCronLocation(time.UTC),
	)

	err := tm.RegisterDurableCronTask(context.Background(), "tick", "*/1 * * * * *", func(context.Context) (worker.DurableTask, error) {
		return worker.DurableTask{
			Handler: "cron_handler",
			Message: &workerpb.SendEmailPayload{},
		}, nil
	})
	if err != nil {
		t.Fatalf("register durable cron: %v", err)
	}

	select {
	case task := <-backend.enqueueCh:
		if task.Handler != "cron_handler" {
			t.Fatalf("expected handler cron_handler, got %s", task.Handler)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for durable cron")
	}

	tm.StopNow()
}

type cronDurableBackend struct {
	enqueueCh chan worker.DurableTask
}

func (b *cronDurableBackend) Enqueue(_ context.Context, task worker.DurableTask) error {
	b.enqueueCh <- task

	return nil
}

func (*cronDurableBackend) Dequeue(context.Context, int, time.Duration) ([]worker.DurableTaskLease, error) {
	return []worker.DurableTaskLease{}, nil
}

func (*cronDurableBackend) Ack(context.Context, worker.DurableTaskLease) error {
	return nil
}

func (*cronDurableBackend) Nack(context.Context, worker.DurableTaskLease, time.Duration) error {
	return nil
}

func (*cronDurableBackend) Fail(context.Context, worker.DurableTaskLease, error) error {
	return nil
}

func (*cronDurableBackend) Extend(context.Context, worker.DurableTaskLease, time.Duration) error {
	return nil
}
