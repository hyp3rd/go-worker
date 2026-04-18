package worker

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

type duplicateDurableBackend struct {
	enqueues int
}

func (b *duplicateDurableBackend) Enqueue(context.Context, DurableTask) error {
	b.enqueues++

	return ErrDurableTaskAlreadyExists
}

func (*duplicateDurableBackend) Dequeue(context.Context, int, time.Duration) ([]DurableTaskLease, error) {
	return nil, nil
}

func (*duplicateDurableBackend) Ack(context.Context, DurableTaskLease) error {
	return nil
}

func (*duplicateDurableBackend) Nack(context.Context, DurableTaskLease, time.Duration) error {
	return nil
}

func (*duplicateDurableBackend) Fail(context.Context, DurableTaskLease, error) error {
	return nil
}

func (*duplicateDurableBackend) Extend(context.Context, DurableTaskLease, time.Duration) error {
	return nil
}

func TestRunDurableCronDuplicateIsBenign(t *testing.T) {
	t.Parallel()

	backend := &duplicateDurableBackend{}
	tm := NewTaskManagerWithOptions(
		context.Background(),
		WithDurableBackend(backend),
		WithDurableHandlers(map[string]DurableHandlerSpec{
			"cron_handler": {
				Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
				Fn: func(context.Context, proto.Message) (any, error) {
					return struct{}{}, nil
				},
			},
		}),
		WithCronLocation(time.UTC),
	)

	factory := cronFactory{
		Durable: true,
		DurableFactory: func(context.Context) (DurableTask, error) {
			return DurableTask{
				ID:      uuid.New(),
				Handler: "cron_handler",
				Message: &workerpb.SendEmailPayload{},
			}, nil
		},
	}

	tm.runDurableCron(context.Background(), "tick", cronSpec{Spec: "*/1 * * * * *", Durable: true}, factory)

	if backend.enqueues != 1 {
		t.Fatalf("expected one enqueue attempt, got %d", backend.enqueues)
	}

	tm.cronEventsMu.RLock()
	defer tm.cronEventsMu.RUnlock()

	if len(tm.cronRuns) != 0 {
		t.Fatalf("expected duplicate durable cron to clear pending run tracking, got %d entries", len(tm.cronRuns))
	}
}

func TestAdminRunScheduleDuplicateReturnsTaskID(t *testing.T) {
	t.Parallel()

	backend := &duplicateDurableBackend{}
	tm := NewTaskManagerWithOptions(
		context.Background(),
		WithDurableBackend(backend),
		WithDurableHandlers(map[string]DurableHandlerSpec{
			"cron_handler": {
				Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
				Fn: func(context.Context, proto.Message) (any, error) {
					return struct{}{}, nil
				},
			},
		}),
		WithCronLocation(time.UTC),
	)

	expectedID := uuid.New()

	err := tm.RegisterDurableCronTask(context.Background(), "tick", "*/1 * * * * *", func(context.Context) (DurableTask, error) {
		return DurableTask{
			ID:      expectedID,
			Handler: "cron_handler",
			Message: &workerpb.SendEmailPayload{},
		}, nil
	})
	if err != nil {
		t.Fatalf("register durable cron: %v", err)
	}

	taskID, err := tm.AdminRunSchedule(context.Background(), "tick")
	if err != nil {
		t.Fatalf("admin run schedule: %v", err)
	}

	if taskID != expectedID.String() {
		t.Fatalf("expected task id %s, got %s", expectedID, taskID)
	}
}
