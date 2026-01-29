package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func TestGRPCServer_IdempotencyKeyReuse(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{
		"echo": {
			Make: func() protoreflect.ProtoMessage { return &wrapperspb.StringValue{} },
			Fn: func(_ context.Context, _ protoreflect.ProtoMessage) (any, error) {
				return "ok", nil
			},
		},
	})

	req := &workerpb.RegisterTasksRequest{
		Tasks: []*workerpb.Task{
			{
				Name:           "echo",
				IdempotencyKey: "idempotent-key",
				CorrelationId:  "corr-1",
				Metadata:       map[string]string{"source": "test"},
				Description:    "test-task",
				Priority:       1,
			},
		},
	}

	respOne, err := server.RegisterTasks(context.TODO(), req)
	if err != nil {
		t.Fatalf("RegisterTasks returned error: %v", err)
	}

	respTwo, err := server.RegisterTasks(context.TODO(), req)
	if err != nil {
		t.Fatalf("RegisterTasks returned error: %v", err)
	}

	if respOne.GetIds()[0] != respTwo.GetIds()[0] {
		t.Fatalf("expected same id for idempotent request, got %v and %v", respOne.GetIds()[0], respTwo.GetIds()[0])
	}

	if len(tm.GetTasks()) != 1 {
		t.Fatalf("expected one task in registry, got %d", len(tm.GetTasks()))
	}
}

func TestGRPCServer_IdempotencyKeyConflict(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{
		"echo": {
			Make: func() protoreflect.ProtoMessage { return &wrapperspb.StringValue{} },
			Fn: func(_ context.Context, _ protoreflect.ProtoMessage) (any, error) {
				return "ok", nil
			},
		},
	})

	req := &workerpb.RegisterTasksRequest{
		Tasks: []*workerpb.Task{
			{
				Name:           "echo",
				IdempotencyKey: "conflict-key",
			},
		},
	}

	_, err := server.RegisterTasks(context.TODO(), req)
	if err != nil {
		t.Fatalf("RegisterTasks returned error: %v", err)
	}

	conflictReq := &workerpb.RegisterTasksRequest{
		Tasks: []*workerpb.Task{
			{
				Name:           "different",
				IdempotencyKey: "conflict-key",
			},
		},
	}

	_, err = server.RegisterTasks(context.TODO(), conflictReq)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}

	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", status.Code(err))
	}
}

func TestGRPCServer_DurableIdempotencyKeyReuse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend := &idempotencyDurableBackend{}

	tm := worker.NewTaskManagerWithOptions(
		ctx,
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(map[string]worker.DurableHandlerSpec{
			"send_email": {
				Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
				Fn: func(context.Context, proto.Message) (any, error) {
					return "ok", nil
				},
			},
		}),
	)

	t.Cleanup(func() { stopTaskManager(t, tm) })

	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{
		"send_email": {
			Make: func() protoreflect.ProtoMessage { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, protoreflect.ProtoMessage) (any, error) {
				return "ok", nil
			},
		},
	})

	payload, err := anypb.New(&workerpb.SendEmailPayload{
		To:      "ops@example.com",
		Subject: "hello",
		Body:    "body",
	})
	if err != nil {
		t.Fatalf("anypb: %v", err)
	}

	req := &workerpb.RegisterDurableTasksRequest{
		Tasks: []*workerpb.DurableTask{
			{
				Name:           "send_email",
				Payload:        payload,
				IdempotencyKey: "durable:send_email:ops@example.com",
			},
		},
	}

	respOne, err := server.RegisterDurableTasks(ctx, req)
	if err != nil {
		t.Fatalf("RegisterDurableTasks returned error: %v", err)
	}

	respTwo, err := server.RegisterDurableTasks(ctx, req)
	if err != nil {
		t.Fatalf("RegisterDurableTasks returned error: %v", err)
	}

	if respOne.GetIds()[0] != respTwo.GetIds()[0] {
		t.Fatalf("expected same id for idempotent request, got %v and %v", respOne.GetIds()[0], respTwo.GetIds()[0])
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected one durable task enqueued, got %d", len(backend.enqueued))
	}
}

type idempotencyDurableBackend struct {
	mu       sync.Mutex
	enqueued []worker.DurableTask
}

func (b *idempotencyDurableBackend) Enqueue(_ context.Context, task worker.DurableTask) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.enqueued = append(b.enqueued, task)

	return nil
}

func (*idempotencyDurableBackend) Dequeue(context.Context, int, time.Duration) ([]worker.DurableTaskLease, error) {
	return nil, nil
}

func (*idempotencyDurableBackend) Ack(context.Context, worker.DurableTaskLease) error {
	return nil
}

func (*idempotencyDurableBackend) Nack(context.Context, worker.DurableTaskLease, time.Duration) error {
	return nil
}

func (*idempotencyDurableBackend) Fail(context.Context, worker.DurableTaskLease, error) error {
	return nil
}
