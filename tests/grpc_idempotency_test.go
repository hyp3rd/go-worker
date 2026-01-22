package tests

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
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
