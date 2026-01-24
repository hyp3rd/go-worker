package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func TestGRPCServer_CancelTaskNotFound(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{})

	id := uuid.New().String()

	_, err := server.CancelTask(context.TODO(), &workerpb.CancelTaskRequest{Id: id})
	if err == nil {
		t.Fatal("expected error for unknown task")
	}

	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", status.Code(err))
	}
}

func TestGRPCServer_GetTaskNotFound(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)
	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{})

	id := uuid.New().String()

	_, err := server.GetTask(context.TODO(), &workerpb.GetTaskRequest{Id: id})
	if err == nil {
		t.Fatal("expected error for unknown task")
	}

	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", status.Code(err))
	}
}

func TestGRPCServer_AuthDenied(t *testing.T) {
	t.Parallel()

	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, time.Second*30, time.Second*30, maxRetries)

	var (
		gotMethod string
		gotReq    bool
	)

	auth := func(_ context.Context, method string, req any) error {
		gotMethod = method
		_, gotReq = req.(*workerpb.GetTaskRequest)

		return status.Error(codes.Unauthenticated, "missing token")
	}

	server := worker.NewGRPCServer(tm, map[string]worker.HandlerSpec{}, worker.WithGRPCAuth(auth))

	_, err := server.GetTask(context.TODO(), &workerpb.GetTaskRequest{Id: uuid.New().String()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", status.Code(err))
	}

	if gotMethod != workerpb.WorkerService_GetTask_FullMethodName {
		t.Fatalf("expected method %s, got %s", workerpb.WorkerService_GetTask_FullMethodName, gotMethod)
	}

	if !gotReq {
		t.Fatal("expected auth hook to receive request")
	}
}
