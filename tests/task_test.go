package tests

import (
	"context"
	"testing"

	"github.com/google/uuid"

	worker "github.com/hyp3rd/go-worker"
)

func TestTask_IsValid(t *testing.T) {
	// valid task
	task := &worker.Task{
		ID:      uuid.New(),
		Ctx:     context.Background(),
		Execute: func(ctx context.Context, args ...any) (any, error) { return nil, nil },
	}
	if err := task.IsValid(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// invalid id
	task = &worker.Task{
		ID:      uuid.Nil,
		Ctx:     context.Background(),
		Execute: func(ctx context.Context, args ...any) (any, error) { return nil, nil },
	}
	if err := task.IsValid(); err != worker.ErrInvalidTaskID {
		t.Fatalf("expected ErrInvalidTaskID, got %v", err)
	}

	// nil context
	task = &worker.Task{
		ID:      uuid.New(),
		Ctx:     nil,
		Execute: func(ctx context.Context, args ...any) (any, error) { return nil, nil },
	}
	if err := task.IsValid(); err != worker.ErrInvalidTaskContext {
		t.Fatalf("expected ErrInvalidTaskContext, got %v", err)
	}

	// nil execute func
	task = &worker.Task{
		ID:      uuid.New(),
		Ctx:     context.Background(),
		Execute: nil,
	}
	if err := task.IsValid(); err != worker.ErrInvalidTaskFunc {
		t.Fatalf("expected ErrInvalidTaskFunc, got %v", err)
	}
}
