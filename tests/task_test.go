package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	worker "github.com/hyp3rd/go-worker"
)

const testOutput = "out"

func TestTask_IsValid(t *testing.T) {
	t.Parallel()
	// valid task
	task := &worker.Task{
		ID:      uuid.New(),
		Ctx:     context.Background(),
		Execute: func(_ context.Context, _ ...any) (any, error) { return testOutput, nil },
	}

	err := task.IsValid()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// invalid id
	task = &worker.Task{
		ID:      uuid.Nil,
		Ctx:     context.Background(),
		Execute: func(_ context.Context, _ ...any) (any, error) { return testOutput, nil },
	}

	err = task.IsValid()
	if !errors.Is(err, worker.ErrInvalidTaskID) {
		t.Fatalf("expected ErrInvalidTaskID, got %v", err)
	}

	// nil context
	task = &worker.Task{
		ID:      uuid.New(),
		Ctx:     nil,
		Execute: func(_ context.Context, _ ...any) (any, error) { return testOutput, nil },
	}

	err = task.IsValid()
	if !errors.Is(err, worker.ErrInvalidTaskContext) {
		t.Fatalf("expected ErrInvalidTaskContext, got %v", err)
	}

	// nil execute func
	task = &worker.Task{
		ID:      uuid.New(),
		Ctx:     context.Background(),
		Execute: nil,
	}

	err = task.IsValid()
	if !errors.Is(err, worker.ErrInvalidTaskFunc) {
		t.Fatalf("expected ErrInvalidTaskFunc, got %v", err)
	}
}
