package worker

import (
	"context"
	"log/slog"
)

// TaskSpan represents an in-flight task span.
type TaskSpan interface {
	End(err error)
}

// TaskTracer provides tracing spans around task execution.
type TaskTracer interface {
	Start(ctx context.Context, task *Task) (context.Context, TaskSpan)
}

type tracerHolder struct {
	tracer TaskTracer
}

// SetTracer configures the task tracer.
func (tm *TaskManager) SetTracer(tracer TaskTracer) {
	if tracer == nil {
		tm.tracer.Store(nil)

		return
	}

	tm.tracer.Store(&tracerHolder{tracer: tracer})
}

func (tm *TaskManager) startSpan(ctx context.Context, task *Task) (context.Context, TaskSpan) {
	holder := tm.tracer.Load()
	if holder == nil || holder.tracer == nil {
		return ctx, nil
	}

	defer func() {
		if recover() != nil {
			// ignore tracer panics
			logger := slog.Default()
			logger.Log(tm.ctx, slog.LevelWarn, "tracer panic recovered")
		}
	}()

	newCtx, span := holder.tracer.Start(ctx, task)
	if newCtx == nil {
		newCtx = ctx
	}

	return newCtx, span
}

func (tm *TaskManager) endSpan(span TaskSpan, err error) {
	if span == nil {
		return
	}

	defer func() {
		if recover() != nil {
			// ignore tracer panics
			logger := slog.Default()
			logger.Log(tm.ctx, slog.LevelWarn, "tracer panic recovered")
		}
	}()

	span.End(err)
}
