package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers     = 2
	maxTasks       = 10
	tasksPerSecond = 5
	taskTimeout    = 5 * time.Second
)

type otelTracer struct {
	tracer trace.Tracer
}

type otelSpan struct {
	span trace.Span
}

func (t otelTracer) Start(ctx context.Context, task *worker.Task) (context.Context, worker.TaskSpan) {
	attrs := []attribute.KeyValue{
		attribute.String("task.id", task.ID.String()),
		attribute.String("task.name", task.Name),
		attribute.Int("task.priority", task.Priority),
	}

	ctx, span := t.tracer.Start(ctx, "worker.task", trace.WithAttributes(attrs...))

	return ctx, otelSpan{span: span}
}

func (s otelSpan) End(err error) {
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	} else {
		s.span.SetStatus(codes.Ok, "")
	}

	s.span.End()
}

func main() {
	ctx := context.Background()

	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatalf("stdout trace exporter: %v", err)
	}

	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	defer func() {
		_ = traceProvider.Shutdown(ctx)
	}()

	otel.SetTracerProvider(traceProvider)
	tracer := otel.Tracer("github.com/hyp3rd/go-worker/example")

	tm := worker.NewTaskManager(ctx, maxWorkers, maxTasks, tasksPerSecond, taskTimeout, time.Second, 1)
	tm.SetTracer(otelTracer{tracer: tracer})

	task := &worker.Task{
		ID:       uuid.New(),
		Name:     "otel-tracing",
		Priority: 1,
		Ctx:      context.Background(),
		Execute: func(_ context.Context, _ ...any) (any, error) {
			time.Sleep(150 * time.Millisecond)
			return "ok", nil
		},
	}

	if err := tm.RegisterTask(ctx, task); err != nil {
		log.Fatalf("register task: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	if err := tm.Wait(waitCtx); err != nil {
		log.Fatalf("wait: %v", err)
	}
}
