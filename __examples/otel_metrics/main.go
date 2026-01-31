package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers       = 2
	maxTasks         = 10
	tasksPerSecond   = 5
	taskTimeout      = 5 * time.Second
	metricsFlushWait = 200 * time.Millisecond
)

func main() {
	ctx := context.Background()

	exporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		log.Fatalf("stdout exporter: %v", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(100*time.Millisecond))
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		_ = meterProvider.Shutdown(ctx)
	}()

	tm := worker.NewTaskManager(ctx, maxWorkers, maxTasks, tasksPerSecond, taskTimeout, time.Second, 1)
	if err := tm.SetMeterProvider(meterProvider); err != nil {
		log.Fatalf("set meter provider: %v", err)
	}

	task := &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Ctx:      context.Background(),
		Execute: func(_ context.Context, _ ...any) (any, error) {
			time.Sleep(50 * time.Millisecond)
			return "ok", nil
		},
	}

	if err := tm.RegisterTask(ctx, task); err != nil {
		log.Fatalf("register task: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	err := tm.Wait(waitCtx)
	if err != nil {
		log.Fatalf("wait: %v", err)
	}

	err := meterProvider.ForceFlush(ctx)
	if err != nil {
		log.Printf("force flush: %v", err)
	}

	time.Sleep(metricsFlushWait)
}
