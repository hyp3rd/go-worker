package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers     = 2
	maxTasks       = 10
	tasksPerSecond = 5
	taskTimeout    = 5 * time.Second
)

func main() {
	ctx := context.Background()

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4318"
	}

	exporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(endpoint),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		log.Fatalf("otlp metric exporter: %v", err)
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(2*time.Second))
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
			time.Sleep(100 * time.Millisecond)
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
}
