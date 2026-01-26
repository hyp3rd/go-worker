package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hyp3rd/go-worker"
)

func TestTaskManager_OTelMetrics(t *testing.T) {
	t.Parallel()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	tm := worker.NewTaskManager(context.TODO(), 1, maxTasks, tasksPerSecondHigh, time.Second*30, time.Second*30, maxRetries)

	err := tm.SetMeterProvider(provider)
	if err != nil {
		t.Fatalf("SetMeterProvider failed: %v", err)
	}

	task := &worker.Task{
		ID:       uuid.New(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return taskName, nil },
		Priority: 10,
		Ctx:      context.Background(),
	}

	err = tm.RegisterTask(context.TODO(), task)
	if err != nil {
		t.Fatalf(errMsgFailedRegisterTask, err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), metricsWaitTimeout)
	defer cancel()

	err = tm.Wait(waitCtx)
	if err != nil {
		t.Fatalf(ErrMsgWaitReturnedError, err)
	}

	var rm metricdata.ResourceMetrics

	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("Collect metrics failed: %v", err)
	}

	assertMetricSumAtLeast(t, &rm, "tasks_scheduled_total", 1)
	assertMetricSumAtLeast(t, &rm, "tasks_completed_total", 1)
}

func assertMetricSumAtLeast(t *testing.T, rm *metricdata.ResourceMetrics, name string, minVal int64) {
	t.Helper()

	value, ok := metricSumValue(rm, name)
	if !ok {
		t.Fatalf("expected metric %s to be exported", name)
	}

	if value < minVal {
		t.Fatalf("expected metric %s to be at least %d, got %d", name, minVal, value)
	}
}

func metricSumValue(rm *metricdata.ResourceMetrics, name string) (int64, bool) {
	for _, scope := range rm.ScopeMetrics {
		for _, metricInScope := range scope.Metrics {
			if metricInScope.Name != name {
				continue
			}

			switch data := metricInScope.Data.(type) {
			case metricdata.Sum[int64]:
				var total int64
				for _, point := range data.DataPoints {
					total += point.Value
				}

				return total, true
			case metricdata.Gauge[int64]:
				var total int64
				for _, point := range data.DataPoints {
					total += point.Value
				}

				return total, true
			}
		}
	}

	return 0, false
}
