package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/hyp3rd/ewrap"
	"go.opentelemetry.io/otel/metric"
)

const (
	otelMeterName                 = "github.com/hyp3rd/go-worker"
	otelMetricTasksScheduledTotal = "tasks_scheduled_total"
	otelMetricTasksRunning        = "tasks_running"
	otelMetricTasksCompletedTotal = "tasks_completed_total"
	otelMetricTasksFailedTotal    = "tasks_failed_total"
	otelMetricTasksCancelledTotal = "tasks_cancelled_total"
	otelMetricTasksRetriedTotal   = "tasks_retried_total"
	otelMetricResultsDroppedTotal = "results_dropped_total"
	otelMetricQueueDepth          = "queue_depth"
	otelMetricTaskLatencySeconds  = "task_latency_seconds"
	otelMetricLatencyUnit         = "s"
	errMsgOTelMetricsInit         = "otel metrics initialization failed"
)

const errMsgCreateMetricFailed = "%s: create %s"

// OTelMetricsOption configures OpenTelemetry metrics.
type OTelMetricsOption func(*otelMetricsConfig)

type otelMetricsConfig struct {
	meterName    string
	meterVersion string
}

// WithOTelMeterName overrides the default OTel meter name.
func WithOTelMeterName(name string) OTelMetricsOption {
	return func(cfg *otelMetricsConfig) {
		if name != "" {
			cfg.meterName = name
		}
	}
}

// WithOTelMeterVersion sets the instrumentation version reported by the meter.
func WithOTelMeterVersion(version string) OTelMetricsOption {
	return func(cfg *otelMetricsConfig) {
		cfg.meterVersion = version
	}
}

type otelMetrics struct {
	scheduled      metric.Int64ObservableCounter
	running        metric.Int64ObservableGauge
	completed      metric.Int64ObservableCounter
	failed         metric.Int64ObservableCounter
	cancelled      metric.Int64ObservableCounter
	retried        metric.Int64ObservableCounter
	resultsDropped metric.Int64ObservableCounter
	queueDepth     metric.Int64ObservableGauge
	latency        metric.Float64Histogram
	registration   metric.Registration
}

// SetMeterProvider enables OpenTelemetry metrics collection. Passing nil disables it.
func (tm *TaskManager) SetMeterProvider(provider metric.MeterProvider, opts ...OTelMetricsOption) error {
	if provider == nil {
		tm.disableOTelMetrics()

		return nil
	}

	cfg := otelMetricsConfig{meterName: otelMeterName}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.meterName == "" {
		cfg.meterName = otelMeterName
	}

	meter := provider.Meter(cfg.meterName, metric.WithInstrumentationVersion(cfg.meterVersion))

	metrics, err := newOTelMetrics(tm, meter)
	if err != nil {
		return err
	}

	old := tm.otel.Swap(metrics)
	if old != nil {
		old.unregister(tm.ctx)
	}

	return nil
}

func (tm *TaskManager) disableOTelMetrics() {
	old := tm.otel.Swap(nil)
	if old != nil {
		old.unregister(tm.ctx)
	}
}

func newOTelMetrics(tm *TaskManager, meter metric.Meter) (*otelMetrics, error) {
	metrics, err := buildOTelMetrics(meter)
	if err != nil {
		return nil, err
	}

	err = registerOTelCallback(tm, meter, metrics)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func buildOTelMetrics(meter metric.Meter) (*otelMetrics, error) {
	metrics := &otelMetrics{}

	var err error

	metrics.scheduled, err = createInt64Counter(meter, otelMetricTasksScheduledTotal, "Total number of tasks scheduled")
	if err != nil {
		return nil, err
	}

	metrics.running, err = createInt64Gauge(meter, otelMetricTasksRunning, "Number of tasks running")
	if err != nil {
		return nil, err
	}

	metrics.completed, err = createInt64Counter(meter, otelMetricTasksCompletedTotal, "Total number of tasks completed")
	if err != nil {
		return nil, err
	}

	metrics.failed, err = createInt64Counter(meter, otelMetricTasksFailedTotal, "Total number of tasks failed")
	if err != nil {
		return nil, err
	}

	metrics.cancelled, err = createInt64Counter(meter, otelMetricTasksCancelledTotal, "Total number of tasks cancelled")
	if err != nil {
		return nil, err
	}

	metrics.retried, err = createInt64Counter(meter, otelMetricTasksRetriedTotal, "Total number of task retries")
	if err != nil {
		return nil, err
	}

	metrics.resultsDropped, err = createInt64Counter(meter, otelMetricResultsDroppedTotal, "Total number of dropped results")
	if err != nil {
		return nil, err
	}

	metrics.queueDepth, err = createInt64Gauge(meter, otelMetricQueueDepth, "Number of tasks queued")
	if err != nil {
		return nil, err
	}

	metrics.latency, err = createFloat64Histogram(meter, otelMetricTaskLatencySeconds, "Task execution latency", otelMetricLatencyUnit)
	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func registerOTelCallback(tm *TaskManager, meter metric.Meter, metrics *otelMetrics) error {
	registration, err := meter.RegisterCallback(func(_ context.Context, metricObserver metric.Observer) error {
		metricObserver.ObserveInt64(metrics.scheduled, tm.metrics.scheduled.Load())
		metricObserver.ObserveInt64(metrics.running, tm.metrics.running.Load())
		metricObserver.ObserveInt64(metrics.completed, tm.metrics.completed.Load())
		metricObserver.ObserveInt64(metrics.failed, tm.metrics.failed.Load())
		metricObserver.ObserveInt64(metrics.cancelled, tm.metrics.cancelled.Load())
		metricObserver.ObserveInt64(metrics.retried, tm.metrics.retried.Load())
		metricObserver.ObserveInt64(metrics.resultsDropped, tm.results.Drops())
		metricObserver.ObserveInt64(metrics.queueDepth, int64(tm.queueDepth()))

		return nil
	}, metrics.scheduled,
		metrics.running,
		metrics.completed,
		metrics.failed,
		metrics.cancelled,
		metrics.retried,
		metrics.resultsDropped,
		metrics.queueDepth)
	if err != nil {
		return ewrap.Wrapf(err, "%s: register callback", errMsgOTelMetricsInit)
	}

	metrics.registration = registration

	return nil
}

func createInt64Counter(meter metric.Meter, name, description string) (metric.Int64ObservableCounter, error) {
	counter, err := meter.Int64ObservableCounter(name, metric.WithDescription(description))
	if err != nil {
		return counter, ewrap.Wrapf(err, errMsgCreateMetricFailed, errMsgOTelMetricsInit, name)
	}

	return counter, nil
}

func createInt64Gauge(meter metric.Meter, name, description string) (metric.Int64ObservableGauge, error) {
	gauge, err := meter.Int64ObservableGauge(name, metric.WithDescription(description))
	if err != nil {
		return gauge, ewrap.Wrapf(err, errMsgCreateMetricFailed, errMsgOTelMetricsInit, name)
	}

	return gauge, nil
}

func createFloat64Histogram(meter metric.Meter, name, description, unit string) (metric.Float64Histogram, error) {
	histogram, err := meter.Float64Histogram(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if err != nil {
		return histogram, ewrap.Wrapf(err, errMsgCreateMetricFailed, errMsgOTelMetricsInit, name)
	}

	return histogram, nil
}

func (tm *TaskManager) recordOTelLatency(latency time.Duration) {
	metrics := tm.otel.Load()
	if metrics == nil {
		return
	}

	if tm.ctx == nil {
		return
	}

	metrics.latency.Record(tm.ctx, latency.Seconds())
}

func (m *otelMetrics) unregister(ctx context.Context) {
	if m == nil || m.registration == nil {
		return
	}

	err := m.registration.Unregister()
	if err != nil {
		if ctx == nil {
			return
		}

		logger := slog.Default()
		logger.Log(ctx, slog.LevelWarn, "failed to unregister otel metrics callback", "error", err)
	}
}
