package worker

import (
	"sync/atomic"
	"time"
)

// taskMetrics holds counters for task lifecycle events.
type taskMetrics struct {
	scheduled      atomic.Int64
	running        atomic.Int64
	completed      atomic.Int64
	failed         atomic.Int64
	cancelled      atomic.Int64
	retried        atomic.Int64
	latencyCount   atomic.Int64
	latencyTotalNs atomic.Int64
	latencyMaxNs   atomic.Int64
}

// MetricsSnapshot represents a snapshot of task metrics.
type MetricsSnapshot struct {
	Scheduled        int64
	Running          int64
	Completed        int64
	Failed           int64
	Cancelled        int64
	Retried          int64
	ResultsDropped   int64
	QueueDepth       int
	TaskLatencyCount int64
	TaskLatencyTotal time.Duration
	TaskLatencyMax   time.Duration
}

// GetMetrics returns a snapshot of current metrics.
func (tm *TaskManager) GetMetrics() MetricsSnapshot {
	queueDepth := tm.queueDepth()
	latencyCount := tm.metrics.latencyCount.Load()
	latencyTotal := time.Duration(tm.metrics.latencyTotalNs.Load())
	latencyMax := time.Duration(tm.metrics.latencyMaxNs.Load())

	return MetricsSnapshot{
		Scheduled:        tm.metrics.scheduled.Load(),
		Running:          tm.metrics.running.Load(),
		Completed:        tm.metrics.completed.Load(),
		Failed:           tm.metrics.failed.Load(),
		Cancelled:        tm.metrics.cancelled.Load(),
		Retried:          tm.metrics.retried.Load(),
		ResultsDropped:   tm.results.Drops(),
		QueueDepth:       queueDepth,
		TaskLatencyCount: latencyCount,
		TaskLatencyTotal: latencyTotal,
		TaskLatencyMax:   latencyMax,
	}
}
