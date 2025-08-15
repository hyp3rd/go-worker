package worker

import "sync/atomic"

// taskMetrics holds counters for task lifecycle events.
type taskMetrics struct {
	scheduled atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	cancelled atomic.Int64
}

// MetricsSnapshot represents a snapshot of task metrics.
type MetricsSnapshot struct {
	Scheduled int64
	Completed int64
	Failed    int64
	Cancelled int64
}

// GetMetrics returns a snapshot of current metrics.
func (tm *TaskManager) GetMetrics() MetricsSnapshot {
	return MetricsSnapshot{
		Scheduled: tm.metrics.scheduled.Load(),
		Completed: tm.metrics.completed.Load(),
		Failed:    tm.metrics.failed.Load(),
		Cancelled: tm.metrics.cancelled.Load(),
	}
}
