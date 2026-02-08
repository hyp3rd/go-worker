package worker

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	defaultAdminJobEventLimit = 200
	jobEventResultMaxLen      = 240
)

func (tm *TaskManager) recordJobCompletion(task *Task, status TaskStatus, result any, err error) {
	if tm == nil || task == nil {
		return
	}

	meta := jobMetadataFromTask(task)

	handler := jobHandlerFromTask(task)
	if handler != JobHandlerName && meta[jobMetaNameKey] == "" {
		return
	}

	event := AdminJobEvent{
		TaskID:       task.ID.String(),
		Name:         strings.TrimSpace(meta[jobMetaNameKey]),
		Status:       jobStatusLabel(status),
		Queue:        jobQueue(task, meta, tm.defaultQueue),
		Repo:         strings.TrimSpace(meta[jobMetaRepoKey]),
		Tag:          strings.TrimSpace(meta[jobMetaTagKey]),
		Path:         strings.TrimSpace(meta[jobMetaPathKey]),
		Dockerfile:   strings.TrimSpace(meta[jobMetaDockerfileKey]),
		Command:      strings.TrimSpace(meta[jobMetaCommandKey]),
		ScheduleName: strings.TrimSpace(meta[cronMetaNameKey]),
		ScheduleSpec: strings.TrimSpace(meta[cronMetaSpecKey]),
		Result:       formatJobResult(result),
		Error:        formatJobError(err),
		StartedAt:    task.StartedAt(),
		FinishedAt:   jobFinishedAt(task, status),
		Metadata:     sanitizeJobMetadata(meta),
	}

	if event.Name == "" {
		if handler != "" {
			event.Name = handler
		} else {
			event.Name = task.Name
		}
	}

	if event.StartedAt.IsZero() {
		event.StartedAt = time.Now()
	}

	if !event.StartedAt.IsZero() && !event.FinishedAt.IsZero() {
		event.DurationMs = event.FinishedAt.Sub(event.StartedAt).Milliseconds()
	}

	tm.jobEventsMu.Lock()
	defer tm.jobEventsMu.Unlock()

	tm.jobEvents = append(tm.jobEvents, event)
	if tm.jobEventLimit <= 0 {
		return
	}

	if len(tm.jobEvents) > tm.jobEventLimit {
		tm.jobEvents = tm.jobEvents[len(tm.jobEvents)-tm.jobEventLimit:]
	}
}

func jobStatusLabel(status TaskStatus) string {
	switch status {
	case RateLimited:
		return "rate_limited"
	case Queued:
		return "queued"
	case Running:
		return "running"
	case ContextDeadlineReached:
		return "deadline"
	case Completed:
		return "completed"
	case Failed:
		return "failed"
	case Cancelled:
		return "cancelled"
	case Invalid:
		return "invalid"
	}

	return "unknown"
}

func jobFinishedAt(task *Task, status TaskStatus) time.Time {
	if task == nil {
		return time.Time{}
	}

	switch status {
	case Cancelled:
		return task.CancelledAt()
	case Completed, Failed, Invalid, ContextDeadlineReached:
		return task.CompletedAt()
	case RateLimited, Queued, Running:
		return time.Time{}
	}

	return time.Time{}
}

func formatJobResult(result any) string {
	if result == nil {
		return ""
	}

	var raw string

	switch value := result.(type) {
	case string:
		raw = value
	case []byte:
		raw = string(value)
	default:
		raw = fmt.Sprintf("%v", value)
	}

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if len(raw) > jobEventResultMaxLen {
		return raw[:jobEventResultMaxLen] + "..."
	}

	return raw
}

func formatJobError(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return ""
	}

	if len(msg) > jobEventResultMaxLen {
		return msg[:jobEventResultMaxLen] + "..."
	}

	return msg
}

func sanitizeJobMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}

	out := make(map[string]string, len(meta))
	maps.Copy(out, meta)

	delete(out, jobMetaNameKey)
	delete(out, jobMetaRepoKey)
	delete(out, jobMetaTagKey)
	delete(out, jobMetaSourceKey)
	delete(out, jobMetaTarballURLKey)
	delete(out, jobMetaTarballPathKey)
	delete(out, jobMetaTarballSHAKey)
	delete(out, jobMetaPathKey)
	delete(out, jobMetaDockerfileKey)
	delete(out, jobMetaCommandKey)
	delete(out, jobMetaQueueKey)
	delete(out, cronMetaNameKey)
	delete(out, cronMetaSpecKey)
	delete(out, cronMetaDurableKey)
	delete(out, cronMetaQueueKey)

	if len(out) == 0 {
		return nil
	}

	return out
}

func jobQueue(task *Task, meta map[string]string, fallback string) string {
	queue := strings.TrimSpace(meta[jobMetaQueueKey])
	if queue != "" {
		return queue
	}

	if task != nil && task.Queue != "" {
		return task.Queue
	}

	return fallback
}

func jobMetadataFromTask(task *Task) map[string]string {
	if task == nil || task.durableLease == nil {
		return nil
	}

	return task.durableLease.Task.Metadata
}

func jobHandlerFromTask(task *Task) string {
	if task == nil || task.durableLease == nil {
		return ""
	}

	return task.durableLease.Task.Handler
}
