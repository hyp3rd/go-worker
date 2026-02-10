package worker

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"time"
)

const (
	defaultAdminJobEventLimit   = 200
	jobEventResultMaxLen        = 240
	adminJobEventPersistTimeout = 2 * time.Second
	adminStatusRateLimited      = "rate_limited"
	adminStatusQueued           = "queued"
	adminStatusRunning          = "running"
	adminStatusDeadline         = "deadline"
	adminStatusCompleted        = "completed"
	adminStatusFailed           = "failed"
	adminStatusCancelled        = "cancelled"
	adminStatusInvalid          = "invalid"
	adminStatusUnknown          = "unknown"
)

type adminJobEventRecorder interface {
	AdminRecordJobEvent(ctx context.Context, event AdminJobEvent, limit int) error
}

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

	tm.jobEvents = append(tm.jobEvents, event)
	if tm.jobEventLimit > 0 && len(tm.jobEvents) > tm.jobEventLimit {
		tm.jobEvents = tm.jobEvents[len(tm.jobEvents)-tm.jobEventLimit:]
	}

	tm.jobEventsMu.Unlock()

	tm.persistJobEvent(task.Ctx, event)
}

func (tm *TaskManager) recordJobStart(task *Task) {
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
		Status:       jobStatusLabel(Running),
		Queue:        jobQueue(task, meta, tm.defaultQueue),
		Repo:         strings.TrimSpace(meta[jobMetaRepoKey]),
		Tag:          strings.TrimSpace(meta[jobMetaTagKey]),
		Path:         strings.TrimSpace(meta[jobMetaPathKey]),
		Dockerfile:   strings.TrimSpace(meta[jobMetaDockerfileKey]),
		Command:      strings.TrimSpace(meta[jobMetaCommandKey]),
		ScheduleName: strings.TrimSpace(meta[cronMetaNameKey]),
		ScheduleSpec: strings.TrimSpace(meta[cronMetaSpecKey]),
		StartedAt:    task.StartedAt(),
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

	tm.jobEventsMu.Lock()

	tm.jobEvents = append(tm.jobEvents, event)
	if tm.jobEventLimit > 0 && len(tm.jobEvents) > tm.jobEventLimit {
		tm.jobEvents = tm.jobEvents[len(tm.jobEvents)-tm.jobEventLimit:]
	}

	tm.jobEventsMu.Unlock()

	tm.persistJobEvent(task.Ctx, event)
}

func (tm *TaskManager) recordJobQueued(task DurableTask) {
	if tm == nil {
		return
	}

	meta := task.Metadata

	handler := strings.TrimSpace(task.Handler)
	if handler != JobHandlerName && meta[jobMetaNameKey] == "" {
		return
	}

	event := AdminJobEvent{
		TaskID:       task.ID.String(),
		Name:         strings.TrimSpace(meta[jobMetaNameKey]),
		Status:       jobStatusLabel(Queued),
		Queue:        jobQueue(nil, meta, tm.defaultQueue),
		Repo:         strings.TrimSpace(meta[jobMetaRepoKey]),
		Tag:          strings.TrimSpace(meta[jobMetaTagKey]),
		Path:         strings.TrimSpace(meta[jobMetaPathKey]),
		Dockerfile:   strings.TrimSpace(meta[jobMetaDockerfileKey]),
		Command:      strings.TrimSpace(meta[jobMetaCommandKey]),
		ScheduleName: strings.TrimSpace(meta[cronMetaNameKey]),
		ScheduleSpec: strings.TrimSpace(meta[cronMetaSpecKey]),
		StartedAt:    time.Now(),
		Metadata:     sanitizeJobMetadata(meta),
	}

	if event.Name == "" {
		if handler != "" {
			event.Name = handler
		} else {
			event.Name = task.ID.String()
		}
	}

	tm.jobEventsMu.Lock()

	tm.jobEvents = append(tm.jobEvents, event)
	if tm.jobEventLimit > 0 && len(tm.jobEvents) > tm.jobEventLimit {
		tm.jobEvents = tm.jobEvents[len(tm.jobEvents)-tm.jobEventLimit:]
	}

	tm.jobEventsMu.Unlock()

	tm.persistJobEvent(tm.ctx, event)
}

func (tm *TaskManager) persistJobEvent(ctx context.Context, event AdminJobEvent) {
	if tm == nil {
		return
	}

	recorder, ok := tm.durableBackend.(adminJobEventRecorder)
	if !ok || recorder == nil {
		return
	}

	if ctx == nil {
		return
	}

	ctx = context.WithoutCancel(ctx)

	persistCtx, cancel := context.WithTimeout(ctx, adminJobEventPersistTimeout)
	defer cancel()

	err := recorder.AdminRecordJobEvent(persistCtx, event, tm.jobEventLimit)
	if err != nil {
		// Best-effort persistence; ignore to avoid blocking task completion.
		fmt.Fprintln(os.Stderr, err)
	}
}

func jobStatusLabel(status TaskStatus) string {
	switch status {
	case RateLimited:
		return adminStatusRateLimited
	case Queued:
		return adminStatusQueued
	case Running:
		return adminStatusRunning
	case ContextDeadlineReached:
		return adminStatusDeadline
	case Completed:
		return adminStatusCompleted
	case Failed:
		return adminStatusFailed
	case Cancelled:
		return adminStatusCancelled
	case Invalid:
		return adminStatusInvalid
	}

	return adminStatusUnknown
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
