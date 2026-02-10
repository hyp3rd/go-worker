package worker

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultAdminScheduleEventLimit = 200
	cronMetaNameKey                = "worker.schedule.name"
	cronMetaSpecKey                = "worker.schedule.spec"
	cronMetaDurableKey             = "worker.schedule.durable"
	cronMetaQueueKey               = "worker.schedule.queue"
	cronEventResultMaxLen          = 240
)

type cronRunInfo struct {
	id         uuid.UUID
	name       string
	spec       string
	durable    bool
	queue      string
	enqueuedAt time.Time
	metadata   map[string]string
}

func (tm *TaskManager) noteCronRun(info cronRunInfo) {
	if tm == nil {
		return
	}

	if info.id == uuid.Nil {
		return
	}

	tm.cronEventsMu.Lock()
	defer tm.cronEventsMu.Unlock()

	if tm.cronRuns == nil {
		tm.cronRuns = map[uuid.UUID]cronRunInfo{}
	}

	if info.queue == "" {
		info.queue = tm.defaultQueue
	}

	if _, exists := tm.cronRuns[info.id]; exists {
		return
	}

	tm.cronRuns[info.id] = info
}

func (tm *TaskManager) dropCronRun(id uuid.UUID) {
	if tm == nil || id == uuid.Nil {
		return
	}

	tm.cronEventsMu.Lock()

	if tm.cronRuns != nil {
		delete(tm.cronRuns, id)
	}

	tm.cronEventsMu.Unlock()
}

func (tm *TaskManager) recordCronCompletion(task *Task, status TaskStatus, result any, err error) {
	if tm == nil || task == nil {
		return
	}

	tm.cronEventsMu.Lock()
	defer tm.cronEventsMu.Unlock()

	if tm.cronRuns == nil {
		return
	}

	info, ok := tm.cronRuns[task.ID]
	if !ok {
		return
	}

	delete(tm.cronRuns, task.ID)

	event := AdminScheduleEvent{
		TaskID:     task.ID.String(),
		Name:       info.name,
		Spec:       info.spec,
		Durable:    info.durable,
		Status:     cronStatusLabel(status),
		Queue:      info.queue,
		Metadata:   sanitizeCronMetadata(info.metadata),
		Result:     formatCronResult(result),
		Error:      formatCronError(err),
		StartedAt:  task.StartedAt(),
		FinishedAt: cronFinishedAt(task, status),
	}

	if event.StartedAt.IsZero() {
		event.StartedAt = info.enqueuedAt
	}

	if !event.StartedAt.IsZero() && !event.FinishedAt.IsZero() {
		event.DurationMs = event.FinishedAt.Sub(event.StartedAt).Milliseconds()
	}

	tm.appendCronEventLocked(event)
}

func (tm *TaskManager) appendCronEventLocked(event AdminScheduleEvent) {
	tm.cronEvents = append(tm.cronEvents, event)
	if tm.cronEventLimit <= 0 {
		return
	}

	if len(tm.cronEvents) > tm.cronEventLimit {
		tm.cronEvents = tm.cronEvents[len(tm.cronEvents)-tm.cronEventLimit:]
	}
}

func cronStatusLabel(status TaskStatus) string {
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

func cronFinishedAt(task *Task, status TaskStatus) time.Time {
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

func formatCronResult(result any) string {
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

	if len(raw) > cronEventResultMaxLen {
		return raw[:cronEventResultMaxLen] + "..."
	}

	return raw
}

func formatCronError(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return ""
	}

	if len(msg) > cronEventResultMaxLen {
		return msg[:cronEventResultMaxLen] + "..."
	}

	return msg
}

func sanitizeCronMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}

	out := make(map[string]string, len(meta))
	for key, value := range meta {
		switch key {
		case cronMetaNameKey, cronMetaSpecKey, cronMetaDurableKey, cronMetaQueueKey:
			continue
		default:
			out[key] = value
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func cronRunInfoFromTask(name string, spec cronSpec, task *Task, defaultQueue string) cronRunInfo {
	info := cronRunInfo{
		id:         task.ID,
		name:       name,
		spec:       spec.Spec,
		durable:    spec.Durable,
		queue:      task.Queue,
		enqueuedAt: time.Now(),
		metadata:   cronMetadataFromTask(task),
	}

	if info.queue == "" {
		info.queue = defaultQueue
	}

	return info
}

func cronMetadataFromTask(task *Task) map[string]string {
	if task == nil {
		return nil
	}

	meta := map[string]string{}
	if task.Name != "" {
		meta["task_name"] = task.Name
	}

	if task.Description != "" {
		meta["description"] = task.Description
	}

	if len(meta) == 0 {
		return nil
	}

	return meta
}

func ensureCronMetadata(task *DurableTask, name string, spec cronSpec, defaultQueue string) {
	if task == nil {
		return
	}

	if task.Metadata == nil {
		task.Metadata = map[string]string{}
	}

	task.Metadata[cronMetaNameKey] = name
	task.Metadata[cronMetaSpecKey] = spec.Spec
	task.Metadata[cronMetaDurableKey] = strconv.FormatBool(spec.Durable)

	queue := task.Queue
	if queue == "" {
		queue = defaultQueue
	}

	task.Metadata[cronMetaQueueKey] = queue
}

func cronRunInfoFromDurable(task DurableTask, defaultQueue string) (cronRunInfo, bool) {
	meta := task.Metadata
	if len(meta) == 0 {
		return cronRunInfo{}, false
	}

	name := strings.TrimSpace(meta[cronMetaNameKey])
	if name == "" {
		return cronRunInfo{}, false
	}

	spec := strings.TrimSpace(meta[cronMetaSpecKey])
	durable := true

	if raw, ok := meta[cronMetaDurableKey]; ok {
		parsed, err := strconv.ParseBool(raw)
		if err == nil {
			durable = parsed
		}
	}

	queue := task.Queue
	if queue == "" {
		queue = strings.TrimSpace(meta[cronMetaQueueKey])
	}

	if queue == "" {
		queue = defaultQueue
	}

	metadata := copyStringMap(meta)
	if metadata == nil {
		metadata = map[string]string{}
	}

	if task.Handler != "" {
		if _, ok := metadata["handler"]; !ok {
			metadata["handler"] = task.Handler
		}
	}

	return cronRunInfo{
		id:         task.ID,
		name:       name,
		spec:       spec,
		durable:    durable,
		queue:      queue,
		enqueuedAt: time.Now(),
		metadata:   metadata,
	}, true
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	out := make(map[string]string, len(values))
	maps.Copy(out, values)

	return out
}
