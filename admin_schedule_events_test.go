package worker

import (
	"context"
	"testing"
	"time"
)

const testScheduleName = "tick"

type scheduleAdminBackend struct {
	recorded []AdminScheduleEvent
	events   []AdminScheduleEvent
}

func (*scheduleAdminBackend) Enqueue(context.Context, DurableTask) error { return nil }

func (*scheduleAdminBackend) Dequeue(context.Context, int, time.Duration) ([]DurableTaskLease, error) {
	return nil, nil
}

func (*scheduleAdminBackend) Ack(context.Context, DurableTaskLease) error { return nil }

func (*scheduleAdminBackend) Nack(context.Context, DurableTaskLease, time.Duration) error { return nil }

func (*scheduleAdminBackend) Fail(context.Context, DurableTaskLease, error) error { return nil }

func (*scheduleAdminBackend) Extend(context.Context, DurableTaskLease, time.Duration) error {
	return nil
}

func (*scheduleAdminBackend) AdminOverview(context.Context) (AdminOverview, error) {
	return AdminOverview{}, nil
}

func (*scheduleAdminBackend) AdminQueues(context.Context) ([]AdminQueueSummary, error) {
	return nil, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminQueue(context.Context, string) (AdminQueueSummary, error) {
	return AdminQueueSummary{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminPauseQueue(context.Context, string, bool) (AdminQueueSummary, error) {
	return AdminQueueSummary{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminSetQueueWeight(context.Context, string, int) (AdminQueueSummary, error) {
	return AdminQueueSummary{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminResetQueueWeight(context.Context, string) (AdminQueueSummary, error) {
	return AdminQueueSummary{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminSchedules(context.Context) ([]AdminSchedule, error) {
	return nil, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminScheduleFactories(context.Context) ([]AdminScheduleFactory, error) {
	return nil, ErrAdminUnsupported
}

func (b *scheduleAdminBackend) AdminScheduleEvents(
	_ context.Context,
	filter AdminScheduleEventFilter,
) (AdminScheduleEventPage, error) {
	name := filter.Name
	limit := normalizeAdminEventLimit(filter.Limit, 0)
	filtered := filterAdminEvents(b.events, name, limit, func(event AdminScheduleEvent) string {
		return event.Name
	})

	return AdminScheduleEventPage{Events: filtered}, nil
}

func (*scheduleAdminBackend) AdminPauseSchedules(context.Context, bool) (int, error) {
	return 0, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminRunSchedule(context.Context, string) (string, error) {
	return "", ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminCreateSchedule(context.Context, AdminScheduleSpec) (AdminSchedule, error) {
	return AdminSchedule{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminDeleteSchedule(context.Context, string) (bool, error) {
	return false, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminPauseSchedule(context.Context, string, bool) (AdminSchedule, error) {
	return AdminSchedule{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminDLQ(context.Context, AdminDLQFilter) (AdminDLQPage, error) {
	return AdminDLQPage{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminDLQEntry(context.Context, string) (AdminDLQEntryDetail, error) {
	return AdminDLQEntryDetail{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminPause(context.Context) error { return ErrAdminUnsupported }

func (*scheduleAdminBackend) AdminResume(context.Context) error { return ErrAdminUnsupported }

func (*scheduleAdminBackend) AdminReplayDLQ(context.Context, int) (int, error) {
	return 0, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminReplayDLQByID(context.Context, []string) (int, error) {
	return 0, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminJobs(context.Context) ([]AdminJob, error) {
	return nil, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminJob(context.Context, string) (AdminJob, error) {
	return AdminJob{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminUpsertJob(context.Context, AdminJobSpec) (AdminJob, error) {
	return AdminJob{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminDeleteJob(context.Context, string) (bool, error) {
	return false, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminJobEvents(context.Context, AdminJobEventFilter) (AdminJobEventPage, error) {
	return AdminJobEventPage{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminAuditEvents(context.Context, AdminAuditEventFilter) (AdminAuditEventPage, error) {
	return AdminAuditEventPage{}, ErrAdminUnsupported
}

func (*scheduleAdminBackend) AdminRecordAuditEvent(context.Context, AdminAuditEvent, int) error {
	return ErrAdminUnsupported
}

func (b *scheduleAdminBackend) AdminRecordScheduleEvent(
	_ context.Context,
	event AdminScheduleEvent,
	limit int,
) error {
	b.recorded = append(b.recorded, event)

	b.events = append([]AdminScheduleEvent{event}, b.events...)
	if limit > 0 && len(b.events) > limit {
		b.events = b.events[:limit]
	}

	return nil
}

func TestRecordCronCompletionPersistsScheduleEvent(t *testing.T) {
	t.Parallel()

	backend := &scheduleAdminBackend{}
	tm := NewTaskManagerWithOptions(
		context.Background(),
		WithDurableBackend(backend),
		WithCronLocation(time.UTC),
	)

	task, err := NewTask(context.Background(), func(context.Context, ...any) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("new task: %v", err)
	}

	startedAt := time.Now().UTC().Add(-2 * time.Second)
	finishedAt := startedAt.Add(1500 * time.Millisecond)

	task.mu.Lock()
	task.started = startedAt
	task.completed = finishedAt
	task.mu.Unlock()

	spec := cronSpec{Spec: "*/1 * * * * *", Durable: true}
	tm.noteCronRun(cronRunInfoFromTask(testScheduleName, spec, task, tm.defaultQueue))
	tm.recordCronCompletion(task, Completed, "ok", nil)

	if len(backend.recorded) != 1 {
		t.Fatalf("expected one persisted schedule event, got %d", len(backend.recorded))
	}

	event := backend.recorded[0]
	if event.Name != testScheduleName {
		t.Fatalf("expected schedule name %s, got %q", testScheduleName, event.Name)
	}

	if event.TaskID != task.ID.String() {
		t.Fatalf("expected task id %s, got %s", task.ID, event.TaskID)
	}

	if event.DurationMs != finishedAt.Sub(startedAt).Milliseconds() {
		t.Fatalf("expected duration %d, got %d", finishedAt.Sub(startedAt).Milliseconds(), event.DurationMs)
	}
}

func TestAdminScheduleEventsUsesDurableBackend(t *testing.T) {
	t.Parallel()

	backend := &scheduleAdminBackend{
		events: []AdminScheduleEvent{
			{TaskID: "backend", Name: testScheduleName, Status: adminStatusCompleted},
		},
	}

	tm := NewTaskManagerWithOptions(
		context.Background(),
		WithDurableBackend(backend),
		WithCronLocation(time.UTC),
	)

	tm.cronEventsMu.Lock()
	tm.cronEvents = []AdminScheduleEvent{
		{TaskID: "local", Name: testScheduleName, Status: adminStatusFailed},
	}
	tm.cronEventsMu.Unlock()

	page, err := tm.AdminScheduleEvents(context.Background(), AdminScheduleEventFilter{
		Name:  testScheduleName,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("admin schedule events: %v", err)
	}

	if len(page.Events) != 1 {
		t.Fatalf("expected one schedule event, got %d", len(page.Events))
	}

	if page.Events[0].TaskID != "backend" {
		t.Fatalf("expected durable backend event, got %q", page.Events[0].TaskID)
	}
}
