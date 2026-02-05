package worker

import (
	"context"
	"sort"

	"github.com/robfig/cron/v3"
)

func (tm *TaskManager) adminBackend() (AdminBackend, error) {
	if tm == nil {
		return nil, ErrAdminBackendUnavailable
	}

	if tm.durableBackend == nil {
		return nil, ErrAdminBackendUnavailable
	}

	backend, ok := tm.durableBackend.(AdminBackend)
	if !ok {
		return nil, ErrAdminUnsupported
	}

	return backend, nil
}

// AdminOverview returns an admin overview snapshot.
func (tm *TaskManager) AdminOverview(ctx context.Context) (AdminOverview, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminOverview{}, err
	}

	overview, err := backend.AdminOverview(ctx)
	if err != nil {
		return AdminOverview{}, err
	}

	if overview.ActiveWorkers < 0 {
		overview.ActiveWorkers = tm.GetActiveTasks()
	}

	return overview, nil
}

// AdminQueues lists queue summaries.
func (tm *TaskManager) AdminQueues(ctx context.Context) ([]AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return nil, err
	}

	return backend.AdminQueues(ctx)
}

// AdminQueue returns a queue summary by name.
func (tm *TaskManager) AdminQueue(ctx context.Context, name string) (AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminQueueSummary{}, err
	}

	return backend.AdminQueue(ctx, name)
}

// AdminSchedules lists cron schedules.
func (tm *TaskManager) AdminSchedules(ctx context.Context) ([]AdminSchedule, error) {
	if tm == nil {
		return nil, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron == nil {
		return []AdminSchedule{}, nil
	}

	nameByID := make(map[cron.EntryID]string, len(tm.cronEntries))
	for name, id := range tm.cronEntries {
		nameByID[id] = name
	}

	results := make([]AdminSchedule, 0, len(tm.cronEntries))
	for _, entry := range tm.cron.Entries() {
		name := nameByID[entry.ID]
		if name == "" {
			continue
		}

		spec := tm.cronSpecs[name]
		results = append(results, AdminSchedule{
			Name:    name,
			Spec:    spec.Spec,
			NextRun: entry.Next,
			LastRun: entry.Prev,
			Durable: spec.Durable,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	return results, nil
}

// AdminDLQ lists DLQ entries.
func (tm *TaskManager) AdminDLQ(ctx context.Context, filter AdminDLQFilter) (AdminDLQPage, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminDLQPage{}, err
	}

	return backend.AdminDLQ(ctx, filter)
}

// AdminPause pauses durable dequeue.
func (tm *TaskManager) AdminPause(ctx context.Context) error {
	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	return backend.AdminPause(ctx)
}

// AdminResume resumes durable dequeue.
func (tm *TaskManager) AdminResume(ctx context.Context) error {
	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	return backend.AdminResume(ctx)
}

// AdminReplayDLQ replays DLQ entries.
func (tm *TaskManager) AdminReplayDLQ(ctx context.Context, limit int) (int, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return 0, err
	}

	return backend.AdminReplayDLQ(ctx, limit)
}
