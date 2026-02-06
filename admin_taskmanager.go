package worker

import (
	"context"
	"sort"
	"strings"
	"time"

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

	overview.Actions = tm.adminActions.snapshot()

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

	tm.cronMu.RLock()
	defer tm.cronMu.RUnlock()

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

		nextRun := entry.Next
		if spec.Paused {
			nextRun = time.Time{}
		}

		results = append(results, AdminSchedule{
			Name:    name,
			Spec:    spec.Spec,
			NextRun: nextRun,
			LastRun: entry.Prev,
			Durable: spec.Durable,
			Paused:  spec.Paused,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	return results, nil
}

// AdminCreateSchedule creates or updates a cron schedule by name.
func (tm *TaskManager) AdminCreateSchedule(ctx context.Context, spec AdminScheduleSpec) (AdminSchedule, error) {
	if tm == nil {
		return AdminSchedule{}, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return AdminSchedule{}, ErrInvalidTaskContext
	}

	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return AdminSchedule{}, ErrAdminScheduleNameRequired
	}

	specValue := strings.TrimSpace(spec.Spec)
	if specValue == "" {
		return AdminSchedule{}, ErrAdminScheduleSpecRequired
	}

	schedule, err := parseCronSpec(specValue, tm.cronLoc)
	if err != nil {
		return AdminSchedule{}, err
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron == nil {
		return AdminSchedule{}, ErrAdminBackendUnavailable
	}

	factory, ok := tm.cronFactories[name]
	if !ok {
		return AdminSchedule{}, ErrAdminScheduleFactoryMissing
	}

	if factory.Durable != spec.Durable {
		return AdminSchedule{}, ErrAdminScheduleDurableMismatch
	}

	if existing, ok := tm.cronEntries[name]; ok {
		tm.cron.Remove(existing)
	}

	entryID := tm.cron.Schedule(schedule, cron.FuncJob(tm.cronJob(name)))
	tm.cronEntries[name] = entryID
	tm.cronSpecs[name] = cronSpec{Spec: specValue, Durable: factory.Durable}

	entry := tm.cron.Entry(entryID)
	nextRun := entry.Next

	return AdminSchedule{
		Name:    name,
		Spec:    specValue,
		NextRun: nextRun,
		LastRun: entry.Prev,
		Durable: factory.Durable,
		Paused:  false,
	}, nil
}

// AdminDeleteSchedule removes a cron schedule by name.
func (tm *TaskManager) AdminDeleteSchedule(ctx context.Context, name string) (bool, error) {
	if tm == nil {
		return false, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return false, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return false, ErrAdminScheduleNameRequired
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron == nil {
		return false, ErrAdminBackendUnavailable
	}

	entryID, ok := tm.cronEntries[name]
	if !ok {
		return false, ErrAdminScheduleNotFound
	}

	tm.cron.Remove(entryID)
	delete(tm.cronEntries, name)
	delete(tm.cronSpecs, name)

	return true, nil
}

// AdminPauseSchedule pauses or resumes a cron schedule by name.
func (tm *TaskManager) AdminPauseSchedule(ctx context.Context, name string, paused bool) (AdminSchedule, error) {
	if tm == nil {
		return AdminSchedule{}, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return AdminSchedule{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminSchedule{}, ErrAdminScheduleNameRequired
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron == nil {
		return AdminSchedule{}, ErrAdminBackendUnavailable
	}

	spec, ok := tm.cronSpecs[name]
	if !ok {
		return AdminSchedule{}, ErrAdminScheduleNotFound
	}

	spec.Paused = paused
	tm.cronSpecs[name] = spec

	entry := tm.cron.Entry(tm.cronEntries[name])

	nextRun := entry.Next
	if paused {
		nextRun = time.Time{}
	}

	return AdminSchedule{
		Name:    name,
		Spec:    spec.Spec,
		NextRun: nextRun,
		LastRun: entry.Prev,
		Durable: spec.Durable,
		Paused:  spec.Paused,
	}, nil
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

	err = backend.AdminPause(ctx)
	if err != nil {
		return err
	}

	tm.adminActions.incPause()

	return nil
}

// AdminResume resumes durable dequeue.
func (tm *TaskManager) AdminResume(ctx context.Context) error {
	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	err = backend.AdminResume(ctx)
	if err != nil {
		return err
	}

	tm.adminActions.incResume()

	return nil
}

// AdminReplayDLQ replays DLQ entries.
func (tm *TaskManager) AdminReplayDLQ(ctx context.Context, limit int) (int, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return 0, err
	}

	moved, err := backend.AdminReplayDLQ(ctx, limit)
	if err != nil {
		return 0, err
	}

	tm.adminActions.incReplay()

	return moved, nil
}

// AdminReplayDLQByID replays specific DLQ entries by ID.
func (tm *TaskManager) AdminReplayDLQByID(ctx context.Context, ids []string) (int, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return 0, err
	}

	moved, err := backend.AdminReplayDLQByID(ctx, ids)
	if err != nil {
		return 0, err
	}

	tm.adminActions.incReplay()

	return moved, nil
}
