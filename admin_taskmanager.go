package worker

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
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

// AdminPauseQueue pauses or resumes a specific queue.
func (tm *TaskManager) AdminPauseQueue(ctx context.Context, name string, paused bool) (AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminQueueSummary{}, err
	}

	return backend.AdminPauseQueue(ctx, name, paused)
}

// AdminSetQueueWeight updates queue weight and returns the updated summary.
func (tm *TaskManager) AdminSetQueueWeight(ctx context.Context, name string, weight int) (AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminQueueSummary{}, err
	}

	return backend.AdminSetQueueWeight(ctx, name, weight)
}

// AdminResetQueueWeight resets a queue weight to default.
func (tm *TaskManager) AdminResetQueueWeight(ctx context.Context, name string) (AdminQueueSummary, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminQueueSummary{}, err
	}

	return backend.AdminResetQueueWeight(ctx, name)
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

// AdminScheduleFactories lists registered schedule factories.
func (tm *TaskManager) AdminScheduleFactories(ctx context.Context) ([]AdminScheduleFactory, error) {
	if tm == nil {
		return nil, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	tm.cronMu.RLock()
	defer tm.cronMu.RUnlock()

	if tm.cron == nil {
		return []AdminScheduleFactory{}, nil
	}

	factories := make([]AdminScheduleFactory, 0, len(tm.cronFactories))
	for name, factory := range tm.cronFactories {
		factories = append(factories, AdminScheduleFactory{
			Name:    name,
			Durable: factory.Durable,
		})
	}

	sort.Slice(factories, func(i, j int) bool {
		return factories[i].Name < factories[j].Name
	})

	return factories, nil
}

// AdminScheduleEvents returns recent cron schedule execution events.
func (tm *TaskManager) AdminScheduleEvents(
	ctx context.Context,
	filter AdminScheduleEventFilter,
) (AdminScheduleEventPage, error) {
	if tm == nil {
		return AdminScheduleEventPage{}, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return AdminScheduleEventPage{}, ErrInvalidTaskContext
	}

	tm.cronEventsMu.RLock()
	events := make([]AdminScheduleEvent, len(tm.cronEvents))
	copy(events, tm.cronEvents)
	tm.cronEventsMu.RUnlock()

	name := strings.TrimSpace(filter.Name)
	limit := normalizeAdminEventLimit(filter.Limit, tm.cronEventLimit)
	filtered := filterAdminEvents(events, name, limit, func(event AdminScheduleEvent) string {
		return event.Name
	})

	return AdminScheduleEventPage{Events: filtered}, nil
}

// AdminJobEvents returns recent job execution events.
func (tm *TaskManager) AdminJobEvents(
	ctx context.Context,
	filter AdminJobEventFilter,
) (AdminJobEventPage, error) {
	if tm == nil {
		return AdminJobEventPage{}, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return AdminJobEventPage{}, ErrInvalidTaskContext
	}

	backend, err := tm.adminBackend()
	if err == nil && backend != nil {
		page, fetchErr := backend.AdminJobEvents(ctx, filter)
		if fetchErr == nil {
			return page, nil
		}

		if !errors.Is(fetchErr, ErrAdminUnsupported) {
			return AdminJobEventPage{}, fetchErr
		}
	}

	tm.jobEventsMu.RLock()
	events := make([]AdminJobEvent, len(tm.jobEvents))
	copy(events, tm.jobEvents)
	tm.jobEventsMu.RUnlock()

	name := strings.TrimSpace(filter.Name)
	limit := normalizeAdminEventLimit(filter.Limit, tm.jobEventLimit)
	filtered := filterAdminEvents(events, name, limit, func(event AdminJobEvent) string {
		return event.Name
	})

	return AdminJobEventPage{Events: filtered}, nil
}

func normalizeAdminEventLimit(limit, maxLimit int) int {
	if limit <= 0 {
		limit = defaultAdminJobEventLimit
	}

	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}

	return limit
}

func filterAdminEvents[T any](events []T, name string, limit int, nameFn func(T) string) []T {
	filtered := make([]T, 0, min(limit, len(events)))
	for i := len(events) - 1; i >= 0 && len(filtered) < limit; i-- {
		event := events[i]
		if name != "" && nameFn(event) != name {
			continue
		}

		filtered = append(filtered, event)
	}

	return filtered
}

// AdminPauseSchedules pauses or resumes all cron schedules.
func (tm *TaskManager) AdminPauseSchedules(ctx context.Context, paused bool) (int, error) {
	if tm == nil {
		return 0, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return 0, ErrInvalidTaskContext
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron == nil {
		return 0, ErrAdminBackendUnavailable
	}

	updated := 0

	for name, spec := range tm.cronSpecs {
		if spec.Paused == paused {
			continue
		}

		spec.Paused = paused
		tm.cronSpecs[name] = spec
		updated++
	}

	return updated, nil
}

// AdminRunSchedule triggers a cron schedule immediately.
func (tm *TaskManager) AdminRunSchedule(ctx context.Context, name string) (string, error) {
	if tm == nil {
		return "", ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return "", ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrAdminScheduleNameRequired
	}

	tm.cronMu.RLock()
	spec, specOk := tm.cronSpecs[name]
	factory, factoryOk := tm.cronFactories[name]
	tm.cronMu.RUnlock()

	if !specOk {
		return "", ErrAdminScheduleNotFound
	}

	if !factoryOk {
		return "", ErrAdminScheduleFactoryMissing
	}

	if factory.Durable != spec.Durable {
		return "", ErrAdminScheduleDurableMismatch
	}

	return tm.adminRunCron(name, spec, factory)
}

func (tm *TaskManager) adminRunCron(name string, spec cronSpec, factory cronFactory) (string, error) {
	if factory.Durable {
		return tm.adminRunDurableCron(name, spec, factory)
	}

	return tm.adminRunInMemoryCron(name, spec, factory)
}

func (tm *TaskManager) adminRunDurableCron(name string, spec cronSpec, factory cronFactory) (string, error) {
	task, err := factory.DurableFactory(tm.ctx)
	if err != nil {
		return "", err
	}

	if task.ID == uuid.Nil {
		task.ID = uuid.New()
	}

	ensureCronMetadata(&task, name, spec, tm.defaultQueue)

	queueName := task.Queue
	if queueName == "" {
		queueName = tm.defaultQueue
	}

	metadata := copyStringMap(task.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}

	if task.Handler != "" {
		if _, ok := metadata["handler"]; !ok {
			metadata["handler"] = task.Handler
		}
	}

	runInfo := cronRunInfo{
		id:         task.ID,
		name:       name,
		spec:       spec.Spec,
		durable:    true,
		queue:      queueName,
		enqueuedAt: time.Now(),
		metadata:   metadata,
	}

	tm.noteCronRun(runInfo)

	err = tm.RegisterDurableTask(tm.ctx, task)
	if err != nil {
		tm.dropCronRun(task.ID)

		return "", err
	}

	return task.ID.String(), nil
}

func (tm *TaskManager) adminRunInMemoryCron(name string, spec cronSpec, factory cronFactory) (string, error) {
	task, err := factory.TaskFactory(tm.ctx)
	if err != nil {
		return "", err
	}

	if task == nil {
		return "", ewrap.New("cron task is nil")
	}

	if task.ID == uuid.Nil {
		task.ID = uuid.New()
	}

	runInfo := cronRunInfoFromTask(name, spec, task, tm.defaultQueue)
	tm.noteCronRun(runInfo)

	err = tm.RegisterTask(tm.ctx, task)
	if err != nil {
		tm.dropCronRun(task.ID)

		return "", err
	}

	return task.ID.String(), nil
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

// AdminJobs lists registered job definitions.
func (tm *TaskManager) AdminJobs(ctx context.Context) ([]AdminJob, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return nil, err
	}

	return backend.AdminJobs(ctx)
}

// AdminJob returns a job definition by name.
func (tm *TaskManager) AdminJob(ctx context.Context, name string) (AdminJob, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminJob{}, err
	}

	return backend.AdminJob(ctx, name)
}

// AdminUpsertJob creates or updates a job definition and registers a cron factory.
func (tm *TaskManager) AdminUpsertJob(ctx context.Context, spec AdminJobSpec) (AdminJob, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminJob{}, err
	}

	job, err := backend.AdminUpsertJob(ctx, spec)
	if err != nil {
		return AdminJob{}, err
	}

	err = tm.registerJobFactory(job)
	if err != nil {
		return AdminJob{}, err
	}

	return job, nil
}

// AdminDeleteJob removes a job definition and its cron factory.
func (tm *TaskManager) AdminDeleteJob(ctx context.Context, name string) (bool, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return false, err
	}

	deleted, err := backend.AdminDeleteJob(ctx, name)
	if err != nil {
		return false, err
	}

	if deleted {
		tm.unregisterJobFactory(name)
	}

	return deleted, nil
}

// AdminRunJob enqueues a job immediately as a durable task.
func (tm *TaskManager) AdminRunJob(ctx context.Context, name string) (string, error) {
	if tm == nil {
		return "", ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return "", ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrAdminJobNameRequired
	}

	backend, err := tm.adminBackend()
	if err != nil {
		return "", err
	}

	job, err := backend.AdminJob(ctx, name)
	if err != nil {
		return "", err
	}

	task, err := jobDurableTask(job)
	if err != nil {
		return "", err
	}

	err = tm.RegisterDurableTask(ctx, task)
	if err != nil {
		return "", err
	}

	return task.ID.String(), nil
}

func (tm *TaskManager) registerJobFactory(job AdminJob) error {
	if tm == nil {
		return ErrAdminBackendUnavailable
	}

	name := strings.TrimSpace(job.Name)
	if name == "" {
		return ErrAdminJobNameRequired
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cronFactories == nil {
		return ErrAdminBackendUnavailable
	}

	if existing, ok := tm.cronFactories[name]; ok && existing.Origin != cronFactoryOriginJob {
		return ewrap.New("job factory name conflicts with existing schedule factory")
	}

	tm.cronFactories[name] = cronFactory{
		Durable: true,
		DurableFactory: func(_ context.Context) (DurableTask, error) {
			return jobDurableTask(job)
		},
		Origin: cronFactoryOriginJob,
	}

	return nil
}

func (tm *TaskManager) unregisterJobFactory(name string) {
	if tm == nil {
		return
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if factory, ok := tm.cronFactories[name]; ok && factory.Origin == cronFactoryOriginJob {
		delete(tm.cronFactories, name)
	}

	if entryID, ok := tm.cronEntries[name]; ok {
		if tm.cron != nil {
			tm.cron.Remove(entryID)
		}

		delete(tm.cronEntries, name)
	}

	delete(tm.cronSpecs, name)
}

// SyncJobFactories loads persisted jobs and registers factories for them.
func (tm *TaskManager) SyncJobFactories(ctx context.Context) error {
	if tm == nil {
		return ErrAdminBackendUnavailable
	}

	backend, err := tm.adminBackend()
	if err != nil {
		return err
	}

	jobs, err := backend.AdminJobs(ctx)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		err = tm.registerJobFactory(job)
		if err != nil {
			return err
		}
	}

	return nil
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

// AdminDLQEntry returns a detailed DLQ entry by ID.
func (tm *TaskManager) AdminDLQEntry(ctx context.Context, id string) (AdminDLQEntryDetail, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		return AdminDLQEntryDetail{}, err
	}

	return backend.AdminDLQEntry(ctx, id)
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
