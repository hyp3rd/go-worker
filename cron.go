package worker

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"github.com/robfig/cron/v3"
)

const errParseCronSchedule = "parse cron schedule"

// CronTaskFactory builds a task when a cron schedule fires.
type CronTaskFactory func(ctx context.Context) (*Task, error)

// CronDurableFactory builds a durable task when a cron schedule fires.
type CronDurableFactory func(ctx context.Context) (DurableTask, error)

type cronSpec struct {
	Spec    string
	Durable bool
	Paused  bool
}

type cronFactory struct {
	Durable        bool
	TaskFactory    CronTaskFactory
	DurableFactory CronDurableFactory
	Origin         string
}

const (
	cronFactoryOriginUser = "user"
	cronFactoryOriginJob  = "job"
)

// RegisterCronTask registers a cron job that enqueues a task on each tick.
func (tm *TaskManager) RegisterCronTask(
	ctx context.Context,
	name string,
	spec string,
	factory CronTaskFactory,
) error {
	if tm.durableEnabled {
		return ewrap.New("durable backend enabled; use RegisterDurableCronTask")
	}

	normalized, schedule, err := tm.prepareCronRegistration(ctx, name, spec, factory != nil)
	if err != nil {
		return err
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if _, exists := tm.cronEntries[normalized]; exists {
		return ewrap.New("cron task already registered")
	}

	tm.cronFactories[normalized] = cronFactory{
		Durable:     false,
		TaskFactory: factory,
		Origin:      cronFactoryOriginUser,
	}

	entryID := tm.cron.Schedule(schedule, cron.FuncJob(tm.cronJob(normalized)))

	tm.cronEntries[normalized] = entryID
	tm.cronSpecs[normalized] = cronSpec{Spec: strings.TrimSpace(spec), Durable: false}

	return nil
}

// RegisterDurableCronTask registers a cron job that enqueues a durable task on each tick.
func (tm *TaskManager) RegisterDurableCronTask(
	ctx context.Context,
	name string,
	spec string,
	factory CronDurableFactory,
) error {
	if !tm.durableEnabled {
		return ewrap.New("durable backend not configured")
	}

	normalized, schedule, err := tm.prepareCronRegistration(ctx, name, spec, factory != nil)
	if err != nil {
		return err
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if _, exists := tm.cronEntries[normalized]; exists {
		return ewrap.New("cron task already registered")
	}

	tm.cronFactories[normalized] = cronFactory{
		Durable:        true,
		DurableFactory: factory,
		Origin:         cronFactoryOriginUser,
	}

	entryID := tm.cron.Schedule(schedule, cron.FuncJob(tm.cronJob(normalized)))

	tm.cronEntries[normalized] = entryID
	tm.cronSpecs[normalized] = cronSpec{Spec: strings.TrimSpace(spec), Durable: true}

	return nil
}

func (tm *TaskManager) cronJob(name string) func() {
	return func() {
		if tm.skipCronTick() {
			return
		}

		spec, factory, ok := tm.cronSpecAndFactory(name)
		if !ok {
			return
		}

		if factory.Durable {
			tm.runDurableCron(name, spec, factory)

			return
		}

		tm.runInMemoryCron(name, spec, factory)
	}
}

func (tm *TaskManager) cronSpecAndFactory(name string) (cronSpec, cronFactory, bool) {
	tm.cronMu.RLock()
	spec, specOk := tm.cronSpecs[name]
	factory, factoryOk := tm.cronFactories[name]
	tm.cronMu.RUnlock()

	if !specOk || !factoryOk || spec.Paused {
		return cronSpec{}, cronFactory{}, false
	}

	return spec, factory, true
}

func (tm *TaskManager) runDurableCron(name string, spec cronSpec, factory cronFactory) {
	task, err := factory.DurableFactory(tm.ctx)
	if err != nil {
		cronLogError("cron durable task factory", name, err)

		return
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
		cronLogError("cron register durable task", name, err)
	}
}

func (tm *TaskManager) runInMemoryCron(name string, spec cronSpec, factory cronFactory) {
	task, err := factory.TaskFactory(tm.ctx)
	if err != nil {
		cronLogError("cron task factory", name, err)

		return
	}

	if task == nil {
		cronLogError("cron task factory", name, ewrap.New("cron task is nil"))

		return
	}

	if task.ID == uuid.Nil {
		task.ID = uuid.New()
	}

	runInfo := cronRunInfoFromTask(name, spec, task, tm.defaultQueue)
	tm.noteCronRun(runInfo)

	err = tm.RegisterTask(tm.ctx, task)
	if err != nil {
		tm.dropCronRun(task.ID)
		cronLogError("cron register task", name, err)
	}
}

func (tm *TaskManager) prepareCronRegistration(
	ctx context.Context,
	name string,
	spec string,
	hasFactory bool,
) (string, cron.Schedule, error) {
	if tm == nil {
		return "", nil, ewrap.New("task manager is nil")
	}

	if ctx == nil {
		return "", nil, ErrInvalidTaskContext
	}

	if !hasFactory {
		return "", nil, ewrap.New("cron task factory is nil")
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil, ewrap.New("cron task name is required")
	}

	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", nil, ewrap.New("cron schedule is required")
	}

	schedule, err := parseCronSpec(spec, tm.cronLoc)
	if err != nil {
		return "", nil, err
	}

	return name, schedule, nil
}

func (tm *TaskManager) skipCronTick() bool {
	return tm.ctx.Err() != nil || !tm.accepting.Load()
}

// UnregisterCronTask removes a cron job by name.
func (tm *TaskManager) UnregisterCronTask(name string) bool {
	if tm == nil {
		return false
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}

	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	entryID, ok := tm.cronEntries[name]
	if !ok {
		return false
	}

	tm.cron.Remove(entryID)
	delete(tm.cronEntries, name)
	delete(tm.cronSpecs, name)

	return true
}

func (tm *TaskManager) initCron() {
	location := tm.cronLoc
	if location == nil {
		location = time.UTC
		tm.cronLoc = location
	}

	parser := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
	tm.cron = cron.New(cron.WithLocation(location), cron.WithParser(parser))
}

func (tm *TaskManager) startCron() {
	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron != nil {
		tm.cron.Start()
	}
}

func (tm *TaskManager) stopCron() {
	tm.cronMu.Lock()
	defer tm.cronMu.Unlock()

	if tm.cron != nil {
		tm.cron.Stop()
	}
}

func parseCronSpec(spec string, location *time.Location) (cron.Schedule, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, ewrap.New("cron schedule is required")
	}

	fields := strings.Fields(spec)
	//nolint:revive,mnd
	switch len(fields) {
	case 5:
		schedule, err := cronParserStandard(location).Parse(spec)
		if err != nil {
			return nil, ewrap.Wrap(err, errParseCronSchedule)
		}

		return schedule, nil
	case 6:
		schedule, err := cronParserSeconds(location).Parse(spec)
		if err != nil {
			return nil, ewrap.Wrap(err, errParseCronSchedule)
		}

		return schedule, nil
	default:
		schedule, err := cronParserSeconds(location).Parse(spec)
		if err == nil {
			return schedule, nil
		}

		schedule, errStandard := cronParserStandard(location).Parse(spec)
		if errStandard == nil {
			return schedule, nil
		}

		return nil, ewrap.Wrap(err, errParseCronSchedule)
	}
}

func cronParserStandard(_ *time.Location) cron.Parser {
	return cron.NewParser(
		cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
}

func cronParserSeconds(_ *time.Location) cron.Parser {
	return cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
}

func cronLogError(action, name string, err error) {
	logger := slog.Default()
	logger.Error(action, "name", name, "error", err)
}
