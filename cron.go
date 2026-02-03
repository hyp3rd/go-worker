package worker

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/robfig/cron/v3"
)

const errParseCronSchedule = "parse cron schedule"

// CronTaskFactory builds a task when a cron schedule fires.
type CronTaskFactory func(ctx context.Context) (*Task, error)

// CronDurableFactory builds a durable task when a cron schedule fires.
type CronDurableFactory func(ctx context.Context) (DurableTask, error)

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

	entryID := tm.cron.Schedule(schedule, cron.FuncJob(func() {
		if tm.skipCronTick() {
			return
		}

		task, err := factory(tm.ctx)
		if err != nil {
			cronLogError("cron task factory", normalized, err)

			return
		}

		if task == nil {
			cronLogError("cron task factory", normalized, ewrap.New("cron task is nil"))

			return
		}

		err = tm.RegisterTask(tm.ctx, task)
		if err != nil {
			cronLogError("cron register task", normalized, err)
		}
	}))

	tm.cronEntries[normalized] = entryID

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

	entryID := tm.cron.Schedule(schedule, cron.FuncJob(func() {
		if tm.skipCronTick() {
			return
		}

		task, err := factory(tm.ctx)
		if err != nil {
			cronLogError("cron durable task factory", normalized, err)

			return
		}

		err = tm.RegisterDurableTask(tm.ctx, task)
		if err != nil {
			cronLogError("cron register durable task", normalized, err)
		}
	}))

	tm.cronEntries[normalized] = entryID

	return nil
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
