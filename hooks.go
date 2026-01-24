package worker

import (
	"log/slog"
	"time"
)

// TaskHooks defines optional callbacks for task lifecycle events.
type TaskHooks struct {
	OnQueued func(task *Task)
	OnStart  func(task *Task)
	OnFinish func(task *Task, status TaskStatus, result any, err error)
	OnRetry  func(task *Task, delay time.Duration, attempt int)
}

// SetHooks configures callbacks for task lifecycle events.
func (tm *TaskManager) SetHooks(hooks TaskHooks) {
	h := hooks
	tm.hooks.Store(&h)
}

func (tm *TaskManager) hookQueued(task *Task) {
	hooks := tm.hooks.Load()
	if hooks == nil || hooks.OnQueued == nil {
		return
	}

	runHook(func() { hooks.OnQueued(task) })
}

func (tm *TaskManager) hookStart(task *Task) {
	hooks := tm.hooks.Load()
	if hooks == nil || hooks.OnStart == nil {
		return
	}

	runHook(func() { hooks.OnStart(task) })
}

func (tm *TaskManager) hookFinish(task *Task, status TaskStatus, result any, err error) {
	hooks := tm.hooks.Load()
	if hooks == nil || hooks.OnFinish == nil {
		return
	}

	runHook(func() { hooks.OnFinish(task, status, result, err) })
}

func (tm *TaskManager) hookRetry(task *Task, delay time.Duration, attempt int) {
	hooks := tm.hooks.Load()
	if hooks == nil || hooks.OnRetry == nil {
		return
	}

	runHook(func() { hooks.OnRetry(task, delay, attempt) })
}

func runHook(fn func()) {
	defer func() {
		err := recover()
		if err != nil {
			logger := slog.Default()
			// Log the panic from the hook to avoid crashing the task manager.
			logger.Error("task hook panic", "error", err)
		}
	}()

	fn()
}
