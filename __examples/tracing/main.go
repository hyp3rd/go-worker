package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/hyp3rd/go-worker"
)

const (
	maxWorkers     = 2
	maxTasks       = 10
	tasksPerSecond = 5
	taskTimeout    = 5 * time.Second
)

type logTracer struct {
	logger *slog.Logger
}

type logSpan struct {
	logger *slog.Logger
	taskID uuid.UUID
	name   string
	start  time.Time
}

func (t logTracer) Start(ctx context.Context, task *worker.Task) (context.Context, worker.TaskSpan) {
	span := logSpan{
		logger: t.logger,
		taskID: task.ID,
		name:   task.Name,
		start:  time.Now(),
	}

	span.logger.Info("task start", "id", span.taskID, "name", span.name)

	return ctx, span
}

func (s logSpan) End(err error) {
	duration := time.Since(s.start)
	if err != nil {
		s.logger.Error("task end", "id", s.taskID, "name", s.name, "duration", duration, "error", err)
		return
	}

	s.logger.Info("task end", "id", s.taskID, "name", s.name, "duration", duration)
}

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	tm := worker.NewTaskManager(ctx, maxWorkers, maxTasks, tasksPerSecond, taskTimeout, time.Second, 1)
	tm.SetTracer(logTracer{logger: logger})

	task := &worker.Task{
		ID:       uuid.New(),
		Name:     "trace-example",
		Priority: 1,
		Ctx:      context.Background(),
		Execute: func(_ context.Context, _ ...any) (any, error) {
			time.Sleep(200 * time.Millisecond)
			return "done", nil
		},
	}

	if err := tm.RegisterTask(ctx, task); err != nil {
		logger.Error("register task failed", "error", err)
		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	err := tm.Wait(waitCtx)
	if err != nil {
		logger.Error("wait failed", "error", err)
	}
}
