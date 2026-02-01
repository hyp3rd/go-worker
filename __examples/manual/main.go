package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

const (
	taskTimeout    = 5 * time.Second
	maxWorkers     = 4
	maxTasks       = 10
	tasksPerSecond = 5
	timeout        = 30 * time.Second
	retryDelay     = 30 * time.Second
	maxRetries     = 3
)

func main() {
	tm := worker.NewTaskManager(context.Background(), maxWorkers, maxTasks, tasksPerSecond, timeout, retryDelay, maxRetries)
	// Example of using stdlib logger
	logger := log.Default()

	// apply middleware in the same order as you want to execute them
	srv := worker.RegisterMiddleware[worker.Service](tm,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, logger)
		},
	)

	task := &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Ctx:      context.Background(),
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "Hello, World from Task!", err },
	}

	err := srv.RegisterTask(context.Background(), task)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}

	res, err := srv.ExecuteTask(context.Background(), task.ID, taskTimeout)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}

	fmt.Fprintln(os.Stdout, res)
}
