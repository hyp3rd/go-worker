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
	// Example of using zap logger from uber
	logger := log.Default()

	// apply middleware in the same order as you want to execute them
	srv := worker.RegisterMiddleware[worker.Service](tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, logger)
		},
	)

	task := &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Execute:  func(ctx context.Context, args ...any) (val any, err error) { return "Hello, World from Task!", err },
	}

	res, err := srv.ExecuteTask(context.Background(), task.ID, taskTimeout)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	} else {
		fmt.Fprint(os.Stdout, res)
	}

	err = srv.RegisterTask(context.Background(), task)
	if err != nil {
		fmt.Fprint(os.Stderr, err)

		return
	}

	res, err = srv.ExecuteTask(context.Background(), task.ID, taskTimeout)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	} else {
		fmt.Fprintln(os.Stdout, res)
	}
}
