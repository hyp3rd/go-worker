package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	worker "github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

const (
	maxWorkers     = 4
	maxTasks       = 10
	tasksPerSecond = 5
	interval       = time.Second * 3
	maxRetries     = 3
)

//nolint:mnd
func main() {
	tm := worker.NewTaskManager(context.TODO(), maxWorkers, maxTasks, tasksPerSecond, interval, interval, maxRetries)

	// defer tm.Stop()

	var srv worker.Service = tm
	// apply middleware in the same order as you want to execute them
	srv = worker.RegisterMiddleware(srv,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, middleware.DefaultLogger())
		},
	)

	// defer srv.Stop()

	task := &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Execute: func() (val any, err error) {
			return func(a int, b int) (val any, err error) {
				return a + b, err
			}(2, 5)
		},
	}

	// Invalid task, it doesn't have a function
	task1 := &worker.Task{
		ID:       uuid.New(),
		Priority: 10,
		// Execute:       func() (val any, err error) { return "Hello, World from Task 1!", err },
	}

	task2 := &worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Execute: func() (val any, err error) {
			time.Sleep(time.Second * 2)

			return "Hello, World from Task 2!", err
		},
		Ctx: context.TODO(),
	}

	task3 := &worker.Task{
		ID:       uuid.New(),
		Priority: 90,
		Execute: func() (val any, err error) {
			// Simulate a long running task
			time.Sleep(3 * time.Second)

			return "Hello, World from Task 3!", err
		},
	}

	task4 := &worker.Task{
		ID:       uuid.New(),
		Priority: 150,
		Execute: func() (val any, err error) {
			// Simulate a long running task
			time.Sleep(1 * time.Second)

			return "Hello, World from Task 4!", err
		},
	}

	srv.RegisterTasks(context.TODO(), task, task1, task2, task3)
	srv.CancelTask(task3.ID)

	err := srv.RegisterTask(context.TODO(), task4)
	if err != nil {
		log.Println("unable to register task", err)
	}

	tasks := srv.GetTasks()
	for _, task := range tasks {
		fmt.Fprintln(os.Stdout, task.Result)
	}

	fmt.Fprintln(os.Stdout, "printing cancelled tasks")

	// get the cancelled tasks
	cancelledTasks := tm.GetCancelledTasks()

	select {
	case task := <-cancelledTasks:
		fmt.Fprintf(os.Stdout, "Task %s was cancelled\n", task.ID.String())
	default:
		fmt.Fprintln(os.Stdout, "No tasks have been cancelled yet")
	}
}
