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

	var srv worker.Service = tm
	// apply middleware in the same order as you want to execute them
	srv = worker.RegisterMiddleware(srv,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, middleware.DefaultLogger())
		},
	)

	task := &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Ctx:      context.Background(),
		Execute: func(ctx context.Context, args ...any) (val any, err error) {
			return func(a, b int) (val any, err error) {
				return a + b, err
			}(2, 5)
		},
	}

	// Invalid task, it doesn't have a function
	task1 := &worker.Task{
		ID:       uuid.New(),
		Priority: 10,
		Ctx:      context.Background(),
	}

	task2 := &worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Execute: func(ctx context.Context, args ...any) (val any, err error) {
			time.Sleep(time.Second * 2)

			return "Hello, World from Task 2!", err
		},
		Ctx: context.TODO(),
	}

	task3 := &worker.Task{
		ID:       uuid.New(),
		Priority: 90,
		Execute: func(ctx context.Context, args ...any) (val any, err error) {
			// Simulate a long running task
			time.Sleep(3 * time.Second)

			return "Hello, World from Task 3!", err
		},
		Ctx: context.Background(),
	}

	task4 := &worker.Task{
		ID:       uuid.New(),
		Priority: 150,
		Execute: func(ctx context.Context, args ...any) (val any, err error) {
			// Simulate a long running task
			time.Sleep(1 * time.Second)

			return "Hello, World from Task 4!", err
		},
		Ctx: context.Background(),
	}

	if err := srv.RegisterTasks(context.TODO(), task, task1, task2, task3); err != nil {
		log.Println("register tasks error", err)
	}

	_ = srv.CancelTask(task3.ID)

	if err := srv.RegisterTask(context.TODO(), task4); err != nil {
		log.Println("unable to register task", err)
	}

	tasks := srv.GetTasks()
	for _, task := range tasks {
		fmt.Fprintln(os.Stdout, task.Result())
	}

	fmt.Fprintln(os.Stdout, "printing cancelled tasks")

	results, cancel := srv.SubscribeResults(10)
	defer cancel()

	ctx, cancelWait := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelWait()
	_ = srv.Wait(ctx)

	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}
			if res.Task != nil && res.Task.Status() == worker.Cancelled {
				fmt.Fprintf(os.Stdout, "Task %s was cancelled\n", res.Task.ID.String())
			}
		default:
			fmt.Fprintln(os.Stdout, "No tasks have been cancelled yet")
			return
		}
	}
}
