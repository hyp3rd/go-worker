package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

func main() {
	tm := worker.NewTaskManager(4, 10, 5, time.Second*30, time.Second*30, 3)

	defer tm.Close()

	var srv worker.Service = tm
	// apply middleware in the same order as you want to execute them
	srv = worker.RegisterMiddleware(tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, middleware.DefaultLogger())
		},
	)

	defer srv.Close()

	task := worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Fn: func() (val interface{}, err error) {
			return func(a int, b int) (val interface{}, err error) {
				return a + b, err
			}(2, 5)
		},
	}

	// Invalid task, it doesn't have a function
	task1 := worker.Task{
		ID:       uuid.New(),
		Priority: 10,
		// Fn:       func() (val interface{}, err error) { return "Hello, World from Task 1!", err },
	}

	task2 := worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Fn: func() (val interface{}, err error) {
			time.Sleep(time.Second * 2)
			return "Hello, World from Task 2!", err
		},
	}

	task3 := worker.Task{
		ID:       uuid.New(),
		Priority: 90,
		Fn: func() (val interface{}, err error) {
			// Simulate a long running task
			// time.Sleep(3 * time.Second)
			return "Hello, World from Task 3!", err
		},
	}

	task4 := worker.Task{
		ID:       uuid.New(),
		Priority: 150,
		Fn: func() (val interface{}, err error) {
			// Simulate a long running task
			time.Sleep(1 * time.Second)
			return "Hello, World from Task 4!", err
		},
	}

	srv.RegisterTasks(context.Background(), task, task1, task2, task3, task4)
	// srv.RegisterTask(context.Background(), task)
	// srv.RegisterTask(context.Background(), task1)
	// srv.RegisterTask(context.Background(), task2)
	// srv.RegisterTask(context.Background(), task3)
	// srv.RegisterTask(context.Background(), task4)

	srv.CancelTask(task3.ID)

	// srv.Start(1)

	// Print results
	// for result := range srv.GetCancelled() {
	// 	fmt.Println(result)
	// }
	// // Print results
	for result := range srv.GetResultsChannel() {
		fmt.Println(result)
	}

	tasks := srv.GetTasks()
	for _, task := range tasks {
		fmt.Println(task)
	}
}
