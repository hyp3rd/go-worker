package main

import (
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

func main() {
	tm := worker.NewTaskManager(5, 10)
	// Example of using zap logger from uber
	logger := log.Default()

	// apply middleware in the same order as you want to execute them
	tm = worker.RegisterMiddleware(tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, logger)
		},
	)

	task := worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Fn:       func() interface{} { return "Hello, World from Task 1!" },
	}

	task2 := worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Fn:       func() interface{} { return "Hello, World from Task 2!" },
	}

	task3 := worker.Task{
		ID:       uuid.New(),
		Priority: 90,
		Fn: func() interface{} {
			// Simulate a long running task
			time.Sleep(3 * time.Second)
			return "Hello, World from Task 3!"
		},
	}

	task4 := worker.Task{
		ID:       uuid.New(),
		Priority: 15,
		Fn: func() interface{} {
			// Simulate a long running task
			time.Sleep(5 * time.Second)
			return "Hello, World from Task 4!"
		},
	}

	tm.RegisterTask(task, task2, task3, task4)
	tm.Start(5)

	tm.CancelTask(task3.ID)

	// Print results
	for result := range tm.GetResults() {
		fmt.Println(result)
	}

	tasks := tm.GetTasks()
	for _, task := range tasks {
		fmt.Println(task)
	}
}
