package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

func main() {
	tm := worker.NewTaskManager(5, 10)

	// apply middleware in the same order as you want to execute them
	tm = worker.RegisterMiddleware(tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, middleware.DefaultLogger())
		},
	)

	task := worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Fn: func() interface{} {
			return func(a int, b int) interface{} {
				return a + b
			}(2, 5)
		},
	}

	// Invalid task, it doesn't have a function
	task1 := worker.Task{
		ID:       uuid.New(),
		Priority: 10,
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
		Priority: 150,
		Fn: func() interface{} {
			// Simulate a long running task
			time.Sleep(5 * time.Second)
			return "Hello, World from Task 4!"
		},
	}

	task5 := worker.Task{
		ID:       uuid.New(),
		Priority: 50,
		Fn:       func() interface{} { return "Hello, World from Task 5!" },
	}

	task6 := worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Fn:       func() interface{} { return "Hello, World from Task 6!" },
	}

	task7 := worker.Task{
		ID:       uuid.New(),
		Priority: 8,
		Fn:       func() interface{} { return "Hello, World from Task 7!" },
	}

	task8 := worker.Task{
		ID:       uuid.New(),
		Priority: 9,
		Fn:       func() interface{} { return "Hello, World from Task 8!" },
	}

	task9 := worker.Task{
		ID:       uuid.New(),
		Priority: 105,
		Fn:       func() interface{} { return "Hello, World from Task 9!" },
	}

	task10 := worker.Task{
		ID:       uuid.New(),
		Priority: 15,
		Fn:       func() interface{} { return "Hello, World from Task 10!" },
	}

	tm.RegisterTask(task, task1, task2, task3, task4, task5, task6, task7, task8, task9, task10)
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
