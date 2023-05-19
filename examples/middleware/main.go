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
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*3, time.Second*3, 3)

	// defer tm.Stop()

	var srv worker.Service = tm
	// apply middleware in the same order as you want to execute them
	srv = worker.RegisterMiddleware(tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, middleware.DefaultLogger())
		},
	)

	// defer srv.Stop()

	task := worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Execute: func() (val interface{}, err error) {
			return func(a int, b int) (val interface{}, err error) {
				return a + b, err
			}(2, 5)
		},
	}

	// Invalid task, it doesn't have a function
	task1 := worker.Task{
		ID:       uuid.New(),
		Priority: 10,
		// Execute:       func() (val interface{}, err error) { return "Hello, World from Task 1!", err },
	}

	task2 := worker.Task{
		ID:       uuid.New(),
		Priority: 5,
		Execute: func() (val interface{}, err error) {
			time.Sleep(time.Second * 2)
			return "Hello, World from Task 2!", err
		},
		Ctx: context.TODO(),
	}

	task3 := worker.Task{
		ID:       uuid.New(),
		Priority: 90,
		Execute: func() (val interface{}, err error) {
			// Simulate a long running task
			// time.Sleep(3 * time.Second)
			return "Hello, World from Task 3!", err
		},
	}

	task4 := worker.Task{
		ID:       uuid.New(),
		Priority: 150,
		Execute: func() (val interface{}, err error) {
			// Simulate a long running task
			time.Sleep(1 * time.Second)
			return "Hello, World from Task 4!", err
		},
	}

	srv.RegisterTasks(context.TODO(), task, task1, task2, task3)

	srv.CancelTask(task3.ID)

	srv.RegisterTask(context.TODO(), task4)

	// Print results
	// for result := range srv.GetResults() {
	// 	fmt.Println(result)
	// }
	// tm.Wait(tm.Timeout)

	tasks := srv.GetTasks()
	for _, task := range tasks {
		fmt.Print(task.ID, " ", task.Priority, " ", task.Status, " ", task.Error, " ", "\n")
	}

	fmt.Println("printing cancelled tasks")

	// get the cancelled tasks
	cancelledTasks := tm.GetCancelledTasks()

	select {
	case task := <-cancelledTasks:
		fmt.Printf("Task %s was cancelled\n", task.ID.String())
	default:
		fmt.Println("No tasks have been cancelled yet")
	}

}
