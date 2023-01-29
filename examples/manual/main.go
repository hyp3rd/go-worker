package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

func main() {
	tm := worker.NewTaskManager(1, 10)
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
		Fn:       func() interface{} { return "Hello, World from Task!" },
	}

	// tm.RegisterTask(task)

	res, err := tm.ExecuteTask(task.ID)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}

	tm.RegisterTask(task)
	res, err = tm.ExecuteTask(task.ID)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}
}
