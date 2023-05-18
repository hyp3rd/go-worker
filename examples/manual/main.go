package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/go-worker"
	"github.com/hyp3rd/go-worker/middleware"
)

func main() {
	tm := worker.NewTaskManager(context.TODO(), 4, 10, 5, time.Second*30, time.Second*30, 3)
	// Example of using zap logger from uber
	logger := log.Default()

	// apply middleware in the same order as you want to execute them
	srv := worker.RegisterMiddleware(tm,
		// middleware.YourMiddleware,
		func(next worker.Service) worker.Service {
			return middleware.NewLoggerMiddleware(next, logger)
		},
	)

	task := worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Fn:       func() (val interface{}, err error) { return "Hello, World from Task!", err },
	}

	res, err := srv.ExecuteTask(task.ID, time.Second*5)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}

	srv.RegisterTask(context.TODO(), task)
	res, err = srv.ExecuteTask(task.ID, time.Second*5)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res)
	}
}
