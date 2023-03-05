package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
)

func main() {
	// create a new task manager with a rate limit of 1 task per second
	tm := worker.NewTaskManager(10, 5, time.Second*30, time.Second*30, 3)
	// close the task manager
	defer tm.Close()
	// start the task manager
	tm.Start(10)

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			j := i
			// create a new task
			id := uuid.New()
			task := worker.Task{
				ID: id,
				Fn: func() (val interface{}, err error) {
					emptyFile, error := os.Create(fmt.Sprintf("EmptyFile_%v.txt", j))
					if error != nil {
						log.Fatal(error)
					}
					log.Println(emptyFile)
					emptyFile.Close()
					time.Sleep(time.Second)
					return fmt.Sprintf("task number %v with id %s executed", j, id), err
				},
			}

			// register the task
			tm.RegisterTask(context.Background(), task)
		}
	}()

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			j := i
			// create a new task
			id := uuid.New()
			task := worker.Task{
				ID: id,
				Fn: func() (val interface{}, err error) {
					emptyFile, error := os.Create(fmt.Sprintf("__EmptyFile___%v.txt", j))
					if error != nil {
						log.Fatal(error)
					}
					log.Println(emptyFile)
					emptyFile.Close()
					time.Sleep(time.Second)
					return fmt.Sprintf("task number %v with id %s executed", j, id), err
				},
			}

			// register the task
			tm.RegisterTask(context.Background(), task)
		}
	}()

	// wait for the tasks to finish and print the results
	for result := range tm.GetResults() {
		fmt.Println(result)
	}

}
