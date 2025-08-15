package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	worker "github.com/hyp3rd/go-worker"
)

func main() {
	// create a new task manager
	tm := worker.NewTaskManager(context.TODO(), 1, 50, 10, time.Second*60, time.Second*2, 5)

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			j := i
			// create a new task
			id := uuid.New()
			task := &worker.Task{
				ID:          id,
				Name:        "Some task",
				Description: "Here goes the description of the task",
				Priority:    10,
				Execute: func() (val interface{}, err error) {
					emptyFile, error := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("1st__EmptyFile___%v.txt", j)))
					if error != nil {
						log.Fatal(error)
					}
					emptyFile.Close()
					return fmt.Sprintf("** task number %v with id %s executed", j, id), err
				},
				Ctx:        context.TODO(),
				Retries:    5,
				RetryDelay: 1,
			}

			// register the task
			tm.RegisterTask(context.TODO(), task)
		}
	}()

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := 0; i < 10; i++ {
			j := i
			// create a new task
			id := uuid.New()
			task := &worker.Task{
				ID: id,
				Execute: func() (val interface{}, err error) {
					emptyFile, error := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("2nd__EmptyFile___%v.txt", j)))

					if error != nil {
						log.Fatal(error)
					}
					emptyFile.Close()
					// time.Sleep(time.Millisecond * 100)
					return fmt.Sprintf("**** task number %v with id %s executed", j, id), err
				},
				// Ctx:        context.TODO(),
				Retries:    5,
				RetryDelay: 1,
			}

			// register the task
			tm.RegisterTask(context.TODO(), task)
		}
	}()

	for i := 0; i < 10; i++ {
		j := i
		// create a new task
		id := uuid.New()
		task := &worker.Task{
			ID: id,
			Execute: func() (val interface{}, err error) {
				emptyFile, error := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("3nd__EmptyFile___%v.txt", j)))

				if error != nil {
					log.Fatal(error)
				}
				emptyFile.Close()
				// time.Sleep(time.Millisecond * 100)
				return fmt.Sprintf("**** task number %v with id %s executed", j, id), err
			},
			Ctx:        context.TODO(),
			Retries:    5,
			RetryDelay: 1,
		}

		// register the task
		tm.RegisterTask(context.TODO(), task)
	}

	for i := 0; i < 10; i++ {
		j := i
		// create a new task
		id := uuid.New()
		task := &worker.Task{
			ID: id,
			Execute: func() (val interface{}, err error) {
				emptyFile, err := os.Create(path.Join("examples", "wrong-path", "res", fmt.Sprintf("4nd__EmptyFile___%v.txt", j)))

				if err != nil {
					log.Println(err)
				}
				emptyFile.Close()
				// time.Sleep(time.Millisecond * 100)
				return fmt.Sprintf("**** wrong task number %v with id %s executed", j, id), err
			},
			Ctx:     context.TODO(),
			Retries: 3,
		}

		// register the task
		tm.RegisterTask(context.TODO(), task)
	}

	// wait for the tasks to finish and print the results
	for id, result := range tm.GetResults() {
		fmt.Println(id, result.String())
	}
}
