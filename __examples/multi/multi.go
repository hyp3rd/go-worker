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

//nolint:mnd,gocognit,funlen,cyclop
func main() {
	// create a new task manager
	tm := worker.NewTaskManager(context.TODO(), 1, 50, 10, time.Second*60, time.Second*2, 5)

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := range 10 {
			j := i
			// create a new task
			id := uuid.New()
			task := &worker.Task{
				ID:          id,
				Name:        fmt.Sprintf("Some task %d", j),
				Description: fmt.Sprintf("Here goes the description of the task for task %d", j),
				Priority:    10 + i,
				Execute: func(ctx context.Context, args ...any) (val any, err error) {
					emptyFile, err := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("1st__EmptyFile___%v.txt", j)))
					if err != nil {
						log.Fatal(err)
					}

					fileCloseErr := emptyFile.Close()
					if fileCloseErr != nil {
						log.Println(fileCloseErr)
					}

					return fmt.Sprintf("** task number %v with id %s executed", j, id), err
				},
				Ctx:        context.TODO(),
				Retries:    5,
				RetryDelay: 1,
			}

			// register the task
			err := tm.RegisterTask(context.TODO(), task)
			if err != nil {
				log.Println("unable to register task", err)
			}
		}
	}()

	// register and execute 10 tasks in a separate goroutine
	go func() {
		for i := range 10 {
			j := i
			// create a new task
			id := uuid.New()
			task := &worker.Task{
				ID: id,
				Execute: func(ctx context.Context, args ...any) (val any, err error) {
					emptyFile, err := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("2nd__EmptyFile___%v.txt", j)))
					if err != nil {
						log.Fatal(err)
					}

					fileCloseErr := emptyFile.Close()
					if fileCloseErr != nil {
						log.Println(fileCloseErr)
					}
					// time.Sleep(time.Millisecond * 100)
					return fmt.Sprintf("**** task number %v with id %s executed", j, id), err
				},
				// Ctx:        context.TODO(),
				Retries:    5,
				RetryDelay: 1,
			}

			// register the task
			err := tm.RegisterTask(context.TODO(), task)
			if err != nil {
				log.Println("unable to register task", err)
			}
		}
	}()

	for i := range 10 {
		j := i
		// create a new task
		id := uuid.New()
		task := &worker.Task{
			ID: id,
			Execute: func(ctx context.Context, args ...any) (val any, err error) {
				emptyFile, err := os.Create(path.Join("examples", "multi", "res", fmt.Sprintf("3nd__EmptyFile___%v.txt", j)))
				if err != nil {
					log.Fatal(err)
				}

				fileCloseErr := emptyFile.Close()
				if fileCloseErr != nil {
					log.Println(fileCloseErr)
				}
				// time.Sleep(time.Millisecond * 100)
				return fmt.Sprintf("**** task number %v with id %s executed", j, id), err
			},
			Ctx:        context.TODO(),
			Retries:    5,
			RetryDelay: 1,
		}

		// register the task
		err := tm.RegisterTask(context.TODO(), task)
		if err != nil {
			log.Println("unable to register task", err)
		}
	}

	for i := range 10 {
		j := i
		// create a new task
		id := uuid.New()
		task := &worker.Task{
			ID: id,
			Execute: func(ctx context.Context, args ...any) (val any, err error) {
				emptyFile, err := os.Create(path.Join("examples", "wrong-path", "res", fmt.Sprintf("4nd__EmptyFile___%v.txt", j)))
				if err != nil {
					log.Println(err)
				}

				fileCloseErr := emptyFile.Close()
				if fileCloseErr != nil {
					log.Println(fileCloseErr)
				}
				// time.Sleep(time.Millisecond * 100)
				fmt.Fprintf(os.Stdout, "**** wrong task number %v with id %s executed", j, id)

				return fmt.Sprintf("**** wrong task number %v with id %s executed", j, id), err
			},
			Ctx:     context.TODO(),
			Retries: 3,
		}

		// register the task
		err := tm.RegisterTask(context.TODO(), task)
		if err != nil {
			log.Println("unable to register task", err)
		}
	}

	// wait for the tasks to finish and print the results
	for id, result := range tm.GetResults() {
		fmt.Fprintln(os.Stdout, id, result.String())
	}
}
