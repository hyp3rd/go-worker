# go-worker

[![Go](https://github.com/hyp3rd/go-worker/actions/workflows/go.yml/badge.svg)](https://github.com/hyp3rd/go-worker/actions/workflows/go.yml) [![CodeQL](https://github.com/hyp3rd/go-worker/actions/workflows/codeql.yml/badge.svg)](https://github.com/hyp3rd/go-worker/actions/workflows/codeql.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/hyp3rd/go-worker)](https://goreportcard.com/report/github.com/hyp3rd/go-worker) [![Go Reference](https://pkg.go.dev/badge/github.com/hyp3rd/go-worker.svg)](https://pkg.go.dev/github.com/hyp3rd/go-worker) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`go-worker` provides a simple way to manage and execute tasks concurrently and prioritized, leveraging a `TaskManager` that spawns a pool of `workers`.
Each `Task` represents a function scheduled by priority.

## Features

- Task prioritization: You can register tasks with a priority level influencing the execution order.
- Concurrent execution: Tasks are executed concurrently by a pool of workers.
- Middleware: You can apply middleware to the `TaskManager` to add additional functionality.
- Results: You can access the results of the tasks via the `Results` channel.
- Rate limiting: You can rate limit the tasks schedule by setting a maximum number of jobs per second.
- Cancellation: You can cancel Tasks before or while they are running.

## API

### Initialization

Create a new `TaskManager` by calling the `NewTaskManager()` function with the following parameters:

- `maxWorkers` is the number of workers to start. If 0 is specified, it will default to the number of available CPUs
- `maxTasks` is the maximum number of tasks that can be executed at once, defaults to 10
- `tasksPerSecond` is the rate limit of tasks that can be executed per second, defaults to 1
- `timeout` is the default timeout for tasks, defaults to 5 minute
- `retryDelay` is the default delay between retries, defaults to 1 second
- `maxRetries` is the default maximum number of retries, defaults to 3

```go
tm := worker.NewTaskManager(4, 10, 5, time.Second*30, time.Second*30, 3)
```

### Registering Tasks

Register new tasks by calling the `RegisterTasks()` method of the `TaskManager` struct and passing in a variadic number of tasks.

```go
id := uuid.New()
task := worker.Task{
    ID:          id,
    Name:        "Some task",
    Description: "Here goes the description of the task",
    Priority:    10,
    Fn: func() (any, error) {
        emptyFile, err := os.Create(path.Join("examples", "test", "res", fmt.Sprintf("1st__EmptyFile___%v.txt", j)))
        if err != nil {
            log.Fatal(err)
        }
        emptyFile.Close()
        time.Sleep(time.Second)
        return fmt.Sprintf("** task number %v with id %s executed", j, id), err
    },
    Retries:    10,
    RetryDelay: 3,
}

task2 := worker.Task{
    ID:       uuid.New(),
    Priority: 10,
    Fn:       func() (val any, err error){ return "Hello, World!", err },
}

tm.RegisterTasks(context.Background(), task, task2)
```

### Stopping the Task Manager

You can stop the task manager and its goroutines by calling the Stop() method of the TaskManager struct.

```go
tm.Stop()
```

### Results

The results of the tasks can be accessed via the Results channel of the `TaskManager`, calling the `GetResults()` method.

```go
for result := range tm.GetResults() {
   // Do something with the result
}

```

### Cancellation

You can cancel a `Task` by calling the `CancelTask()` method of the `TaskManager` struct and passing in the task ID as a parameter.

```go
tm.CancelTask(task.ID)
```

You can cancel all tasks by calling the `CancelAllTasks()` method of the `TaskManager` struct.

```go
tm.CancelAllTasks()
```

### Middleware

You can apply middleware to the `TaskManager` by calling the `RegisterMiddleware()` function and passing in the `TaskManager` and the middleware functions.

```go
tm = worker.RegisterMiddleware(tm,
    //middleware.YourMiddleware,
    func(next worker.Service) worker.Service {
        return middleware.NewLoggerMiddleware(next, logger)
    },
)
```

### Example

```go
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
    tm := worker.NewTaskManager(4, 10, 5, time.Second*3, time.Second*30, 3)

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
        Fn: func() (val any, err error) {
            return func(a int, b int) (val any, err error) {
                return a + b, err
            }(2, 5)
        },
    }

    // Invalid task, it doesn't have a function
    task1 := worker.Task{
        ID:       uuid.New(),
        Priority: 10,
        // Fn:       func() (val any, err error) { return "Hello, World from Task 1!", err },
    }

    task2 := worker.Task{
        ID:       uuid.New(),
        Priority: 5,
        Fn: func() (val any, err error) {
            time.Sleep(time.Second * 2)
            return "Hello, World from Task 2!", err
        },
    }

    task3 := worker.Task{
        ID:       uuid.New(),
        Priority: 90,
        Fn: func() (val any, err error) {
            // Simulate a long running task
            // time.Sleep(3 * time.Second)
            return "Hello, World from Task 3!", err
        },
    }

    task4 := worker.Task{
        ID:       uuid.New(),
        Priority: 150,
        Fn: func() (val any, err error) {
            // Simulate a long running task
            time.Sleep(1 * time.Second)
            return "Hello, World from Task 4!", err
        },
    }

    srv.RegisterTasks(context.Background(), task, task1, task2, task3)

    srv.CancelTask(task3.ID)

    srv.RegisterTask(context.Background(), task4)

    // Print results
    for result := range srv.GetResults() {
        fmt.Println(result)
    }

    tasks := srv.GetTasks()
    for _, task := range tasks {
        fmt.Println(task)
    }
}
```

## Conclusion

The worker package provides an efficient way to manage and execute tasks concurrently and with prioritization. The package is highly configurable and can be used in various scenarios.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
