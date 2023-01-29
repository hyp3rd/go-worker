# go-worker

[![Go](https://github.com/hyp3rd/go-worker/actions/workflows/go.yml/badge.svg)](https://github.com/hyp3rd/go-worker/actions/workflows/go.yml) [![CodeQL](https://github.com/hyp3rd/go-worker/actions/workflows/codeql.yml/badge.svg)](https://github.com/hyp3rd/go-worker/actions/workflows/codeql.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/hyp3rd/go-worker)](https://goreportcard.com/report/github.com/hyp3rd/go-worker) [![Go Reference](https://pkg.go.dev/badge/github.com/hyp3rd/go-worker.svg)](https://pkg.go.dev/github.com/hyp3rd/go-worker) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`go-worker` provides a simple way to manage and execute tasks concurrently and prioritized, leveraging a `TaskManager` that spawns a pool of `workers`.
Each `Task` represents a function scheduled by priority.

## Features

- Task prioritization: You can register tasks with a priority level influencing the execution order.
- Concurrent execution: Tasks are executed concurrently by a pool of workers.
- Rate limiting: You can rate limit the tasks schedule by setting a maximum number of jobs per second.
- Cancellation: Tasks can be canceled before or while they are running.

## API

### Initialization

Create a new `TaskManager` by calling the `NewTaskManager()` function and passing in the maximum number of tasks and the rate limit as parameters.

```go
tm := worker.NewTaskManager(10, 5)
```

### Registering Tasks

Register new tasks by calling the `RegisterTask()` method of the `TaskManager` struct and passing in the tasks.

```go
task := worker.Task{
    ID:       uuid.New(),
    Priority: 1,
    Fn:       func() interface{} { return "Hello, World!" },
}


task2 := worker.Task{
    ID:       uuid.New(),
    Priority: 10,
    Fn:       func() interface{} { return "Hello, World!" },
}

tm.RegisterTask(task, task2)
```

### Starting and Stopping

You can start the task manager and its goroutines by calling the `Start()` method of the `TaskManager` struct and passing in the number of workers.

```go
tm.Start(5)
```

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
    "fmt"
    "log"
    "time"

    worker "github.com/hyp3rd/go-worker"
    "github.com/hyp3rd/go-worker/middleware"
    "github.com/google/uuid"
)

func main() {
    tm := worker.NewTaskManager(5, 10)
    // Example of using zap logger from uber
    logger := log.Default()

    // apply middleware in the same order as you want to execute them
    tm = worker.RegisterMiddleware(tm,
        //middleware.YourMiddleware,
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
        // You can pass in parameters to the function and return a value using a closure
        Fn: func() interface{} {
            return func(a int, b int) interface{} {
                return a + b
            }(2, 5)
        },
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

```

## Conclusion

The worker package provides an efficient way to manage and execute tasks concurrently and with prioritization. The package is highly configurable and can be used in various scenarios.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
