package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/hyp3rd/go-worker"
)

const (
	benchMaxWorkers          = 4
	benchMaxTasks            = 1000
	benchTasksPerSecond      = 1_000_000
	benchTimeout             = 5 * time.Second
	benchRetryDelay          = time.Second
	benchMaxRetries          = 0
	benchRetentionMaxEntries = 128
	benchBatchLarge          = 100
)

func benchBatchSizes() []int {
	return []int{1, 10, benchBatchLarge}
}

func BenchmarkTaskManager_RegisterWait(b *testing.B) {
	for _, size := range benchBatchSizes() {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			tm := worker.NewTaskManager(
				context.Background(),
				benchMaxWorkers,
				benchMaxTasks,
				benchTasksPerSecond,
				benchTimeout,
				benchRetryDelay,
				benchMaxRetries,
			)
			tm.SetRetentionPolicy(worker.RetentionPolicy{MaxEntries: benchRetentionMaxEntries})
			b.Cleanup(func() {
				tm.StopNow()
			})

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				tasks := make([]*worker.Task, 0, size)
				for range size {
					tasks = append(tasks, newBenchmarkTask())
				}

				err := tm.RegisterTasks(ctx, tasks...)
				if err != nil {
					b.Fatalf("RegisterTasks failed: %v", err)
				}

				waitCtx, cancel := context.WithTimeout(ctx, benchTimeout)

				err = tm.Wait(waitCtx)
				if err != nil {
					cancel()
					b.Fatalf("Wait failed: %v", err)
				}

				cancel()
			}
		})
	}
}

func BenchmarkTaskManager_RetentionMaxEntries(b *testing.B) {
	tm := worker.NewTaskManager(
		context.Background(),
		benchMaxWorkers,
		benchMaxTasks,
		benchTasksPerSecond,
		benchTimeout,
		benchRetryDelay,
		benchMaxRetries,
	)
	tm.SetRetentionPolicy(worker.RetentionPolicy{MaxEntries: benchRetentionMaxEntries})
	b.Cleanup(func() {
		tm.StopNow()
	})

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		taskA := newBenchmarkTask()
		taskB := newBenchmarkTask()

		err := tm.RegisterTasks(ctx, taskA, taskB)
		if err != nil {
			b.Fatalf("RegisterTasks failed: %v", err)
		}

		waitCtx, cancel := context.WithTimeout(ctx, benchTimeout)

		err = tm.Wait(waitCtx)
		if err != nil {
			cancel()
			b.Fatalf("Wait failed: %v", err)
		}

		cancel()
	}
}

func newBenchmarkTask() *worker.Task {
	return &worker.Task{
		ID:       uuid.New(),
		Priority: 1,
		Ctx:      context.Background(),
		Execute:  func(_ context.Context, _ ...any) (any, error) { return nil, nil },
	}
}
