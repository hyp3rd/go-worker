package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"google.golang.org/protobuf/proto"

	"github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func main() {
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		redisAddr       = "localhost:6380"
		redisPrefix     = "go-worker"
		redisPassword   = "supersecret"
		resultTimeout   = 25 * time.Second
		shutdownTimeout = 5 * time.Second
		durableRetries  = 3
		durablePriority = 1
		scanCount       = 100
	)
	failOnce := flag.Bool("fail-once", false, "fail the task once to populate DLQ")
	cleanup := flag.Bool("cleanup", true, "delete durable Redis keys for the example prefix")
	flag.Parse()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{redisAddr},
		Password:    redisPassword,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	backend, err := worker.NewRedisDurableBackend(client, worker.WithRedisDurablePrefix(redisPrefix))
	if err != nil {
		log.Fatal(err)
	}

	handlers := map[string]worker.DurableHandlerSpec{
		"send_email": {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(ctx context.Context, payload proto.Message) (any, error) {
				_ = payload.(*workerpb.SendEmailPayload)
				log.Printf("send_email handler invoked")
				if *failOnce {
					*failOnce = false
					return nil, ewrap.New("forced failure")
				}
				return "ok", nil
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		baseCtx,
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
	)

	tm.SetHooks(worker.TaskHooks{
		OnStart: func(task *worker.Task) {
			log.Printf("task %s started", task.ID)
		},
		OnFinish: func(task *worker.Task, status worker.TaskStatus, _ any, err error) {
			if err != nil {
				log.Printf("task %s finished status=%s err=%v", task.ID, status, err)
				return
			}
			log.Printf("task %s finished status=%s", task.ID, status)
		},
	})

	if *cleanup {
		err := cleanupPrefix(baseCtx, client, redisPrefix, scanCount)
		if err != nil {
			log.Fatalf("cleanup failed: %v", err)
		}
	}

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	err = tm.RegisterDurableTask(baseCtx, worker.DurableTask{
		Handler: "send_email",
		Message: &workerpb.SendEmailPayload{
			To:      "ops@example.com",
			Subject: "Hello from durable queue",
			Body:    "This task was persisted in Redis.",
		},
		Priority: durablePriority,
		Retries:  durableRetries,
	})
	if err != nil {
		log.Fatal(err)
	}

	select {
	case res := <-results:
		if res.Error != nil {
			log.Fatalf("task failed: %v", res.Error)
		}
		log.Printf("task completed with status=%s", res.Task.Status())
	case <-time.After(resultTimeout):
		log.Fatal("timed out waiting for result")
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	err = tm.StopGraceful(shutdownCtx)
	if err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func cleanupPrefix(ctx context.Context, client rueidis.Client, prefix string, count int64) error {
	if count <= 0 {
		count = 100
	}

	cursor := uint64(0)
	pattern := prefix + ":*"

	for {
		resp := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(pattern).Count(count).Build())
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}

		if len(entry.Elements) > 0 {
			del := client.B().Del().Key(entry.Elements...).Build()
			err := client.Do(ctx, del).Error()
			if err != nil {
				return err
			}
		}

		if entry.Cursor == 0 {
			break
		}

		cursor = entry.Cursor
	}

	return nil
}
