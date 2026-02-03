package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

const (
	redisEnvAddr     = "REDIS_ADDR"
	redisEnvPassword = "REDIS_PASSWORD"
	redisEnvPrefix   = "REDIS_PREFIX"
	redisTestTimeout = 5 * time.Second
	redisTestQueue   = "default"
)

func TestDurableEnqueueIntegration(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf("%s not set; skipping Redis integration test", redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)

	prefix := os.Getenv(redisEnvPrefix)
	if prefix == "" {
		prefix = "workerctl-test-" + uuid.NewString()
	}

	cfg := &redisConfig{
		addr:     addr,
		password: password,
		prefix:   prefix,
		timeout:  redisTestTimeout,
	}

	client, err := cfg.client()
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	cleanupRedisPrefix(ctx, t, client, keyPrefix(prefix))

	opts := enqueueOptions{
		handler:      "test_handler",
		queue:        redisTestQueue,
		payload:      `{"hello":"world"}`,
		apply:        true,
		defaultQueue: redisTestQueue,
	}

	err = runDurableEnqueue(cfg, opts)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	backend, err := newDurableBackend(client, prefix, redisTestQueue)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	leases, err := backend.Dequeue(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if len(leases) != 1 {
		t.Fatalf("expected one lease, got %d", len(leases))
	}

	if string(leases[0].Task.Payload) != opts.payload {
		t.Fatalf("unexpected payload: %s", string(leases[0].Task.Payload))
	}

	err = backend.Ack(ctx, leases[0])
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func cleanupRedisPrefix(ctx context.Context, t *testing.T, client rueidis.Client, prefix string) {
	t.Helper()

	const scanCount = 100

	cursor := uint64(0)

	for {
		resp := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(prefix+":*").Count(scanCount).Build())

		entry, err := resp.AsScanEntry()
		if err != nil {
			t.Fatalf("scan keys: %v", err)
		}

		if len(entry.Elements) > 0 {
			_, err = client.Do(ctx, client.B().Del().Key(entry.Elements...).Build()).ToInt64()
			if err != nil {
				t.Fatalf("del keys: %v", err)
			}
		}

		if entry.Cursor == 0 {
			break
		}

		cursor = entry.Cursor
	}
}
