package tests

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	worker "github.com/hyp3rd/go-worker"
)

const (
	redisEnvAddr                 = "REDIS_ADDR"
	redisEnvPassword             = "REDIS_PASSWORD"
	redisEnvPrefix               = "REDIS_PREFIX"
	redisScanCount               = 100
	redisLease                   = 2 * time.Second
	redisLeaseShort              = 200 * time.Millisecond
	redisLeaseExtend             = 600 * time.Millisecond
	redisLeaseGrace              = 150 * time.Millisecond
	redisNackDelay               = 50 * time.Millisecond
	redisRetryDelay              = 10 * time.Millisecond
	redisTestTimeout             = 5 * time.Second
	redisErrEnvNotSet            = "%s not set; skipping Redis integration test"
	redisErrEnqueue              = "enqueue: %v"
	redisErrDequeue              = "dequeue: %v"
	redisErrUnexpectedLeaseCount = "expected 1 lease, got %d"
	redisHandler                 = "test_handler"
	redisPayload                 = "payload"
)

func TestRedisDurableBackend_EnqueueDequeueAck(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	client := newRedisClient(t, addr, password)
	defer client.Close()

	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	taskID := uuid.New()
	task := worker.DurableTask{
		ID:         taskID,
		Handler:    redisHandler,
		Payload:    []byte(redisPayload),
		Priority:   1,
		Retries:    0,
		RetryDelay: redisRetryDelay,
		Metadata:   map[string]string{"env": "test"},
	}

	err := backend.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf(redisErrEnqueue, err)
	}

	leases, err := backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf(redisErrDequeue, err)
	}

	if len(leases) != 1 {
		t.Fatalf(redisErrUnexpectedLeaseCount, len(leases))
	}

	if leases[0].Task.ID != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, leases[0].Task.ID)
	}

	err = backend.Ack(ctx, leases[0])
	if err != nil {
		t.Fatalf("ack: %v", err)
	}

	leases, err = backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf("dequeue after ack: %v", err)
	}

	if len(leases) != 0 {
		t.Fatalf("expected no leases after ack, got %d", len(leases))
	}
}

func TestRedisDurableBackend_NackRequeues(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	client := newRedisClient(t, addr, password)
	defer client.Close()

	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	taskID := uuid.New()
	task := worker.DurableTask{
		ID:         taskID,
		Handler:    redisHandler,
		Payload:    []byte(redisPayload),
		Priority:   1,
		Retries:    1,
		RetryDelay: redisRetryDelay,
	}

	err := backend.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf(redisErrEnqueue, err)
	}

	leases, err := backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf(redisErrDequeue, err)
	}

	if len(leases) != 1 {
		t.Fatalf(redisErrUnexpectedLeaseCount, len(leases))
	}

	err = backend.Nack(ctx, leases[0], redisNackDelay)
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	leases, err = backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf("dequeue after nack: %v", err)
	}

	if len(leases) != 0 {
		t.Fatalf("expected no leases before delay, got %d", len(leases))
	}

	time.Sleep(redisNackDelay + 10*time.Millisecond)

	leases, err = backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf("dequeue after delay: %v", err)
	}

	if len(leases) != 1 {
		t.Fatalf("expected 1 lease after delay, got %d", len(leases))
	}
}

func TestRedisDurableBackend_LeaseExpiresAndRequeues(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	client := newRedisClient(t, addr, password)
	defer client.Close()

	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	taskID := uuid.New()
	task := worker.DurableTask{
		ID:         taskID,
		Handler:    redisHandler,
		Payload:    []byte(redisPayload),
		Priority:   1,
		Retries:    1,
		RetryDelay: redisRetryDelay,
	}

	err := backend.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf(redisErrEnqueue, err)
	}

	leases, err := backend.Dequeue(ctx, 1, redisLeaseShort)
	if err != nil {
		t.Fatalf(redisErrDequeue, err)
	}

	if len(leases) != 1 {
		t.Fatalf(redisErrUnexpectedLeaseCount, len(leases))
	}

	time.Sleep(redisLeaseShort + redisLeaseGrace)

	leases, err = backend.Dequeue(ctx, 1, redisLeaseShort)
	if err != nil {
		t.Fatalf("dequeue after lease expiry: %v", err)
	}

	if len(leases) != 1 {
		t.Fatalf("expected 1 lease after expiry, got %d", len(leases))
	}

	if leases[0].Task.ID != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, leases[0].Task.ID)
	}

	if leases[0].Attempts < 2 {
		t.Fatalf("expected attempts >= 2 after expiry, got %d", leases[0].Attempts)
	}
}

//nolint:revive,funlen
func TestRedisDurableBackend_ExtendPreventsRequeue(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	client := newRedisClient(t, addr, password)
	defer client.Close()

	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	taskID := uuid.New()
	task := worker.DurableTask{
		ID:         taskID,
		Handler:    redisHandler,
		Payload:    []byte(redisPayload),
		Priority:   1,
		Retries:    1,
		RetryDelay: redisRetryDelay,
	}

	err := backend.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf(redisErrEnqueue, err)
	}

	leases, err := backend.Dequeue(ctx, 1, redisLeaseShort)
	if err != nil {
		t.Fatalf(redisErrDequeue, err)
	}

	if len(leases) != 1 {
		t.Fatalf(redisErrUnexpectedLeaseCount, len(leases))
	}

	err = backend.Extend(ctx, leases[0], redisLeaseExtend)
	if err != nil {
		t.Fatalf("extend: %v", err)
	}

	time.Sleep(redisLeaseShort + redisLeaseGrace)

	leases, err = backend.Dequeue(ctx, 1, redisLeaseShort)
	if err != nil {
		t.Fatalf("dequeue after extend: %v", err)
	}

	if len(leases) != 0 {
		t.Fatalf("expected no leases before extended expiry, got %d", len(leases))
	}

	time.Sleep(redisLeaseExtend + redisLeaseGrace)

	leases, err = backend.Dequeue(ctx, 1, redisLeaseShort)
	if err != nil {
		t.Fatalf("dequeue after extended expiry: %v", err)
	}

	if len(leases) != 1 {
		t.Fatalf("expected 1 lease after extended expiry, got %d", len(leases))
	}

	if leases[0].Task.ID != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, leases[0].Task.ID)
	}

	if leases[0].Attempts < 2 {
		t.Fatalf("expected attempts >= 2 after extended expiry, got %d", leases[0].Attempts)
	}
}

func TestRedisDurableBackend_FailMovesToDLQ(t *testing.T) {
	t.Parallel()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	defer cancel()

	client := newRedisClient(t, addr, password)
	defer client.Close()

	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	taskID := uuid.New()
	task := worker.DurableTask{
		ID:         taskID,
		Handler:    redisHandler,
		Payload:    []byte(redisPayload),
		Priority:   1,
		Retries:    0,
		RetryDelay: redisRetryDelay,
	}

	err := backend.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf(redisErrEnqueue, err)
	}

	leases, err := backend.Dequeue(ctx, 1, redisLease)
	if err != nil {
		t.Fatalf(redisErrDequeue, err)
	}

	if len(leases) != 1 {
		t.Fatalf(redisErrUnexpectedLeaseCount, len(leases))
	}

	err = backend.Fail(ctx, leases[0], errTestBoom)
	if err != nil {
		t.Fatalf("fail: %v", err)
	}

	deadKey := redisKeyPrefix(prefix) + ":dead"

	deadCount, err := client.Do(ctx, client.B().Llen().Key(deadKey).Build()).AsInt64()
	if err != nil {
		t.Fatalf("dead count: %v", err)
	}

	if deadCount == 0 {
		t.Fatal("expected dead letter list to contain entries")
	}
}

func newRedisClient(t *testing.T, addr, password string) rueidis.Client {
	t.Helper()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{addr},
		Password:    password,
	})
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}

	return client
}

func newRedisBackend(t *testing.T, client rueidis.Client, prefix string) *worker.RedisDurableBackend {
	t.Helper()

	backend, err := worker.NewRedisDurableBackend(client, worker.WithRedisDurablePrefix(prefix))
	if err != nil {
		t.Fatalf("redis backend: %v", err)
	}

	return backend
}

func cleanupRedisPrefix(ctx context.Context, t *testing.T, client rueidis.Client, prefix string) {
	t.Helper()

	cursor := uint64(0)
	pattern := redisKeyPrefix(prefix) + ":*"

	for {
		resp := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(pattern).Count(redisScanCount).Build())

		entry, err := resp.AsScanEntry()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		if len(entry.Elements) > 0 {
			del := client.B().Del().Key(entry.Elements...).Build()

			err := client.Do(ctx, del).Error()
			if err != nil {
				t.Fatalf("del: %v", err)
			}
		}

		if entry.Cursor == 0 {
			break
		}

		cursor = entry.Cursor
	}
}

func redisKeyPrefix(prefix string) string {
	if prefix == "" {
		return "go-worker"
	}

	if strings.Contains(prefix, "{") {
		return prefix
	}

	return "{" + prefix + "}"
}

func redisTestPrefix(t *testing.T) string {
	t.Helper()

	base := os.Getenv(redisEnvPrefix)
	if base == "" {
		base = "go-worker-test"
	}

	return base + "-" + uuid.NewString()
}
