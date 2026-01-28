package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	testPollInterval = 5 * time.Millisecond
	testLease        = 2 * time.Second
	testTaskName     = "send_email"
)

var errTestBoom = errors.New("boom")

type fakeDurableBackend struct {
	mu        sync.Mutex
	leases    []worker.DurableTaskLease
	enqueued  []worker.DurableTask
	acked     []uuid.UUID
	nacked    []uuid.UUID
	failed    []uuid.UUID
	nackDelay time.Duration
}

func (b *fakeDurableBackend) Enqueue(_ context.Context, task worker.DurableTask) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.enqueued = append(b.enqueued, task)

	return nil
}

func (b *fakeDurableBackend) Dequeue(_ context.Context, limit int, _ time.Duration) ([]worker.DurableTaskLease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if limit <= 0 {
		limit = 1
	}

	if len(b.leases) == 0 {
		return nil, nil
	}

	if limit > len(b.leases) {
		limit = len(b.leases)
	}

	out := make([]worker.DurableTaskLease, 0, limit)
	out = append(out, b.leases[:limit]...)
	b.leases = b.leases[limit:]

	return out, nil
}

func (b *fakeDurableBackend) Ack(_ context.Context, lease worker.DurableTaskLease) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.acked = append(b.acked, lease.Task.ID)

	return nil
}

func (b *fakeDurableBackend) Nack(_ context.Context, lease worker.DurableTaskLease, delay time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nacked = append(b.nacked, lease.Task.ID)
	b.nackDelay = delay

	return nil
}

func (b *fakeDurableBackend) Fail(_ context.Context, lease worker.DurableTaskLease, _ error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failed = append(b.failed, lease.Task.ID)

	return nil
}

type testCodec struct {
	marshalCalls   int
	unmarshalCalls int
}

func (c *testCodec) Marshal(_ proto.Message) ([]byte, error) {
	c.marshalCalls++

	return []byte("payload"), nil
}

func (c *testCodec) Unmarshal(_ []byte, _ proto.Message) error {
	c.unmarshalCalls++

	return nil
}

func stopTaskManager(t *testing.T, tm *worker.TaskManager) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := tm.StopGraceful(ctx)
	if err != nil {
		t.Fatalf("stop task manager: %v", err)
	}
}

func TestRegisterDurableTask_MarshalPayload(t *testing.T) {
	t.Parallel()

	backend := &fakeDurableBackend{}
	codec := &testCodec{}
	handlers := map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, proto.Message) (any, error) {
				return "ok", nil
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurableCodec(codec),
		worker.WithDurablePollInterval(testPollInterval),
	)

	t.Cleanup(func() { stopTaskManager(t, tm) })

	err := tm.RegisterDurableTask(context.Background(), worker.DurableTask{
		Handler: testTaskName,
		Message: &workerpb.SendEmailPayload{
			To:      "ops@example.com",
			Subject: "hi",
			Body:    "body",
		},
	})
	if err != nil {
		t.Fatalf("register durable task: %v", err)
	}

	if codec.marshalCalls != 1 {
		t.Fatalf("expected marshal call, got %d", codec.marshalCalls)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued task, got %d", len(backend.enqueued))
	}

	if string(backend.enqueued[0].Payload) != "payload" {
		t.Fatalf("unexpected payload: %q", string(backend.enqueued[0].Payload))
	}
}

func TestRegisterDurableTask_UnmarshalPayload(t *testing.T) {
	t.Parallel()

	backend := &fakeDurableBackend{}
	codec := &testCodec{}
	handlers := map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, proto.Message) (any, error) {
				return "ok", nil
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurableCodec(codec),
		worker.WithDurablePollInterval(testPollInterval),
	)

	t.Cleanup(func() { stopTaskManager(t, tm) })

	err := tm.RegisterDurableTask(context.Background(), worker.DurableTask{
		Handler: testTaskName,
		Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("register durable task: %v", err)
	}

	if codec.unmarshalCalls != 1 {
		t.Fatalf("expected unmarshal call, got %d", codec.unmarshalCalls)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued task, got %d", len(backend.enqueued))
	}

	if backend.enqueued[0].Message == nil {
		t.Fatal("expected message to be set from payload")
	}
}

func TestDurableLoop_AckOnSuccess(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	backend := &fakeDurableBackend{
		leases: []worker.DurableTaskLease{
			{
				Task: worker.DurableTask{
					ID:      taskID,
					Handler: testTaskName,
					Payload: payload,
					Retries: 0,
				},
				Attempts:   1,
				MaxRetries: 0,
			},
		},
	}

	handlers := map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, proto.Message) (any, error) {
				return "ok", nil
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurablePollInterval(testPollInterval),
		worker.WithDurableLease(testLease),
	)

	t.Cleanup(func() { stopTaskManager(t, tm) })

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	select {
	case res := <-results:
		if res.Error != nil {
			t.Fatalf("unexpected error: %v", res.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.acked) != 1 || backend.acked[0] != taskID {
		t.Fatalf("expected ack for %s, got %+v", taskID, backend.acked)
	}

	if len(backend.failed) != 0 || len(backend.nacked) != 0 {
		t.Fatalf("unexpected backend calls acked=%v nacked=%v failed=%v", backend.acked, backend.nacked, backend.failed)
	}
}

func TestDurableLoop_FailOnError(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	backend := &fakeDurableBackend{
		leases: []worker.DurableTaskLease{
			{
				Task: worker.DurableTask{
					ID:      taskID,
					Handler: testTaskName,
					Payload: payload,
					Retries: 0,
				},
				Attempts:   1,
				MaxRetries: 0,
			},
		},
	}

	handlers := map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, proto.Message) (any, error) {
				return nil, errTestBoom
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurablePollInterval(testPollInterval),
		worker.WithDurableLease(testLease),
	)

	t.Cleanup(func() { stopTaskManager(t, tm) })

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	select {
	case res := <-results:
		if res.Error == nil {
			t.Fatal("expected error result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.failed) != 1 || backend.failed[0] != taskID {
		t.Fatalf("expected fail for %s, got %+v", taskID, backend.failed)
	}

	if len(backend.acked) != 0 {
		t.Fatalf("unexpected acked=%v", backend.acked)
	}
}
