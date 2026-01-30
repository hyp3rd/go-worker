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
	testPollInterval  = 5 * time.Millisecond
	testLease         = 2 * time.Second
	testTaskName      = "send_email"
	testTimeout       = 10 * time.Millisecond
	errMarshalPayload = "marshal payload: %v"
)

var errTestBoom = errors.New("boom")

type fakeDurableBackend struct {
	mu        sync.Mutex
	leases    []worker.DurableTaskLease
	enqueued  []worker.DurableTask
	acked     []uuid.UUID
	nacked    []uuid.UUID
	failed    []uuid.UUID
	extended  []uuid.UUID
	nackDelay time.Duration
	nackCh    chan struct{}
	failCh    chan struct{}
	extendCh  chan struct{}
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
	if b.nackCh != nil {
		select {
		case b.nackCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (b *fakeDurableBackend) Fail(_ context.Context, lease worker.DurableTaskLease, _ error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failed = append(b.failed, lease.Task.ID)
	if b.failCh != nil {
		select {
		case b.failCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (b *fakeDurableBackend) Extend(_ context.Context, lease worker.DurableTaskLease, _ time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.extended = append(b.extended, lease.Task.ID)
	if b.extendCh != nil {
		select {
		case b.extendCh <- struct{}{}:
		default:
		}
	}

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

func newDurableLeaseBackend(taskID uuid.UUID, payload []byte, retries int) *fakeDurableBackend {
	return &fakeDurableBackend{
		leases: []worker.DurableTaskLease{
			{
				Task: worker.DurableTask{
					ID:      taskID,
					Handler: testTaskName,
					Payload: payload,
					Retries: retries,
				},
				Attempts:   1,
				MaxRetries: retries,
			},
		},
	}
}

func closeOnce(ch chan struct{}) func() {
	var once sync.Once

	return func() {
		once.Do(func() { close(ch) })
	}
}

func blockingDurableHandlers(done <-chan struct{}) map[string]worker.DurableHandlerSpec {
	return map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(context.Context, proto.Message) (any, error) {
				<-done

				return "ok", nil
			},
		},
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
		t.Fatalf(errMarshalPayload, err)
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

func TestDurableLoop_ExtendLease(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf(errMarshalPayload, err)
	}

	backend := newDurableLeaseBackend(taskID, payload, 0)
	backend.extendCh = make(chan struct{}, 1)

	done := make(chan struct{})
	closeDone := closeOnce(done)
	handlers := blockingDurableHandlers(done)

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurablePollInterval(testPollInterval),
		worker.WithDurableLease(50*time.Millisecond),
		worker.WithDurableLeaseRenewalInterval(10*time.Millisecond),
	)

	t.Cleanup(func() {
		closeDone()
		stopTaskManager(t, tm)
	})

	results, cancel := tm.SubscribeResults(1)
	defer cancel()

	select {
	case <-backend.extendCh:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for lease renewal")
	}

	closeDone()

	select {
	case <-results:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.extended) == 0 {
		t.Fatal("expected lease extension to be recorded")
	}
}

func TestDurableLoop_FailOnError(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf(errMarshalPayload, err)
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

func TestDurableLoop_NackOnRetry(t *testing.T) {
	t.Parallel()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf(errMarshalPayload, err)
	}

	nackCh := make(chan struct{}, 1)
	backend := &fakeDurableBackend{
		leases: []worker.DurableTaskLease{
			{
				Task: worker.DurableTask{
					ID:      taskID,
					Handler: testTaskName,
					Payload: payload,
					Retries: 2,
				},
				Attempts:   1,
				MaxRetries: 2,
			},
		},
		nackCh: nackCh,
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

	select {
	case <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if len(backend.nacked) != 1 || backend.nacked[0] != taskID {
		t.Fatalf("expected nack for %s, got %+v", taskID, backend.nacked)
	}

	if len(backend.failed) != 0 {
		t.Fatalf("expected no fail, got %+v", backend.failed)
	}

	if backend.nackDelay <= 0 {
		t.Fatalf("expected positive nack delay, got %s", backend.nackDelay)
	}
}

func TestDurableLoop_NackOnDeadline(t *testing.T) {
	t.Parallel()

	backend, taskID := setupDeadlineBackend(t, 1, 1)

	tm := newDeadlineTaskManager(backend, deadlineHandlers())

	t.Cleanup(func() { stopTaskManager(t, tm) })

	waitForBackendEvent(t, backend.nackCh, "nack")
	assertDeadlineBackend(t, backend, taskID, true)
}

func TestDurableLoop_FailOnDeadline(t *testing.T) {
	t.Parallel()

	backend, taskID := setupDeadlineBackend(t, 0, 0)

	tm := newDeadlineTaskManager(backend, deadlineHandlers())

	t.Cleanup(func() { stopTaskManager(t, tm) })

	waitForBackendEvent(t, backend.failCh, "fail")
	assertDeadlineBackend(t, backend, taskID, false)
}

func setupDeadlineBackend(t *testing.T, retries, maxRetries int) (*fakeDurableBackend, uuid.UUID) {
	t.Helper()

	taskID := uuid.New()

	payload, err := proto.Marshal(&workerpb.SendEmailPayload{To: "ops@example.com"})
	if err != nil {
		t.Fatalf(errMarshalPayload, err)
	}

	nackCh := make(chan struct{}, 1)
	failCh := make(chan struct{}, 1)

	backend := &fakeDurableBackend{
		leases: []worker.DurableTaskLease{
			{
				Task: worker.DurableTask{
					ID:      taskID,
					Handler: testTaskName,
					Payload: payload,
					Retries: retries,
				},
				Attempts:   1,
				MaxRetries: maxRetries,
			},
		},
		nackCh: nackCh,
		failCh: failCh,
	}

	return backend, taskID
}

func deadlineHandlers() map[string]worker.DurableHandlerSpec {
	return map[string]worker.DurableHandlerSpec{
		testTaskName: {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(ctx context.Context, _ proto.Message) (any, error) {
				<-ctx.Done()

				return nil, ctx.Err()
			},
		},
	}
}

func newDeadlineTaskManager(backend *fakeDurableBackend, handlers map[string]worker.DurableHandlerSpec) *worker.TaskManager {
	return worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableHandlers(handlers),
		worker.WithDurablePollInterval(testPollInterval),
		worker.WithDurableLease(testLease),
		worker.WithTimeout(testTimeout),
	)
}

func waitForBackendEvent(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", label)
	}
}

func assertDeadlineBackend(t *testing.T, backend *fakeDurableBackend, taskID uuid.UUID, expectNack bool) {
	t.Helper()

	backend.mu.Lock()
	defer backend.mu.Unlock()

	if expectNack {
		if len(backend.nacked) != 1 || backend.nacked[0] != taskID {
			t.Fatalf("expected nack for %s, got %+v", taskID, backend.nacked)
		}

		if len(backend.failed) != 0 {
			t.Fatalf("expected no fail, got %+v", backend.failed)
		}

		if backend.nackDelay <= 0 {
			t.Fatalf("expected positive nack delay, got %s", backend.nackDelay)
		}

		return
	}

	if len(backend.failed) != 1 || backend.failed[0] != taskID {
		t.Fatalf("expected fail for %s, got %+v", taskID, backend.failed)
	}

	if len(backend.nacked) != 0 {
		t.Fatalf("expected no nack, got %+v", backend.nacked)
	}
}
