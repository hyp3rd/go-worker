package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	worker "github.com/hyp3rd/go-worker"
)

const (
	adminTestSeedTaskCount      = 3
	adminTestLimit              = 10
	adminTestJobName            = "nightly-backup"
	adminTestJobDescription     = "nightly backup job"
	adminTestJobDescriptionEdit = "nightly backup job (updated)"
	adminTestJobRepo            = "example.com/org/repo"
	adminTestJobTag             = "v1.0.0"
	adminTestAuditActor         = "operator"
	adminTestActionPause        = "queue.pause"
	adminTestActionResume       = "queue.resume"
	adminTestTargetQueueA       = "queue:alpha"
	adminTestTargetQueueB       = "queue:beta"
	adminTestWeightBoost        = 42
)

func setupRedisAdmin(t *testing.T) (context.Context, *worker.RedisDurableBackend, func()) {
	t.Helper()

	addr := os.Getenv(redisEnvAddr)
	if addr == "" {
		t.Skipf(redisErrEnvNotSet, redisEnvAddr)
	}

	password := os.Getenv(redisEnvPassword)
	prefix := redisTestPrefix(t)

	ctx, cancel := context.WithTimeout(context.Background(), redisTestTimeout)
	client := newRedisClient(t, addr, password)
	cleanupRedisPrefix(ctx, t, client, prefix)

	backend := newRedisBackend(t, client, prefix)

	cleanup := func() {
		cancel()
		client.Close()
	}

	return ctx, backend, cleanup
}

// seedDLQ enqueues count tasks and fails each one so they land in the DLQ.
// Returns the task IDs in insertion order.
func seedDLQ(ctx context.Context, t *testing.T, backend *worker.RedisDurableBackend, count int) []string {
	t.Helper()

	ids := make([]string, 0, count)

	for i := range count {
		task := worker.DurableTask{
			ID:         uuid.New(),
			Handler:    redisHandler,
			Payload:    []byte(redisPayload),
			Priority:   1,
			RetryDelay: redisRetryDelay,
			Metadata:   map[string]string{"seq": uuidPosition(i)},
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

		ids = append(ids, task.ID.String())
	}

	return ids
}

func uuidPosition(i int) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, fmtInt(i)).String()
}

func fmtInt(i int) []byte {
	// Small helper avoids pulling in strconv for test seeds that just need a unique
	// deterministic byte string per iteration index.
	digits := []byte("0123456789")

	if i == 0 {
		return []byte{digits[0]}
	}

	var buf []byte

	for i > 0 {
		buf = append([]byte{digits[i%10]}, buf...)

		i /= 10
	}

	return buf
}

// =====================================================================
// Pause/Resume and Overview
// =====================================================================

func TestRedisDurableBackend_AdminPauseResumeOverview(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	overview, err := backend.AdminOverview(ctx)
	if err != nil {
		t.Fatalf("overview (initial): %v", err)
	}

	if overview.Coordination.Paused {
		t.Fatal("expected coordination.paused=false initially")
	}

	err = backend.AdminPause(ctx)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}

	overview, err = backend.AdminOverview(ctx)
	if err != nil {
		t.Fatalf("overview after pause: %v", err)
	}

	if !overview.Coordination.Paused {
		t.Fatal("expected coordination.paused=true after AdminPause")
	}

	err = backend.AdminResume(ctx)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	overview, err = backend.AdminOverview(ctx)
	if err != nil {
		t.Fatalf("overview after resume: %v", err)
	}

	if overview.Coordination.Paused {
		t.Fatal("expected coordination.paused=false after AdminResume")
	}
}

// =====================================================================
// Queue weight CRUD
// =====================================================================

func TestRedisDurableBackend_AdminQueueWeightLifecycle(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	const queue = "alpha"

	summary, err := backend.AdminSetQueueWeight(ctx, queue, adminTestWeightBoost)
	if err != nil {
		t.Fatalf("set weight: %v", err)
	}

	if summary.Weight != adminTestWeightBoost {
		t.Fatalf("weight after set = %d, want %d", summary.Weight, adminTestWeightBoost)
	}

	queues, err := backend.AdminQueues(ctx)
	if err != nil {
		t.Fatalf("list queues: %v", err)
	}

	found := false

	for _, summary := range queues {
		if summary.Name != queue {
			continue
		}

		found = true

		if summary.Weight != adminTestWeightBoost {
			t.Fatalf("weight in list = %d, want %d", summary.Weight, adminTestWeightBoost)
		}
	}

	if !found {
		t.Fatalf("weighted queue %q not present in AdminQueues result", queue)
	}

	resetSummary, err := backend.AdminResetQueueWeight(ctx, queue)
	if err != nil {
		t.Fatalf("reset weight: %v", err)
	}

	if resetSummary.Weight == adminTestWeightBoost {
		t.Fatalf("weight after reset still = %d, expected default", resetSummary.Weight)
	}
}

// =====================================================================
// AdminPauseQueue
// =====================================================================

func TestRedisDurableBackend_AdminPauseQueueTogglesFlag(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	const queue = "beta"

	// The queue needs to exist so AdminPauseQueue has something to toggle.
	_, err := backend.AdminSetQueueWeight(ctx, queue, 1)
	if err != nil {
		t.Fatalf("seed queue: %v", err)
	}

	paused, err := backend.AdminPauseQueue(ctx, queue, true)
	if err != nil {
		t.Fatalf("pause queue: %v", err)
	}

	if !paused.Paused {
		t.Fatalf("expected paused=true, got summary %+v", paused)
	}

	resumed, err := backend.AdminPauseQueue(ctx, queue, false)
	if err != nil {
		t.Fatalf("resume queue: %v", err)
	}

	if resumed.Paused {
		t.Fatalf("expected paused=false, got summary %+v", resumed)
	}
}

// =====================================================================
// DLQ listing + entry detail
// =====================================================================

func TestRedisDurableBackend_AdminDLQListAndEntry(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	ids := seedDLQ(ctx, t, backend, adminTestSeedTaskCount)

	page, err := backend.AdminDLQ(ctx, worker.AdminDLQFilter{Limit: adminTestLimit})
	if err != nil {
		t.Fatalf("AdminDLQ: %v", err)
	}

	if len(page.Entries) != adminTestSeedTaskCount {
		t.Fatalf("entries length = %d, want %d", len(page.Entries), adminTestSeedTaskCount)
	}

	// AdminDLQ should report a total equal to (or at least as large as) the seeded count.
	if page.Total < int64(adminTestSeedTaskCount) {
		t.Fatalf("total = %d, want >= %d", page.Total, adminTestSeedTaskCount)
	}

	detail, err := backend.AdminDLQEntry(ctx, ids[0])
	if err != nil {
		t.Fatalf("AdminDLQEntry: %v", err)
	}

	if detail.Handler != redisHandler {
		t.Fatalf("entry handler = %q, want %q", detail.Handler, redisHandler)
	}

	if detail.Attempts < 1 {
		t.Fatalf("entry attempts = %d, want >= 1", detail.Attempts)
	}
}

// =====================================================================
// AdminReplayDLQ atomicity
// =====================================================================

func TestRedisDurableBackend_AdminReplayDLQMovesEntries(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	_ = seedDLQ(ctx, t, backend, adminTestSeedTaskCount)

	moved, err := backend.AdminReplayDLQ(ctx, adminTestSeedTaskCount)
	if err != nil {
		t.Fatalf("AdminReplayDLQ: %v", err)
	}

	if moved != adminTestSeedTaskCount {
		t.Fatalf("replayed = %d, want %d", moved, adminTestSeedTaskCount)
	}

	// DLQ should be empty now.
	page, err := backend.AdminDLQ(ctx, worker.AdminDLQFilter{Limit: adminTestLimit})
	if err != nil {
		t.Fatalf("AdminDLQ post-replay: %v", err)
	}

	if len(page.Entries) != 0 {
		t.Fatalf("DLQ not drained after replay, entries = %d", len(page.Entries))
	}

	// Replayed tasks should be re-dequeueable. Dequeue is weighted round-robin:
	// default queue weight is 1, so a single call returns at most one task per
	// queue. Drain in a loop capped at 2x the expected count to avoid a tight
	// infinite loop if replay silently dropped entries.
	total := 0

	for range adminTestSeedTaskCount * 2 {
		batch, dequeueErr := backend.Dequeue(ctx, 1, redisLease)
		if dequeueErr != nil {
			t.Fatalf("dequeue replayed: %v", dequeueErr)
		}

		total += len(batch)
		if total >= adminTestSeedTaskCount {
			break
		}

		if len(batch) == 0 {
			break
		}
	}

	if total != adminTestSeedTaskCount {
		t.Fatalf("dequeued %d leases across attempts, want %d", total, adminTestSeedTaskCount)
	}
}

// =====================================================================
// AdminJob CRUD
// =====================================================================

func seedJob(ctx context.Context, t *testing.T, backend *worker.RedisDurableBackend) worker.AdminJob {
	t.Helper()

	spec := worker.AdminJobSpec{
		Name:        adminTestJobName,
		Description: adminTestJobDescription,
		Repo:        adminTestJobRepo,
		Tag:         adminTestJobTag,
	}

	created, err := backend.AdminUpsertJob(ctx, spec)
	if err != nil {
		t.Fatalf("upsert (seed): %v", err)
	}

	return created
}

func TestRedisDurableBackend_AdminJobUpsertCreate(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	created := seedJob(ctx, t, backend)

	if created.Name != adminTestJobName {
		t.Fatalf("created.Name = %q, want %q", created.Name, adminTestJobName)
	}

	if created.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set on create")
	}

	if created.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt to be set on create")
	}

	fetched, err := backend.AdminJob(ctx, adminTestJobName)
	if err != nil {
		t.Fatalf("AdminJob: %v", err)
	}

	if fetched.Description != adminTestJobDescription {
		t.Fatalf("fetched.Description = %q, want %q", fetched.Description, adminTestJobDescription)
	}
}

func TestRedisDurableBackend_AdminJobUpsertUpdatePreservesCreatedAt(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	created := seedJob(ctx, t, backend)

	// Sleep so UpdatedAt differs on low-resolution clocks.
	time.Sleep(10 * time.Millisecond)

	spec := worker.AdminJobSpec{
		Name:        adminTestJobName,
		Description: adminTestJobDescriptionEdit,
		Repo:        adminTestJobRepo,
		Tag:         adminTestJobTag,
	}

	updated, err := backend.AdminUpsertJob(ctx, spec)
	if err != nil {
		t.Fatalf("upsert (update): %v", err)
	}

	if !updated.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("CreatedAt changed on update: %v -> %v", created.CreatedAt, updated.CreatedAt)
	}

	if !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("UpdatedAt did not advance: %v -> %v", created.UpdatedAt, updated.UpdatedAt)
	}
}

func TestRedisDurableBackend_AdminJobListAndDelete(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	_ = seedJob(ctx, t, backend)

	jobs, err := backend.AdminJobs(ctx)
	if err != nil {
		t.Fatalf("AdminJobs: %v", err)
	}

	if len(jobs) == 0 {
		t.Fatal("expected at least one job in list")
	}

	deleted, err := backend.AdminDeleteJob(ctx, adminTestJobName)
	if err != nil {
		t.Fatalf("AdminDeleteJob: %v", err)
	}

	if !deleted {
		t.Fatal("expected AdminDeleteJob to report deleted=true")
	}

	jobs, err = backend.AdminJobs(ctx)
	if err != nil {
		t.Fatalf("AdminJobs post-delete: %v", err)
	}

	for _, job := range jobs {
		if job.Name == adminTestJobName {
			t.Fatalf("job %q still present after delete", adminTestJobName)
		}
	}
}

// =====================================================================
// AdminAuditEvents filtering
// =====================================================================

func TestRedisDurableBackend_AdminAuditEventsFilter(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	now := time.Now().UTC()
	events := []worker.AdminAuditEvent{
		{
			At:     now.Add(-3 * time.Second),
			Actor:  adminTestAuditActor,
			Action: adminTestActionPause,
			Target: adminTestTargetQueueA,
			Status: "ok",
		},
		{
			At:     now.Add(-2 * time.Second),
			Actor:  adminTestAuditActor,
			Action: adminTestActionResume,
			Target: adminTestTargetQueueA,
			Status: "ok",
		},
		{
			At:     now.Add(-1 * time.Second),
			Actor:  adminTestAuditActor,
			Action: adminTestActionPause,
			Target: adminTestTargetQueueB,
			Status: "ok",
		},
	}

	for _, ev := range events {
		err := backend.AdminRecordAuditEvent(ctx, ev, adminTestLimit)
		if err != nil {
			t.Fatalf("record audit event: %v", err)
		}
	}

	all, err := backend.AdminAuditEvents(ctx, worker.AdminAuditEventFilter{Limit: adminTestLimit})
	if err != nil {
		t.Fatalf("AdminAuditEvents (all): %v", err)
	}

	if len(all.Events) < len(events) {
		t.Fatalf("got %d events, want >= %d", len(all.Events), len(events))
	}

	pauses, err := backend.AdminAuditEvents(ctx, worker.AdminAuditEventFilter{Action: adminTestActionPause, Limit: adminTestLimit})
	if err != nil {
		t.Fatalf("AdminAuditEvents (action=pause): %v", err)
	}

	for _, ev := range pauses.Events {
		if ev.Action != adminTestActionPause {
			t.Fatalf("expected only %q events, got %q", adminTestActionPause, ev.Action)
		}
	}

	// Target filter should scope further.
	targetA, err := backend.AdminAuditEvents(ctx, worker.AdminAuditEventFilter{
		Target: adminTestTargetQueueA,
		Limit:  adminTestLimit,
	})
	if err != nil {
		t.Fatalf("AdminAuditEvents (target): %v", err)
	}

	for _, ev := range targetA.Events {
		if ev.Target != adminTestTargetQueueA {
			t.Fatalf("expected only target=%q, got %q", adminTestTargetQueueA, ev.Target)
		}
	}
}

// =====================================================================
// AdminRecordScheduleEvent trim at limit
// =====================================================================

func TestRedisDurableBackend_AdminScheduleEventsTrimAtLimit(t *testing.T) {
	t.Parallel()

	ctx, backend, cleanup := setupRedisAdmin(t)
	defer cleanup()

	const trimLimit = 5

	const recordedTotal = 10

	for i := range recordedTotal {
		ev := worker.AdminScheduleEvent{
			TaskID: uuid.NewString(),
			Name:   "tick",
			Status: "completed",
			// Stagger timestamps so the newest entries are the last written.
			StartedAt:  time.Now().UTC().Add(time.Duration(-recordedTotal+i) * time.Millisecond),
			FinishedAt: time.Now().UTC(),
		}

		err := backend.AdminRecordScheduleEvent(ctx, ev, trimLimit)
		if err != nil {
			t.Fatalf("record schedule event %d: %v", i, err)
		}
	}

	page, err := backend.AdminScheduleEvents(ctx, worker.AdminScheduleEventFilter{Limit: recordedTotal * 2})
	if err != nil {
		t.Fatalf("AdminScheduleEvents: %v", err)
	}

	if len(page.Events) != trimLimit {
		t.Fatalf("got %d events, want %d (trim limit)", len(page.Events), trimLimit)
	}
}
