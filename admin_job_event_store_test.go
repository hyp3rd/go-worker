package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
)

const (
	testEventSleep        = 2 * time.Millisecond
	testCacheTTL          = 30 * time.Millisecond
	testCacheWait         = 50 * time.Millisecond
	testListLimit         = 100
	testMaxPrunedEntries  = 2
	testExpectedAllEvents = 3
	testConcurrencyJobs   = 4
	testConcurrencyRuns   = 50
	testReadIterations    = 120
	testDurationMillis    = 1000
)

func TestFileJobEventStore_Ordering(t *testing.T) {
	t.Parallel()

	store := testNewFileJobEventStore(t, WithJobEventStoreCacheTTL(0), WithJobEventStoreMaxEntries(0))
	ctx := context.Background()

	firstEvent := testAdminJobEvent("task-1", "job-order")
	secondEvent := testAdminJobEvent("task-2", "job-order")

	testRecordEvent(ctx, t, store, firstEvent)
	time.Sleep(testEventSleep)
	testRecordEvent(ctx, t, store, secondEvent)

	page, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}

	if len(page.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(page.Events))
	}

	if page.Events[0].TaskID != secondEvent.TaskID {
		t.Fatalf("expected newest task %s first, got %s", secondEvent.TaskID, page.Events[0].TaskID)
	}

	if page.Events[1].TaskID != firstEvent.TaskID {
		t.Fatalf("expected oldest task %s second, got %s", firstEvent.TaskID, page.Events[1].TaskID)
	}
}

func TestFileJobEventStore_PerKeyIsolation(t *testing.T) {
	t.Parallel()

	store := testNewFileJobEventStore(t, WithJobEventStoreCacheTTL(0), WithJobEventStoreMaxEntries(0))
	ctx := context.Background()

	alphaFirst := testAdminJobEvent("alpha-1", "alpha")
	bravoFirst := testAdminJobEvent("bravo-1", "bravo")
	alphaSecond := testAdminJobEvent("alpha-2", "alpha")

	testRecordEvent(ctx, t, store, alphaFirst)
	time.Sleep(testEventSleep)
	testRecordEvent(ctx, t, store, bravoFirst)
	time.Sleep(testEventSleep)
	testRecordEvent(ctx, t, store, alphaSecond)

	alphaPage, err := store.List(ctx, AdminJobEventFilter{Name: "alpha", Limit: testListLimit})
	if err != nil {
		t.Fatalf("list alpha events: %v", err)
	}

	if len(alphaPage.Events) != 2 {
		t.Fatalf("expected 2 alpha events, got %d", len(alphaPage.Events))
	}

	for _, event := range alphaPage.Events {
		if event.Name != "alpha" {
			t.Fatalf("expected only alpha events, got %q", event.Name)
		}
	}

	allPage, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("list all events: %v", err)
	}

	if len(allPage.Events) != testExpectedAllEvents {
		t.Fatalf("expected %d total events, got %d", testExpectedAllEvents, len(allPage.Events))
	}
}

func TestFileJobEventStore_PrunesToMaxEntries(t *testing.T) {
	t.Parallel()

	store := testNewFileJobEventStore(
		t,
		WithJobEventStoreCacheTTL(0),
		WithJobEventStoreMaxEntries(testMaxPrunedEntries),
	)
	ctx := context.Background()

	oldest := testAdminJobEvent("prune-1", "prune")
	middle := testAdminJobEvent("prune-2", "prune")
	newest := testAdminJobEvent("prune-3", "prune")

	testRecordEvent(ctx, t, store, oldest)
	time.Sleep(testEventSleep)
	testRecordEvent(ctx, t, store, middle)
	time.Sleep(testEventSleep)
	testRecordEvent(ctx, t, store, newest)

	allPage, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("list all events: %v", err)
	}

	if len(allPage.Events) != testMaxPrunedEntries {
		t.Fatalf("expected %d events after prune, got %d", testMaxPrunedEntries, len(allPage.Events))
	}

	if containsTaskID(allPage.Events, oldest.TaskID) {
		t.Fatalf("expected oldest event %s to be pruned", oldest.TaskID)
	}

	namePage, err := store.List(ctx, AdminJobEventFilter{Name: "prune", Limit: testListLimit})
	if err != nil {
		t.Fatalf("list named events: %v", err)
	}

	if len(namePage.Events) != testMaxPrunedEntries {
		t.Fatalf("expected %d named events after prune, got %d", testMaxPrunedEntries, len(namePage.Events))
	}
}

func TestFileJobEventStore_CacheTTL(t *testing.T) {
	t.Parallel()

	store := testNewFileJobEventStore(
		t,
		WithJobEventStoreCacheTTL(testCacheTTL),
		WithJobEventStoreMaxEntries(0),
	)
	ctx := context.Background()

	firstEvent := testAdminJobEvent("ttl-1", "cache")
	testRecordEvent(ctx, t, store, firstEvent)

	firstPage, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("first list: %v", err)
	}

	if len(firstPage.Events) != 1 {
		t.Fatalf("expected 1 cached event, got %d", len(firstPage.Events))
	}

	secondEvent := testAdminJobEvent("ttl-2", "cache")

	rawEvent, err := json.Marshal(secondEvent)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	filename := fmt.Sprintf("%013d_%s.json", time.Now().UnixMilli()+1000, safeJobEventName(secondEvent.TaskID))
	writePath := filepath.Join(store.baseDir, jobEventStoreAllKey, filename)

	err = os.WriteFile(writePath, rawEvent, 0o600)
	if err != nil {
		t.Fatalf("write event file: %v", err)
	}

	cachedPage, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("cached list: %v", err)
	}

	if len(cachedPage.Events) != 1 {
		t.Fatalf("expected cached list to stay at 1 entry, got %d", len(cachedPage.Events))
	}

	time.Sleep(testCacheWait)

	refreshedPage, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
	if err != nil {
		t.Fatalf("refreshed list: %v", err)
	}

	if len(refreshedPage.Events) != 2 {
		t.Fatalf("expected refreshed list to include 2 entries, got %d", len(refreshedPage.Events))
	}
}

func TestFileJobEventStore_ConcurrentRecordAndList(t *testing.T) {
	t.Parallel()

	store := testNewFileJobEventStore(t, WithJobEventStoreCacheTTL(0), WithJobEventStoreMaxEntries(0))
	ctx := context.Background()
	errCh := make(chan error, testConcurrencyJobs+testConcurrencyJobs)

	var waitGroup sync.WaitGroup

	runConcurrentJobEventWrites(ctx, store, errCh, &waitGroup)
	runConcurrentJobEventReads(ctx, store, errCh, &waitGroup)

	waitGroup.Wait()
	close(errCh)

	assertNoConcurrentErrors(t, errCh)

	expected := testConcurrencyJobs * testConcurrencyRuns

	page, err := store.List(ctx, AdminJobEventFilter{Limit: expected})
	if err != nil {
		t.Fatalf("list final events: %v", err)
	}

	if len(page.Events) != expected {
		t.Fatalf("expected %d events, got %d", expected, len(page.Events))
	}
}

func testNewFileJobEventStore(t *testing.T, opts ...FileJobEventStoreOption) *FileJobEventStore {
	t.Helper()

	store, err := NewFileJobEventStore(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("new file job event store: %v", err)
	}

	return store
}

func testRecordEvent(ctx context.Context, t *testing.T, store *FileJobEventStore, event AdminJobEvent) {
	t.Helper()

	err := store.Record(ctx, event)
	if err != nil {
		t.Fatalf("record event: %v", err)
	}
}

func testAdminJobEvent(taskID, name string) AdminJobEvent {
	now := time.Now().UTC()

	return AdminJobEvent{
		TaskID:     taskID,
		Name:       name,
		Status:     "completed",
		Queue:      "default",
		StartedAt:  now.Add(-time.Second),
		FinishedAt: now,
		DurationMs: testDurationMillis,
		Result:     "ok",
	}
}

func runConcurrentJobEventWrites(
	ctx context.Context,
	store *FileJobEventStore,
	errCh chan<- error,
	waitGroup *sync.WaitGroup,
) {
	for writerIndex := range testConcurrencyJobs {
		waitGroup.Add(1)

		go func(currentWriter int) {
			defer waitGroup.Done()

			for runIndex := range testConcurrencyRuns {
				event := testAdminJobEvent(fmt.Sprintf("w-%d-r-%d", currentWriter, runIndex), "concurrent")

				err := store.Record(ctx, event)
				if err != nil {
					errCh <- err

					return
				}
			}
		}(writerIndex)
	}
}

func runConcurrentJobEventReads(
	ctx context.Context,
	store *FileJobEventStore,
	errCh chan<- error,
	waitGroup *sync.WaitGroup,
) {
	for range testConcurrencyJobs {
		waitGroup.Go(func() {
			for range testReadIterations {
				_, err := store.List(ctx, AdminJobEventFilter{Limit: testListLimit})
				if err != nil {
					errCh <- err

					return
				}
			}
		})
	}
}

func assertNoConcurrentErrors(t *testing.T, errCh <-chan error) {
	t.Helper()

	for err := range errCh {
		t.Fatalf("concurrent operations failed: %v", err)
	}
}

func containsTaskID(events []AdminJobEvent, taskID string) bool {
	taskIDs := make([]string, 0, len(events))
	for _, event := range events {
		taskIDs = append(taskIDs, event.TaskID)
	}

	return slices.Contains(taskIDs, taskID)
}
