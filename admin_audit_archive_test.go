package worker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyp3rd/ewrap"
	sectools "github.com/hyp3rd/sectools/pkg/io"
)

const (
	testAuditArchiveWait    = 250 * time.Millisecond
	testAuditArchiveBackoff = 10 * time.Millisecond
	testAuditEventLimit     = 100
)

func TestAdminAuditArchivalWritesExpiredEvents(t *testing.T) {
	t.Parallel()

	archiveDir := t.TempDir()
	tm := NewTaskManagerWithOptions(
		context.Background(),
		WithAdminAuditEventLimit(testAuditEventLimit),
		WithAdminAuditRetention(25*time.Millisecond),
		WithAdminAuditArchiveDir(archiveDir),
		WithAdminAuditArchiveInterval(10*time.Millisecond),
	)
	t.Cleanup(tm.StopNow)

	first := AdminAuditEvent{
		At:        time.Now(),
		Actor:     "tester",
		Action:    "queue.pause",
		Target:    "default",
		Status:    "ok",
		Detail:    "pause",
		RequestID: "req-1",
	}
	second := AdminAuditEvent{
		At:        time.Now().Add(40 * time.Millisecond),
		Actor:     "tester",
		Action:    "queue.resume",
		Target:    "default",
		Status:    "ok",
		Detail:    "resume",
		RequestID: "req-2",
	}

	err := tm.AdminRecordAuditEvent(context.Background(), first, testAuditEventLimit)
	if err != nil {
		t.Fatalf("record first audit event: %v", err)
	}

	time.Sleep(40 * time.Millisecond)

	err = tm.AdminRecordAuditEvent(context.Background(), second, testAuditEventLimit)
	if err != nil {
		t.Fatalf("record second audit event: %v", err)
	}

	content, ok := waitArchiveContains(t, archiveDir, `"action":"queue.pause"`)
	if !ok {
		t.Fatalf("archived event not found; content=%q", content)
	}

	page, err := tm.AdminAuditEvents(context.Background(), AdminAuditEventFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}

	for _, event := range page.Events {
		if event.Action == first.Action {
			t.Fatalf("expired event should not remain in active set: %s", event.Action)
		}
	}
}

func waitArchiveContains(t *testing.T, dir, needle string) (string, bool) {
	t.Helper()

	reader, err := sectools.NewWithOptions(
		sectools.WithAllowAbsolute(true),
		sectools.WithBaseDir(dir),
		sectools.WithAllowedRoots(dir),
		sectools.WithAllowSymlinks(true),
	)
	if err != nil {
		t.Fatalf("init archive reader: %v", ewrap.Wrap(err, "init archive reader"))
	}

	deadline := time.Now().Add(testAuditArchiveWait)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read archive dir: %v", err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
				continue
			}

			raw, err := reader.ReadFile(entry.Name())
			if err != nil {
				t.Fatalf("read archive file: %v", err)
			}

			content := string(raw)
			if strings.Contains(content, needle) {
				return content, true
			}
		}

		time.Sleep(testAuditArchiveBackoff)
	}

	return "", false
}
