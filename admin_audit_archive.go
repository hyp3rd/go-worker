package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	sectools "github.com/hyp3rd/sectools/pkg/io"
)

const (
	defaultAdminAuditArchiveInterval = 30 * time.Second
	adminAuditArchiveQueueSize       = 2048
	adminAuditArchiveBatchSize       = 256
	adminAuditArchiveEventBytes      = 128
	adminAuditArchiveFileExt         = ".jsonl"
)

type adminAuditArchiver struct {
	interval time.Duration
	queue    chan AdminAuditEvent
	store    *fileAdminAuditArchiveStore
}

type fileAdminAuditArchiveStore struct {
	mu       sync.Mutex
	baseDir  string
	ioClient *sectools.Client
}

func newAdminAuditArchiver(dir string, interval time.Duration) *adminAuditArchiver {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}

	store, err := newFileAdminAuditArchiveStore(dir)
	if err != nil {
		log.Printf("admin audit archive disabled: %v", err)

		return nil
	}

	if interval <= 0 {
		interval = defaultAdminAuditArchiveInterval
	}

	return &adminAuditArchiver{
		interval: interval,
		queue:    make(chan AdminAuditEvent, adminAuditArchiveQueueSize),
		store:    store,
	}
}

func newFileAdminAuditArchiveStore(dir string) (*fileAdminAuditArchiveStore, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, ewrap.Wrap(err, "resolve admin audit archive dir")
	}

	err = os.MkdirAll(absDir, 0o750)
	if err != nil {
		return nil, ewrap.Wrap(err, "prepare admin audit archive dir")
	}

	ioClient, err := sectools.NewWithOptions(
		sectools.WithAllowAbsolute(true),
		sectools.WithBaseDir(absDir),
		sectools.WithAllowedRoots(absDir),
		sectools.WithAllowSymlinks(true),
		sectools.WithDirMode(0o750),
		sectools.WithWriteFileMode(0o600),
	)
	if err != nil {
		return nil, ewrap.Wrap(err, "init admin audit archive io")
	}

	return &fileAdminAuditArchiveStore{
		baseDir:  absDir,
		ioClient: ioClient,
	}, nil
}

func (store *fileAdminAuditArchiveStore) write(events []AdminAuditEvent) error {
	if len(events) == 0 {
		return nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	payload, err := marshalAdminAuditJSONL(events)
	if err != nil {
		return err
	}

	filename := fmt.Sprintf(
		"audit-%013d-%s%s",
		time.Now().UTC().UnixMilli(),
		uuid.NewString(),
		adminAuditArchiveFileExt,
	)

	err = store.ioClient.WriteFile(filename, payload)
	if err != nil {
		return ewrap.Wrap(err, "write admin audit archive")
	}

	return nil
}

func marshalAdminAuditJSONL(events []AdminAuditEvent) ([]byte, error) {
	buffer := make([]byte, 0, len(events)*adminAuditArchiveEventBytes)
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			return nil, ewrap.Wrap(err, "encode admin audit archive event")
		}

		buffer = append(buffer, line...)
		buffer = append(buffer, '\n')
	}

	return buffer, nil
}

func (archiver *adminAuditArchiver) enqueue(events []AdminAuditEvent) {
	if archiver == nil || len(events) == 0 {
		return
	}

	dropped := 0

	for _, event := range events {
		select {
		case archiver.queue <- event:
		default:
			dropped++
		}
	}

	if dropped > 0 {
		log.Printf("admin audit archival queue full; dropped=%d", dropped)
	}
}

func (archiver *adminAuditArchiver) start(ctx context.Context, wg *sync.WaitGroup) {
	if archiver == nil || archiver.store == nil || ctx == nil || wg == nil {
		return
	}

	wg.Go(func() {
		archiver.loop(ctx)
	})
}

func (archiver *adminAuditArchiver) loop(ctx context.Context) {
	interval := archiver.interval
	if interval <= 0 {
		interval = defaultAdminAuditArchiveInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([]AdminAuditEvent, 0, adminAuditArchiveBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}

		err := archiver.store.write(batch)
		if err != nil {
			log.Printf("admin audit archival write: %v", err)
		}

		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()

			return
		case <-ticker.C:
			flush()
		case event := <-archiver.queue:
			batch = append(batch, event)
			if len(batch) >= adminAuditArchiveBatchSize {
				flush()
			}
		}
	}
}

func (tm *TaskManager) startAuditArchival(ctx context.Context) {
	if tm == nil || tm.auditArchiver == nil {
		return
	}

	tm.auditArchiver.start(ctx, &tm.workerWg)
}

func (tm *TaskManager) archiveAuditEvents(events []AdminAuditEvent) {
	if tm == nil || tm.auditArchiver == nil || len(events) == 0 {
		return
	}

	tm.auditArchiver.enqueue(events)
}
