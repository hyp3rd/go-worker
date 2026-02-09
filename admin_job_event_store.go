package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/hyp3rd/ewrap"
	sectools "github.com/hyp3rd/sectools/pkg/io"
)

const (
	jobEventStoreAllKey       = "all"
	defaultJobEventCacheTTL   = 10 * time.Second
	defaultJobEventMaxEntries = 10000
)

// AdminJobEventStore persists and lists admin job events.
type AdminJobEventStore interface {
	Record(ctx context.Context, event AdminJobEvent) error
	List(ctx context.Context, filter AdminJobEventFilter) (AdminJobEventPage, error)
}

// FileJobEventStore stores job events as JSON files on disk.
type FileJobEventStore struct {
	baseDir    string
	ioClient   *sectools.Client
	cacheTTL   time.Duration
	maxEntries int

	mu    sync.Mutex
	cache map[string]jobEventCache
}

type jobEventCache struct {
	expires time.Time
	events  []AdminJobEvent
}

// FileJobEventStoreOption configures FileJobEventStore behavior.
type FileJobEventStoreOption func(*FileJobEventStore)

// WithJobEventStoreCacheTTL configures cache TTL for file-backed job events.
func WithJobEventStoreCacheTTL(ttl time.Duration) FileJobEventStoreOption {
	return func(store *FileJobEventStore) {
		if ttl >= 0 {
			store.cacheTTL = ttl
		}
	}
}

// WithJobEventStoreMaxEntries configures max entries retained per key (0 = unbounded).
func WithJobEventStoreMaxEntries(maxEntries int) FileJobEventStoreOption {
	return func(store *FileJobEventStore) {
		if maxEntries >= 0 {
			store.maxEntries = maxEntries
		}
	}
}

// NewFileJobEventStore creates a file-backed job event store.
func NewFileJobEventStore(dir string, opts ...FileJobEventStoreOption) (*FileJobEventStore, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, ewrap.New("job event dir is required")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, ewrap.Wrap(err, "resolve job event dir")
	}

	err = os.MkdirAll(absDir, 0o750)
	if err != nil {
		return nil, ewrap.Wrap(err, "prepare job event dir")
	}

	ioClient, err := sectools.NewWithOptions(
		sectools.WithAllowAbsolute(true),
		sectools.WithBaseDir(absDir),
		sectools.WithAllowedRoots(absDir),
		sectools.WithAllowSymlinks(true),
	)
	if err != nil {
		return nil, ewrap.Wrap(err, "init job event io")
	}

	store := &FileJobEventStore{
		baseDir:    absDir,
		ioClient:   ioClient,
		cacheTTL:   defaultJobEventCacheTTL,
		maxEntries: defaultJobEventMaxEntries,
		cache:      map[string]jobEventCache{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}

	return store, nil
}

// Record appends a job event record.
func (s *FileJobEventStore) Record(ctx context.Context, event AdminJobEvent) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return ewrap.Wrap(err, "encode job event")
	}

	err = s.writeEvent(jobEventStoreAllKey, event.TaskID, payload)
	if err != nil {
		return err
	}

	name := strings.TrimSpace(event.Name)
	if name == "" {
		return nil
	}

	return s.writeEvent(jobEventStoreKey(name), event.TaskID, payload)
}

// List returns recent job events.
func (s *FileJobEventStore) List(ctx context.Context, filter AdminJobEventFilter) (AdminJobEventPage, error) {
	if ctx == nil {
		return AdminJobEventPage{}, ErrInvalidTaskContext
	}

	key := jobEventStoreAllKey
	if name := strings.TrimSpace(filter.Name); name != "" {
		key = jobEventStoreKey(name)
	}

	limit := normalizeAdminEventLimit(filter.Limit, 0)
	if limit <= 0 {
		return AdminJobEventPage{Events: []AdminJobEvent{}}, nil
	}

	if cached, ok := s.cachedEvents(key); ok {
		if len(cached) > limit {
			cached = cached[:limit]
		}

		return AdminJobEventPage{Events: cached}, nil
	}

	events, err := s.readEvents(key, limit)
	if err != nil {
		return AdminJobEventPage{}, err
	}

	s.storeCache(key, events)

	return AdminJobEventPage{Events: events}, nil
}

func (s *FileJobEventStore) writeEvent(key, taskID string, payload []byte) error {
	dir := key

	err := s.ioClient.MkdirAll(dir)
	if err != nil {
		return ewrap.Wrap(err, "create job event dir")
	}

	filename := fmt.Sprintf("%013d_%s.json", time.Now().UnixMilli(), safeJobEventName(taskID))
	path := filepath.Join(dir, filename)

	err = s.ioClient.WriteFile(path, payload)
	if err != nil {
		return ewrap.Wrap(err, "write job event file")
	}

	s.pruneOldest(dir)
	s.invalidateCache(key)

	if key != jobEventStoreAllKey {
		s.invalidateCache(jobEventStoreAllKey)
	}

	return nil
}

func (s *FileJobEventStore) readEvents(key string, limit int) ([]AdminJobEvent, error) {
	dir := key

	entries, err := s.ioClient.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []AdminJobEvent{}, nil
		}

		return nil, ewrap.Wrap(err, "read job event dir")
	}

	files := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		files = append(files, entry)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() > files[j].Name()
	})

	events := make([]AdminJobEvent, 0, min(limit, len(files)))
	for _, entry := range files {
		if len(events) >= limit {
			break
		}

		path := filepath.Join(dir, entry.Name())

		raw, err := s.ioClient.ReadFile(path)
		if err != nil {
			return nil, ewrap.Wrap(err, "read job event file")
		}

		var event AdminJobEvent

		err = json.Unmarshal(raw, &event)
		if err != nil {
			return nil, ewrap.Wrap(err, "decode job event file")
		}

		events = append(events, event)
	}

	return events, nil
}

func (s *FileJobEventStore) pruneOldest(dir string) {
	if s.maxEntries <= 0 {
		return
	}

	entries, err := s.ioClient.ReadDir(dir)
	if err != nil {
		return
	}

	files := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		files = append(files, entry)
	}

	if len(files) <= s.maxEntries {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	excess := len(files) - s.maxEntries
	for i := range excess {
		err = s.ioClient.Remove(filepath.Join(dir, files[i].Name()))
		if err != nil {
			fmt.Fprintln(os.Stderr, ewrap.Wrap(err, "prune job event file"))
		}
	}
}

func (s *FileJobEventStore) cachedEvents(key string) ([]AdminJobEvent, bool) {
	if s.cacheTTL <= 0 {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.cache[key]
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}

	copied := make([]AdminJobEvent, len(entry.events))
	copy(copied, entry.events)

	return copied, true
}

func (s *FileJobEventStore) storeCache(key string, events []AdminJobEvent) {
	if s.cacheTTL <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]AdminJobEvent, len(events))
	copy(copied, events)

	s.cache[key] = jobEventCache{
		expires: time.Now().Add(s.cacheTTL),
		events:  copied,
	}
}

func (s *FileJobEventStore) invalidateCache(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cache, key)
}

func jobEventStoreKey(name string) string {
	return safeJobEventName(name)
}

func safeJobEventName(name string) string {
	if name == "" {
		return "unknown"
	}

	var builder strings.Builder
	builder.Grow(len(name))

	for _, r := range name {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' {
			builder.WriteRune(r)
		} else {
			builder.WriteByte('_')
		}
	}

	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return "unknown"
	}

	return out
}
