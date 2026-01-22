package worker

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// RetentionPolicy controls how completed tasks are kept in the registry.
type RetentionPolicy struct {
	TTL             time.Duration
	MaxEntries      int
	CleanupInterval time.Duration
}

type retentionConfig struct {
	ttl        time.Duration
	maxEntries int
	interval   time.Duration
}

func normalizeRetentionPolicy(policy RetentionPolicy) retentionConfig {
	ttl := max(policy.TTL, 0)

	maxEntries := max(policy.MaxEntries, 0)

	interval := policy.CleanupInterval
	if interval <= 0 {
		interval = retentionInterval(ttl)
	}

	return retentionConfig{
		ttl:        ttl,
		maxEntries: maxEntries,
		interval:   interval,
	}
}

func retentionInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Minute
	}

	interval := ttl / 2
	if interval < time.Second {
		return time.Second
	}

	if interval > time.Minute {
		return time.Minute
	}

	return interval
}

// SetRetentionPolicy configures task registry retention.
func (tm *TaskManager) SetRetentionPolicy(policy RetentionPolicy) {
	cfg := normalizeRetentionPolicy(policy)

	tm.retentionMu.Lock()
	tm.retention = cfg
	tm.retentionMu.Unlock()

	tm.retentionLastCheck.Store(0)

	if cfg.ttl > 0 {
		tm.startRetentionLoop()
	}

	tm.maybePruneRegistry()
}

func (tm *TaskManager) retentionConfig() retentionConfig {
	tm.retentionMu.RLock()
	defer tm.retentionMu.RUnlock()

	return tm.retention
}

func (tm *TaskManager) registrySize() int {
	tm.registryMu.RLock()
	defer tm.registryMu.RUnlock()

	return len(tm.registry)
}

func (tm *TaskManager) startRetentionLoop() {
	tm.retentionOnce.Do(func() {
		tm.workerWg.Add(1)

		go tm.retentionLoop()
	})
}

func (tm *TaskManager) retentionLoop() {
	defer tm.workerWg.Done()

	for {
		cfg := tm.retentionConfig()

		interval := cfg.interval
		if interval <= 0 {
			interval = time.Minute
		}

		timer := time.NewTimer(interval)
		select {
		case <-tm.ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
			tm.maybePruneRegistry()
		}
	}
}

func (tm *TaskManager) maybePruneRegistry() {
	cfg := tm.retentionConfig()
	if cfg.ttl <= 0 && cfg.maxEntries <= 0 {
		return
	}

	if !tm.retentionPrune.CompareAndSwap(false, true) {
		return
	}
	defer tm.retentionPrune.Store(false)

	now := time.Now()

	ttlTriggered := false

	if cfg.ttl > 0 {
		last := time.Unix(0, tm.retentionLastCheck.Load())
		if now.Sub(last) >= cfg.interval {
			ttlTriggered = true

			tm.retentionLastCheck.Store(now.UnixNano())
		}
	}

	maxTriggered := cfg.maxEntries > 0 && tm.registrySize() > cfg.maxEntries

	if !ttlTriggered && !maxTriggered {
		return
	}

	tm.pruneRegistry(now, cfg)
}

type retentionEntry struct {
	id uuid.UUID
	at time.Time
}

//nolint:cyclop
func (tm *TaskManager) pruneRegistry(now time.Time, cfg retentionConfig) {
	tm.registryMu.Lock()
	defer tm.registryMu.Unlock()

	if cfg.ttl <= 0 && cfg.maxEntries <= 0 {
		return
	}

	var terminal []retentionEntry
	if cfg.maxEntries > 0 {
		terminal = make([]retentionEntry, 0, len(tm.registry))
	}

	for id, task := range tm.registry {
		terminalAt, ok := task.terminalAt()
		if !ok {
			continue
		}

		if cfg.ttl > 0 && !terminalAt.IsZero() && now.Sub(terminalAt) >= cfg.ttl {
			delete(tm.registry, id)

			continue
		}

		if cfg.maxEntries > 0 {
			terminal = append(terminal, retentionEntry{id: id, at: terminalAt})
		}
	}

	if cfg.maxEntries <= 0 || len(tm.registry) <= cfg.maxEntries {
		return
	}

	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].at.Before(terminal[j].at)
	})

	for _, entry := range terminal {
		if len(tm.registry) <= cfg.maxEntries {
			return
		}

		delete(tm.registry, entry.id)
	}
}
