package worker

import (
	"context"
	"errors"
	"strings"
	"time"
)

const (
	defaultAdminAuditEventLimit = 500
	adminAuditPersistTimeout    = 2 * time.Second
)

// AdminRecordAuditEvent persists an admin mutation audit record.
func (tm *TaskManager) AdminRecordAuditEvent(ctx context.Context, event AdminAuditEvent, _ int) error {
	if tm == nil {
		return ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return ErrInvalidTaskContext
	}

	tm.recordAdminAuditEvent(ctx, event)

	return nil
}

// AdminAuditEvents returns recent admin mutation audit records.
func (tm *TaskManager) AdminAuditEvents(
	ctx context.Context,
	filter AdminAuditEventFilter,
) (AdminAuditEventPage, error) {
	if tm == nil {
		return AdminAuditEventPage{}, ErrAdminBackendUnavailable
	}

	if ctx == nil {
		return AdminAuditEventPage{}, ErrInvalidTaskContext
	}

	page, ok, err := tm.auditEventsFromBackend(ctx, filter)
	if err != nil {
		return AdminAuditEventPage{}, err
	}

	if ok {
		return page, nil
	}

	return tm.auditEventsFromMemory(filter), nil
}

func (tm *TaskManager) recordAdminAuditEvent(ctx context.Context, event AdminAuditEvent) {
	if tm == nil {
		return
	}

	if event.At.IsZero() {
		event.At = time.Now()
	}

	if strings.TrimSpace(event.Status) == "" {
		event.Status = "ok"
	}

	tm.auditEventsMu.Lock()

	tm.auditEvents = append(tm.auditEvents, event)

	archived := tm.pruneAuditEventsByAgeLocked(time.Now())
	if tm.auditEventLimit > 0 && len(tm.auditEvents) > tm.auditEventLimit {
		dropCount := len(tm.auditEvents) - tm.auditEventLimit
		dropped := make([]AdminAuditEvent, dropCount)
		copy(dropped, tm.auditEvents[:dropCount])
		archived = append(archived, dropped...)
		tm.auditEvents = tm.auditEvents[dropCount:]
	}

	tm.auditEventsMu.Unlock()

	tm.archiveAuditEvents(archived)
	tm.persistAdminAuditEvent(ctx, event)
}

func (tm *TaskManager) persistAdminAuditEvent(ctx context.Context, event AdminAuditEvent) {
	if tm == nil {
		return
	}

	backend, err := tm.adminBackend()
	if err != nil || backend == nil {
		return
	}

	if ctx == nil {
		return
	}

	persistBaseCtx := context.WithoutCancel(ctx)

	persistCtx, cancel := context.WithTimeout(persistBaseCtx, adminAuditPersistTimeout)
	defer cancel()

	persistErr := backend.AdminRecordAuditEvent(persistCtx, event, tm.auditEventLimit)
	if persistErr != nil {
		return
	}
}

func (tm *TaskManager) auditEventsFromBackend(
	ctx context.Context,
	filter AdminAuditEventFilter,
) (AdminAuditEventPage, bool, error) {
	backend, err := tm.adminBackend()
	if err != nil {
		if errors.Is(err, ErrAdminBackendUnavailable) {
			return AdminAuditEventPage{}, false, nil
		}

		return AdminAuditEventPage{}, false, err
	}

	if backend == nil {
		return AdminAuditEventPage{}, false, nil
	}

	page, fetchErr := backend.AdminAuditEvents(ctx, filter)
	if fetchErr == nil {
		page = tm.filterAuditEventsByAge(page)

		return page, true, nil
	}

	if errors.Is(fetchErr, ErrAdminUnsupported) {
		return AdminAuditEventPage{}, false, nil
	}

	return AdminAuditEventPage{}, false, fetchErr
}

func (tm *TaskManager) auditEventsFromMemory(filter AdminAuditEventFilter) AdminAuditEventPage {
	tm.auditEventsMu.RLock()
	events := make([]AdminAuditEvent, len(tm.auditEvents))
	copy(events, tm.auditEvents)
	tm.auditEventsMu.RUnlock()

	limit := normalizeAdminEventLimit(filter.Limit, tm.auditEventLimit)
	action := strings.TrimSpace(filter.Action)
	target := strings.TrimSpace(filter.Target)
	cutoff, hasCutoff := tm.auditRetentionCutoff(time.Now())

	filtered := make([]AdminAuditEvent, 0, min(limit, len(events)))
	for i := len(events) - 1; i >= 0 && len(filtered) < limit; i-- {
		event := events[i]
		if hasCutoff && event.At.Before(cutoff) {
			continue
		}

		if action != "" && event.Action != action {
			continue
		}

		if target != "" && event.Target != target {
			continue
		}

		filtered = append(filtered, event)
	}

	return AdminAuditEventPage{Events: filtered}
}

func (tm *TaskManager) auditRetentionCutoff(now time.Time) (time.Time, bool) {
	if tm == nil || tm.auditRetention <= 0 {
		return time.Time{}, false
	}

	return now.Add(-tm.auditRetention), true
}

func (tm *TaskManager) pruneAuditEventsByAgeLocked(now time.Time) []AdminAuditEvent {
	cutoff, ok := tm.auditRetentionCutoff(now)
	if !ok || len(tm.auditEvents) == 0 {
		return nil
	}

	firstKeep := 0
	for firstKeep < len(tm.auditEvents) && tm.auditEvents[firstKeep].At.Before(cutoff) {
		firstKeep++
	}

	if firstKeep == 0 {
		return nil
	}

	archived := make([]AdminAuditEvent, firstKeep)
	copy(archived, tm.auditEvents[:firstKeep])

	if firstKeep >= len(tm.auditEvents) {
		tm.auditEvents = tm.auditEvents[:0]

		return archived
	}

	tm.auditEvents = tm.auditEvents[firstKeep:]

	return archived
}

func (tm *TaskManager) filterAuditEventsByAge(page AdminAuditEventPage) AdminAuditEventPage {
	cutoff, ok := tm.auditRetentionCutoff(time.Now())
	if !ok || len(page.Events) == 0 {
		return page
	}

	filtered := make([]AdminAuditEvent, 0, len(page.Events))
	for _, event := range page.Events {
		if event.At.Before(cutoff) {
			continue
		}

		filtered = append(filtered, event)
	}

	page.Events = filtered

	return page
}
