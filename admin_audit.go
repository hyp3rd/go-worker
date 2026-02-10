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
	if tm.auditEventLimit > 0 && len(tm.auditEvents) > tm.auditEventLimit {
		tm.auditEvents = tm.auditEvents[len(tm.auditEvents)-tm.auditEventLimit:]
	}

	tm.auditEventsMu.Unlock()

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

	filtered := make([]AdminAuditEvent, 0, min(limit, len(events)))
	for i := len(events) - 1; i >= 0 && len(filtered) < limit; i-- {
		event := events[i]
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
