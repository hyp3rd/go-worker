package worker

import (
	"context"
	"crypto/subtle"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
)

const (
	adminApprovalMetaKey          = "x-admin-approval"
	defaultAdminReplayLimitMax    = 1000
	defaultAdminReplayIDsMax      = 1000
	defaultAdminScheduleRunMax    = 30
	defaultAdminScheduleRunWindow = time.Minute
)

// AdminGuardrails configures safety limits for high-impact admin operations.
type AdminGuardrails struct {
	ReplayLimitMax    int
	ReplayIDsMax      int
	ScheduleRunMax    int
	ScheduleRunWindow time.Duration
	RequireApproval   bool
	ApprovalToken     string
}

type adminScheduleRunLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	records map[string][]time.Time
}

func normalizeAdminGuardrails(cfg AdminGuardrails) AdminGuardrails {
	if cfg.ReplayLimitMax <= 0 {
		cfg.ReplayLimitMax = defaultAdminReplayLimitMax
	}

	if cfg.ReplayIDsMax <= 0 {
		cfg.ReplayIDsMax = defaultAdminReplayIDsMax
	}

	if cfg.ScheduleRunMax <= 0 {
		cfg.ScheduleRunMax = defaultAdminScheduleRunMax
	}

	if cfg.ScheduleRunWindow <= 0 {
		cfg.ScheduleRunWindow = defaultAdminScheduleRunWindow
	}

	cfg.ApprovalToken = strings.TrimSpace(cfg.ApprovalToken)

	return cfg
}

func newAdminScheduleRunLimiter(cfg AdminGuardrails) *adminScheduleRunLimiter {
	if cfg.ScheduleRunMax <= 0 || cfg.ScheduleRunWindow <= 0 {
		return nil
	}

	return &adminScheduleRunLimiter{
		limit:   cfg.ScheduleRunMax,
		window:  cfg.ScheduleRunWindow,
		records: make(map[string][]time.Time),
	}
}

func (limiter *adminScheduleRunLimiter) allow(name string, now time.Time) bool {
	if limiter == nil {
		return true
	}

	name = strings.TrimSpace(name)
	if name == "" {
		name = "*"
	}

	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	windowStart := now.Add(-limiter.window)
	current := limiter.records[name]

	kept := make([]time.Time, 0, len(current)+1)
	for _, ts := range current {
		if ts.After(windowStart) {
			kept = append(kept, ts)
		}
	}

	if len(kept) >= limiter.limit {
		limiter.records[name] = kept

		return false
	}

	kept = append(kept, now)
	limiter.records[name] = kept

	return true
}

func (s *GRPCServer) enforceAdminApproval(ctx context.Context) error {
	if s == nil {
		return nil
	}

	if !s.adminGuardrails.RequireApproval {
		return nil
	}

	token := adminApprovalTokenFromContext(ctx)
	if token == "" {
		return ErrAdminApprovalRequired
	}

	expected := s.adminGuardrails.ApprovalToken
	if expected == "" {
		return nil
	}

	if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		return ErrAdminApprovalInvalid
	}

	return nil
}

func (s *GRPCServer) enforceReplayLimit(limit int) error {
	if s == nil {
		return nil
	}

	if limit <= 0 {
		limit = defaultAdminDLQLimit
	}

	maxLimit := s.adminGuardrails.ReplayLimitMax
	if maxLimit > 0 && limit > maxLimit {
		return ErrAdminReplayLimitExceeded
	}

	return nil
}

func (s *GRPCServer) enforceReplayIDsLimit(ids []string) error {
	if s == nil {
		return nil
	}

	maxIDs := s.adminGuardrails.ReplayIDsMax
	if maxIDs > 0 && len(ids) > maxIDs {
		return ErrAdminReplayIDsTooLarge
	}

	return nil
}

func (s *GRPCServer) enforceScheduleRun(name string) error {
	if s == nil || s.scheduleRunLimiter == nil {
		return nil
	}

	if s.scheduleRunLimiter.allow(name, time.Now()) {
		return nil
	}

	return ErrAdminScheduleRunRateLimited
}

func adminApprovalTokenFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	values := meta.Get(adminApprovalMetaKey)
	if len(values) == 0 {
		return ""
	}

	return strings.TrimSpace(values[0])
}
