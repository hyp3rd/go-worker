# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

Admin surface (feat/admin-ui, 67 commits):

- Admin HTTP gateway (`worker-admin` binary) fronting the gRPC `AdminService` with mTLS; no plaintext HTTP mode.
- Admin UI (Next.js / React 19) with overview, queues, schedules, jobs, DLQ, audit, and a unified operations timeline view.
- gRPC `AdminService` covering queues, schedules, jobs, DLQ, audit, and coordination state.
- Queue controls: list, get, pause/resume, weight update and reset.
- DLQ: pagination, filters (queue/handler/query), entry detail, bulk replay, replay by IDs.
- Schedule management: create, delete, pause, run-now; schedule event history persisted via the durable backend so it is shared across worker instances.
- Container job system: CRUD API, one-off run, artifact metadata + download, Git-tag / HTTPS tarball / local tarball sources, crash-test preset.
- Audit events with archival to disk and export as JSONL / JSON / CSV.
- Prometheus metrics endpoint (`/admin/v1/metrics/prometheus`) alongside the JSON metrics snapshot.
- Admin action counters (pause / resume / replay) surfaced via `GET /overview`.
- Approval header `X-Admin-Approval` propagated as gRPC metadata `x-admin-approval` on destructive endpoints: DLQ replay, DLQ replay-by-ID, schedule run, job run, queue pause (`/queues/{name}/pause`), schedule pause (`/schedules/{name}/pause`), and schedules pause-all (`/schedules/pause`).
- Server-Sent Events stream at `/admin/v1/events` with 300-event replay buffer and `Last-Event-ID` resume support.
- Request ID header `X-Request-Id` sanitized to `[A-Za-z0-9_.-]`, truncated to 128 chars, echoed in responses, and propagated as gRPC metadata for end-to-end tracing.
- Persistent file-backed job event store with TTL cache; admin UI reads the same history across restarts.
- Structured API error diagnostics envelope (`{requestId, error: {code, message}}`).
- `workerctl` gains a `version` command and dedicated release workflow; Makefile build target added.
- Tests: `admin_gateway_test.go` (HTTP handler coverage across all routes, ~28 functions + ~23 sub-tests), `tests/durable_redis_admin_integration_test.go` (9 admin-backend integration tests), `admin_schedule_events_test.go`, `admin_audit_archive_test.go`, `admin_job_event_store_test.go`.
- Docs: "Admin API & UI" section in README, and expanded `docs/admin-ui.md` covering HTTP endpoint table, SSE event schema, header reference, mTLS setup, and gateway env vars.

Pre-admin (earlier work in this release cycle):

- gRPC auth hook via `WithGRPCAuth`.
- Task tracing hooks via `TaskTracer`.
- OpenTelemetry metrics via `SetMeterProvider`.
- OpenTelemetry tracing example and OTLP metrics example.
- Result fan-out examples for OTel metrics and tracing.
- Compatibility shim `GetResults()` for legacy callers.
- Retention load and OTel metrics wiring tests.
- Benchmarks for registration throughput and retention pruning.
- Durable task APIs with a Redis backend (rueidis).
- Durable lease renewal via `WithDurableLeaseRenewalInterval`.
- `NewTaskManagerWithOptions` for functional configuration.
- gRPC durable task registration via `RegisterDurableTasks`.
- Durable Redis examples and tooling: `__examples/durable_redis`, `__examples/grpc_durable`, `__examples/durable_dlq_replay`, `__examples/durable_queue_inspect`.

### Changed

- Cron migrated from `robfig/cron/v3` to `hyp3rd/cron/v4` (internal fork; external API unchanged for consumers of `TaskManager` cron helpers).
- Timeouts standardized across worker and `workerctl`; cancellation paths tightened.
- Go toolchain bumped to 1.26.2.
- OpenTelemetry SDK updated through v1.43.0.
- Rate limiter burst now defaults to `min(maxWorkers, maxTasks)` for deterministic throttling.
- `ExecuteTask` respects the task rate limiter (may wait or return context errors).
- Breaking: `DurableBackend` now requires `Extend` for lease renewal.
- Breaking: `Stop()` removed; use `StopGraceful(ctx)` or `StopNow()`.
- Breaking: `SubscribeResults` replaces `GetResults()`/`StreamResults()` (shim restored for legacy).
- Breaking: `RegisterTasks` now returns an error.
- Breaking: `RegisterTask(s)` are disabled when a durable backend is enabled (use `RegisterDurableTask(s)`).
- gRPC: new `DurableTask` and `RegisterDurableTasks` for persisted tasks (existing RPCs unchanged).

### Fixed

- Duplicate durable cron registrations are now treated as idempotent instead of returning an error.
- Durable backend records task failure before the lease is finished, preventing a race where the failure state could be lost.
- Admin events stream gracefully handles client disconnects without leaking goroutines.
- Confirm dialog accessibility and job event store concurrency safety in the admin UI.
- React 19 `react-hooks/set-state-in-effect` warnings in the admin UI: redundant prop-mirror effects removed, filter-reset effects converted to the React 19 "reset state during render" pattern, queue pause now uses `useOptimistic`.

### Security

- Request IDs sanitized and truncated before being written to logs or forwarded as gRPC metadata, preventing log/metadata injection via `X-Request-Id`.
- Tarball URL validation hardened on the job artifact fetch path (scheme enforcement, host allowlist).
- mTLS required on the admin gateway (client certs verified against a configurable CA); gateway refuses to start without cert files.

## [v0.1.1] - 2025-08-18

### Added (v0.1.1)

- docs: expand README with architecture and community guidance by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/9>
- feat(api): introduce proto tooling and update Task/Worker interfaces for context support by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/11>
- Add gRPC task service with streaming results by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/12>
- feat: enhance gRPC service and add example by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/13>
- feat: expand gRPC task definition by @hyp3rd in <https://github.com/hyp3rd/go-worker/pull/14>

**Full Changelog**: <https://github.com/hyp3rd/go-worker/compare/v0.1.0...v0.1.1>

## [v0.1.0] - 2025-08-16

### Added (v0.1.0)

- Initial release of go-worker with prioritized concurrent task execution.
