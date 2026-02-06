# PRD: go-worker Production Hardening & Performance

## Background

`go-worker` runs in sensitive production environments (international banking and medical institutions). Reliability, correctness, predictable performance, and safe shutdown behavior are required. The current implementation exhibits lifecycle, concurrency, and rate-limiting inconsistencies that can lead to task loss, unexpected retries, goroutine leaks, and panics.

## Goals

- Provide correct, deterministic task lifecycle semantics (queued → running → terminal) with no duplicate executions.
- Enforce rate limits and concurrency caps precisely and predictably.
- Ensure cancellation, timeouts, retries, and shutdown are safe, idempotent, and leak-free.
- Make behavior observable (metrics, logs, tracing) with minimal overhead.
- Keep API ergonomics while enabling strong operational controls.
- Provide an optional durable backend (Redis) with at-least-once delivery, leasing, and DLQ support.

## Non-Goals

- Additional durable backends beyond Redis (e.g., Postgres) are out of scope for now.
- Building a full scheduler with cron semantics.
- Providing a multi-tenant authorization system inside the library.

## Assumptions

- The library should remain usable as an embedded component (not a standalone service).
- gRPC is an optional interface; the core worker should not depend on it.

## Users / Use Cases

- Internal services enqueue critical tasks with strict SLAs.
- Operators need to safely drain tasks for deploys or incidents.
- Compliance teams need traceability and reliable audit data for tasks.

## Functional Requirements

1. **Task lifecycle correctness**
         - Exactly-once execution per task registration (no duplicate dispatch).
         - Terminal state is immutable once reached.
         - Task status transitions are thread-safe and race-free.
1. **Rate limiting and concurrency control**
         - Single, consistent limiter for execution (not double-applied).
         - Support low values (including 0 retries, 1 worker, and low TPS).
         - Burst handling should be deterministic and configurable.
1. **Timeouts & cancellation**
         - Task execution must receive the task’s context (with timeout/deadline).
         - Cancellation should prevent execution or stop in-flight tasks.
         - Cancellation should not reschedule tasks unless explicitly requested.
1. **Retries**
         - Allow zero retries (disabled) and configurable max retries.
         - Implement exponential backoff with jitter without blocking shared locks.
         - Retries must not deadlock or block the scheduler.
1. **Shutdown / drain semantics**
         - `StopGraceful(ctx)` and `StopNow()` must be idempotent and non-blocking.
         - Support graceful drain (finish running tasks) and hard stop (cancel).
         - No channel closes while writers are active.
1. **Result handling**
         - Provide safe, multi-subscriber result streams (fan-out).
         - Allow bounded buffer size with backpressure strategy.
1. **Registry management**
         - Provide optional retention policy (TTL or max entries) to avoid unbounded memory.
1. **gRPC behavior**
         - Per-client streaming should not steal results from other clients.
         - Idempotency keys should be enforced (optional) with conflict response.
         - Surface NotFound when canceling/querying unknown tasks.

## Non-Functional Requirements

- **Safety**: No panics on normal operation; no data races under `-race`.
- **Reliability**: No goroutine leaks; `Stop()` always completes.
- **Performance**: Minimum overhead per task; no global locks in steady state.
- **Observability**: Provide counters and histograms for queue depth, latency, success/failure, retry counts, and cancellations.
- **Compatibility**: Preserve public API where possible; breaking changes are versioned.

## API / Behavior Changes (Proposed)

- Replace dual scheduling (scheduler + direct channel send) with a single scheduling path.
- Clarify defaults using typed `time.Duration` constants (e.g., `5 * time.Minute`).
- Replace `GetResults()` with `SubscribeResults(buffer)` and keep `GetResults()` as a compatibility shim for one release cycle.
- Provide `StopGraceful(ctx)` and `StopNow()` to separate drain vs cancel semantics.
- When a durable backend is enabled, use `RegisterDurableTask(s)` instead of `RegisterTask(s)`.

## Observability Requirements

- Metrics (Prometheus or OpenTelemetry):
         - tasks_scheduled_total
         - tasks_running
         - tasks_completed_total
         - tasks_failed_total
         - tasks_cancelled_total
         - task_latency_seconds (histogram)
         - queue_depth
         - retry_count_total
- Structured logging hooks (interface-based, no stdlib log dependency).
- Optional tracing spans per task execution.

## Security & Compliance

- gRPC examples should demonstrate TLS and interceptor usage.
- Optional hooks for authentication/authorization in gRPC server.
- Avoid leaking sensitive payloads in logs by default.

## Migration Plan

- Introduce new API variants while keeping existing functions deprecated for one release cycle.
- Provide a compatibility shim for old `GetResults()` behavior.
- Document breaking changes and safe upgrade path.

## Status vs Repo (January 30, 2026)

Status updated: **_February 5, 2026_**

### Functional Requirements (January 30, 2026)

- **Task lifecycle correctness**: Done for in-memory tasks; durable tasks are at-least-once by design.
- **Rate limiting & concurrency**: Done; single scheduling path with deterministic burst (`min(maxWorkers, maxTasks)`).
- **Timeouts & cancellation**: Done; task contexts inherit deadlines and cancellation is propagated.
- **Retries**: Done; exponential backoff with jitter and non-blocking scheduling.
- **Shutdown / drain semantics**: Done; `StopGraceful` + `StopNow` are idempotent and safe.
- **Result handling**: Done; fan-out `SubscribeResults` + drop policy + `GetResults` shim.
- **Registry management**: Done; retention policy and cleanup implemented.
- **gRPC behavior**: Done; fan-out streaming, idempotency enforcement, NotFound for missing tasks.

### Non-Functional Requirements (January 30, 2026)

- **Safety**: Partial; panic recovery in hooks/tracer/broadcaster exists, but `go test -race ./...` still needs to be validated on your machine.
- **Safety**: Partial; panic recovery in hooks/tracer/broadcaster exists, but `go test -race ./...` still needs to be validated on your machine (race fix landed in `resultBroadcaster`).
- **Reliability**: Done; stop paths are idempotent and workers/scheduler loops are tracked via WaitGroups.
- **Performance**: Done; queue scheduling avoids global locks in steady state beyond the queue/registry.
- **Observability**: Done; metrics snapshot + OpenTelemetry metrics/tracing and examples.
- **Compatibility**: Done; breaking changes documented, shim for `GetResults`.

### API / Behavior Changes (January 30, 2026)

- **Single scheduling path**: Done.
- **Defaults clarified**: Done (typed `time.Duration` constants).
- **`GetResults()` replacement**: Done as `SubscribeResults` + compatibility shim.
- **StopGraceful/StopNow**: Done.
- **Durable mode registration**: Done; `RegisterTask(s)` disabled when durable backend is enabled.

### Security & Compliance (January 30, 2026)

- **TLS/interceptor examples**: Done (`__examples/grpc`).
- **Auth hook**: Done (`WithGRPCAuth`).
- **Avoid logging payloads by default**: Partial; core avoids logging payloads, but examples/tests should be reviewed for sensitive logging.

### Durable Backend

- **Redis durable backend**: Done (leases, DLQ, replay/inspect tools, gRPC durable API).
- **Multi-backend support**: Not started.
- **Global coordination**: Done; global rate limiting (`WithRedisDurableGlobalRateLimit`) and leader lock (`WithRedisDurableLeaderLock`) added with tests and docs.

### Admin Service & Admin UI

- **Admin Service (gRPC + HTTP gateway)**: Done (mTLS required, request IDs, timeouts, consistent error envelope, queue detail, DLQ pagination/filters, schedules, health/version).
- **Admin UI (Next.js + Tailwind)**: Done (overview/queues/DLQ/schedules, queue detail page, server-side DLQ paging/filters, runbook actions, session auth, audit banner, replay‑by‑ID selection).
- **Docker/Compose**: Done (`Dockerfile` + `compose.admin.yaml` + cert generation script).
- **Schedule management**: Done (create/delete/pause via gateway + UI).
- **Admin action counters**: Done (pause/resume/replay counts exposed in overview).
- **UI polish**: In progress (settings metadata panel added; continued UX refinements ongoing).

## Milestones

1. **Stability Patch**: Fix critical panics, timeout propagation, cancellation correctness, and shutdown deadlocks.
1. **Scheduling & Retry Refactor**: Unify scheduling path, correct rate limiting, non-blocking retry strategy.
1. **Observability & Metrics**: Add metrics and structured logging hooks.
1. **gRPC Enhancements**: Fan-out results, idempotency, better error codes.
1. **Memory & Retention**: Task registry retention policy and cleanup.

## Acceptance Criteria

- All unit tests pass; new tests cover cancellation, retries, shutdown, rate limiting, and fan-out streaming.
- `go test -race ./...` passes with no data races.
- `StopGraceful(ctx)` returns within a bounded timeout even with pending tasks.
- No panics in `GetTask`, middleware logging, or streaming when tasks have no result/error yet.
- Task execution respects deadlines; canceled tasks do not execute.

## Competitive Landscape & Gaps

### Comparable Go packages

- **Worker pools** (concurrency focus): `ants`, `pond`, `tunny`. These provide fast pool management but lack task lifecycle, retries, hooks, and results fan‑out.
- **Distributed/durable queues**: `river` (Postgres‑backed) and `gocraft/work` (Redis‑backed). These provide persistence, multi‑node coordination, and UIs, but are heavier and require external infrastructure.

### Gaps vs comparable packages

- **Durability**: Redis-backed at-least-once exists, but no transactional enqueue across services and no exactly-once guarantees.
- **Operational tooling**: `workerctl` CLI exists for queue inspection and DLQ replay; admin UI and admin service are now implemented (ongoing polish and feature depth).
- **Distributed coordination**: multi-node guidance + lease renewal exist; global rate limiting and leader lock now exist; no quorum controls.
- **Scheduling**: delayed scheduling and cron are implemented; UX and admin controls still need refinement.

### Nice‑to‑have improvements

- **DLQ & replay**: current replay utility is basic; add safety guards, dry-run, and filtering.
- **Queue segmentation** (multiple named queues, weighted priorities).
- **Typed task payloads** (optional typed registry, stronger compile‑time checks).
- **More observability knobs** (labels for task name/status; exemplar support).
- **Operational guidance** (sizing/rate‑limit recommendations, best‑practice defaults).

### Roadmap (future milestones)

1. **Durable backend**: add additional backends (Postgres) and stronger transactional enqueue semantics.
1. **Operational tooling**: admin UI and admin service implemented; expand CLI release workflow and deep admin features (filters, export, advanced replay).
1. **Scheduled jobs**: cron/delayed scheduling layer (cron now implemented; cron UX improvements TBD).
1. **Multi‑node coordination**: optional distributed workers via backend (global rate limit/leader lock implemented; quorum/leader election improvements TBD).
