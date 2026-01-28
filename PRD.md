# PRD: go-worker Production Hardening & Performance

## Background

`go-worker` runs in sensitive production environments (international banking and medical institutions). Reliability, correctness, predictable performance, and safe shutdown behavior are required. The current implementation exhibits lifecycle, concurrency, and rate-limiting inconsistencies that can lead to task loss, unexpected retries, goroutine leaks, and panics.

## Goals

- Provide correct, deterministic task lifecycle semantics (queued → running → terminal) with no duplicate executions.
- Enforce rate limits and concurrency caps precisely and predictably.
- Ensure cancellation, timeouts, retries, and shutdown are safe, idempotent, and leak-free.
- Make behavior observable (metrics, logs, tracing) with minimal overhead.
- Keep API ergonomics while enabling strong operational controls.

## Non-Goals

- Implementing a durable persistent queue (can be a future extension).
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
         - `Stop()` must be idempotent and non-blocking.
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
- Replace `GetResults()` with either:
         - a) `Results()` returning a receive-only channel, or
         - b) `CollectResults(ctx)` that safely drains results without closing channels used by workers.
- Provide `StopGraceful(ctx)` and `StopNow()` to separate drain vs cancel semantics.

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

## Milestones

1. **Stability Patch**: Fix critical panics, timeout propagation, cancellation correctness, and shutdown deadlocks.
1. **Scheduling & Retry Refactor**: Unify scheduling path, correct rate limiting, non-blocking retry strategy.
1. **Observability & Metrics**: Add metrics and structured logging hooks.
1. **gRPC Enhancements**: Fan-out results, idempotency, better error codes.
1. **Memory & Retention**: Task registry retention policy and cleanup.

## Acceptance Criteria

- All unit tests pass; new tests cover cancellation, retries, shutdown, rate limiting, and fan-out streaming.
- `go test -race ./...` passes with no data races.
- `Stop()` returns within a bounded timeout even with pending tasks.
- No panics in `GetTask`, middleware logging, or streaming when tasks have no result/error yet.
- Task execution respects deadlines; canceled tasks do not execute.

## Competitive Landscape & Gaps

### Comparable Go packages

- **Worker pools** (concurrency focus): `ants`, `pond`, `tunny`. These provide fast pool management but lack task lifecycle, retries, hooks, and results fan‑out.
- **Distributed/durable queues**: `river` (Postgres‑backed) and `gocraft/work` (Redis‑backed). These provide persistence, multi‑node coordination, and UIs, but are heavier and require external infrastructure.

### Gaps vs comparable packages

- **Durability**: no persisted queue, no transactional enqueue, no guaranteed recovery after process restart.
- **Operational tooling**: no built‑in admin UI, dashboards, or DLQ replay tooling.
- **Distributed coordination**: no broker/queue backend or multi‑node worker coordination.
- **Scheduling**: no cron‑like schedules or delayed tasks beyond retries.

### Nice‑to‑have improvements

- **DLQ & replay** for terminal failures.
- **Queue segmentation** (multiple named queues, weighted priorities).
- **Typed task payloads** (optional typed registry, stronger compile‑time checks).
- **More observability knobs** (labels for task name/status; exemplar support).
- **Operational guidance** (sizing/rate‑limit recommendations, best‑practice defaults).

### Roadmap (future milestones)

1. **Durable backend**: pluggable persistence (Postgres/Redis) with transactional enqueue.
1. **Operational tooling**: admin UI + CLI for queue inspection, retries, and DLQ.
1. **Scheduled jobs**: cron/delayed scheduling layer.
1. **Multi‑node coordination**: optional distributed workers via backend.
