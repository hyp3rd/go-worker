# PRD: Worker Admin Service (gRPC + HTTP Gateway)

## Background

The admin UI needs a backend-agnostic control plane. Direct Redis access is a dead-end once multiple durable backends exist. We will expose a gRPC AdminService implemented by the worker, and a HTTP/JSON gateway for the admin UI. The gateway uses mutual TLS (mTLS) for transport authentication.

## Goals

- Provide a stable admin contract independent of the durable backend implementation.
- Allow the Next.js admin UI to read operational data and trigger safe actions.
- Support mTLS authentication between UI and the gateway.
- Keep AdminService deployable alongside the worker in the same process.

## Non-Goals

- Public multi-tenant auth/authorization (beyond mTLS).
- Full RBAC and audit logging (future work).
- WebSocket/bi-directional streaming beyond SSE (future work).

## Functional Requirements

1. **Overview data**
         - Return queue counts, processing counts, and DLQ size.
         - Return coordination state (global rate limit, leader lock, pause state).
1. **Queue data**
         - Return per-queue ready/processing/dead counts and weights.
         - Return queue detail by name (single-queue view).
         - Allow updating queue weight and resetting to default.
         - Allow pausing/resuming a queue (durable dequeue only).
1. **Schedules**
         - List cron schedules with spec and next/previous run.
         - Create/update schedules by name (requires a registered factory).
         - Pause/resume schedules without unregistering the factory.
         - Delete schedules by name.
         - Run schedule immediately on demand.
         - Pause/resume all schedules at once.
1. **Jobs (containerized runner)**
         - Persist job definitions (repo + tag + optional path/dockerfile).
         - Support sources: Git tag, HTTPS tarball URL, or local tarball path.
         - Validate tarball SHA256 when provided and enforce allowlists.
         - Run jobs on demand (enqueue durable task).
         - Register job factories to allow scheduling via existing cron APIs.
         - Enforce tag-only Git fetch (no branches).
         - Truncate output to a configured max bytes for safety.
1. **Health**
         - Report service health and build information (version, commit, Go version).
1. **DLQ**
         - List DLQ entries (bounded, paginated by limit/offset, with optional filters).
         - Fetch DLQ entry detail by ID (metadata, error, payload size, timestamps).
         - Replay DLQ entries (bounded limit).
1. **Actions**
         - Pause durable dequeue.
         - Resume durable dequeue.
1. **Events**
         - Stream overview + schedule + job events to power the UI event log.
         - Persist job events across restarts when a store is configured.
1. **Backend abstraction**
         - Admin API should not depend on Redis directly; use backend interface.

## Non-Functional Requirements

- **Security**: mTLS required for gateway; server certificate validated by client.
- **Reliability**: idempotent actions and bounded timeouts.
- **Performance**: use batched Redis commands (if Redis backend is used).
- **Compatibility**: API versioned as `v1`.

## API Contract (v1)

### gRPC

Service: `worker.v1.AdminService`

- `GetOverview(GetOverviewRequest) returns (GetOverviewResponse)`
- `GetHealth(GetHealthRequest) returns (GetHealthResponse)`
- `ListQueues(ListQueuesRequest) returns (ListQueuesResponse)`
- `GetQueue(GetQueueRequest) returns (GetQueueResponse)`
- `PauseQueue(PauseQueueRequest) returns (PauseQueueResponse)`
- `ListScheduleFactories(ListScheduleFactoriesRequest) returns (ListScheduleFactoriesResponse)`
- `ListSchedules(ListSchedulesRequest) returns (ListSchedulesResponse)`
- `CreateSchedule(CreateScheduleRequest) returns (CreateScheduleResponse)`
- `DeleteSchedule(DeleteScheduleRequest) returns (DeleteScheduleResponse)`
- `PauseSchedule(PauseScheduleRequest) returns (PauseScheduleResponse)`
- `RunSchedule(RunScheduleRequest) returns (RunScheduleResponse)`
- `PauseSchedules(PauseSchedulesRequest) returns (PauseSchedulesResponse)`
- `ListJobs(ListJobsRequest) returns (ListJobsResponse)`
- `GetJob(GetJobRequest) returns (GetJobResponse)`
- `UpsertJob(UpsertJobRequest) returns (UpsertJobResponse)`
- `DeleteJob(DeleteJobRequest) returns (DeleteJobResponse)`
- `RunJob(RunJobRequest) returns (RunJobResponse)`
- `ListDLQ(ListDLQRequest) returns (ListDLQResponse)`
- `GetDLQEntry(GetDLQEntryRequest) returns (GetDLQEntryResponse)`
- `PauseDequeue(PauseDequeueRequest) returns (PauseDequeueResponse)`
- `ResumeDequeue(ResumeDequeueRequest) returns (ResumeDequeueResponse)`
- `ReplayDLQ(ReplayDLQRequest) returns (ReplayDLQResponse)`
- `ReplayDLQByID(ReplayDLQByIDRequest) returns (ReplayDLQByIDResponse)`
- `StreamEvents(StreamEventsRequest) returns (stream AdminEvent)` (gateway uses SSE)

### HTTP Gateway

- `GET /admin/v1/health`
- `GET /admin/v1/overview`
- `GET /admin/v1/queues`
- `GET /admin/v1/queues/{name}`
- `POST /admin/v1/queues/{name}/weight` (body: `{ "weight": 3 }`)
- `DELETE /admin/v1/queues/{name}/weight`
- `POST /admin/v1/queues/{name}/pause` (body: `{ "paused": true }`)
- `GET /admin/v1/schedules/factories`
- `GET /admin/v1/schedules`
- `GET /admin/v1/schedules/events?name=schedule_name&limit=25`
- `POST /admin/v1/schedules` (body: `{ "name": "...", "spec": "...", "durable": false }`)
- `DELETE /admin/v1/schedules/{name}`
- `POST /admin/v1/schedules/{name}/pause` (body: `{ "paused": true }`)
- `POST /admin/v1/schedules/{name}/run`
- `POST /admin/v1/schedules/pause` (body: `{ "paused": true }`)
- `GET /admin/v1/jobs`
- `GET /admin/v1/jobs/{name}`
- `POST /admin/v1/jobs` (body: `{ "name": "...", "description": "...", "source": "git_tag|tarball_url|tarball_path", "repo": "...", "tag": "v1.2.3", "tarballUrl": "...", "tarballPath": "...", "tarballSha256": "...", "path": "subdir", "dockerfile": "Dockerfile", "command": ["..."], "env": ["KEY=VALUE"], "queue": "default", "retries": 2, "timeoutSeconds": 600 }`)
- `DELETE /admin/v1/jobs/{name}`
- `POST /admin/v1/jobs/{name}/run`
- `GET /admin/v1/jobs/events?name=job_name&limit=25`
- `GET /admin/v1/dlq?limit=100&offset=0&queue=default&handler=send_email&query=oops`
- `GET /admin/v1/dlq/{id}`
- `GET /admin/v1/audit?limit=100&action=queue.pause&target=default`
- `GET /admin/v1/audit/export?format=jsonl|json|csv&limit=1000&action=...&target=...`
- `POST /admin/v1/pause`
- `POST /admin/v1/resume`
- `POST /admin/v1/dlq/replay` (body: `{ "limit": 100 }`)
- `POST /admin/v1/dlq/replay/ids` (body: `{ "ids": ["..."] }`)
- `GET /admin/v1/events` (SSE)

## Security

- mTLS required on the gateway listener.
- Certificates provided via env:
      - `WORKER_ADMIN_TLS_CERT`
      - `WORKER_ADMIN_TLS_KEY`
      - `WORKER_ADMIN_TLS_CA`
- Gateway does not accept plaintext connections.

## Configuration

- `WORKER_ADMIN_GRPC_ADDR` (default `127.0.0.1:50052`)
- `WORKER_ADMIN_GRPC_TARGET` (optional; when set, the gateway dials this address instead of starting a local AdminService)
- `WORKER_ADMIN_HTTP_ADDR` (default `127.0.0.1:8081`)
- `WORKER_ADMIN_TLS_CERT`, `WORKER_ADMIN_TLS_KEY`, `WORKER_ADMIN_TLS_CA`
- `WORKER_ADMIN_AUDIT_EVENT_LIMIT` (max audit events retained by service/backend list operations)
- `WORKER_ADMIN_AUDIT_RETENTION` (optional max age, e.g. `168h`; events older than cutoff are pruned/filtered)
- `WORKER_ADMIN_AUDIT_ARCHIVE_DIR` (optional directory for archived audit JSONL files)
- `WORKER_ADMIN_AUDIT_ARCHIVE_INTERVAL` (optional flush interval for archival writer)
- `WORKER_ADMIN_AUDIT_EXPORT_LIMIT_MAX` (gateway cap for export query `limit`)
- `WORKER_ADMIN_DEFAULT_QUEUE`, `WORKER_ADMIN_QUEUE_WEIGHTS` (backend defaults)

Worker service cron presets (when using the bundled worker-service):

- `WORKER_DURABLE_CRON_HANDLERS` (comma-separated handler names to register)
- `WORKER_DURABLE_CRON_PRESETS` (comma-separated `name=spec` overrides)
- `WORKER_CRON_HANDLERS` (comma-separated in-memory handlers)
- `WORKER_CRON_PRESETS` (comma-separated `name=spec` overrides)

Worker service job runner (containerized):

- `WORKER_JOB_REPO_ALLOWLIST` (comma-separated; `*` to allow all)
- `WORKER_JOB_GIT_BIN` (default `git`)
- `WORKER_JOB_DOCKER_BIN` (default `docker`)
- `WORKER_JOB_NETWORK` (optional docker network)
- `WORKER_JOB_WORKDIR` (optional temp workspace root)
- `WORKER_JOB_OUTPUT_BYTES` (max combined stdout+stderr)
- `WORKER_JOB_TARBALL_ALLOWLIST` (comma-separated hostnames for HTTPS tarballs)
- `WORKER_JOB_TARBALL_DIR` (root for local tarball paths)
- `WORKER_JOB_TARBALL_MAX_BYTES` (default 64MiB)
- `WORKER_JOB_TARBALL_TIMEOUT` (default 30s)
- `WORKER_JOB_EVENT_DIR` (file-backed job event store root; enables persistence)
- `WORKER_JOB_EVENT_MAX_ENTRIES` (per key; default 10000)
- `WORKER_JOB_EVENT_CACHE_TTL` (default 10s)

## Observability

- Log errors and action outcomes at INFO/WARN levels.
- Add counters for admin actions (pause/resume/replay).

## Implementation Status (February 10, 2026)

### Service (gRPC + gateway)

- **Implemented**
        - Overview, health, queues (list/get/weight/pause), schedules (list/factories/create/delete/pause/run/pause-all), jobs (list/get/upsert/delete/run), DLQ (list/detail/replay/replay-by-id), pause/resume dequeue, SSE events.
        - Containerized job runner supports `git_tag`, `tarball_url`, `tarball_path`, allowlists, SHA256 validation, output truncation.
        - Job event persistence is available via file-backed store (`WORKER_JOB_EVENT_DIR`).
        - Admin observability collector is available with gateway middleware and gRPC unary interceptors; snapshot endpoint: `GET /admin/v1/metrics` and Prometheus endpoint: `GET /admin/v1/metrics/prometheus` (also `GET /admin/v1/metrics?format=prometheus`).
        - Guardrails are configurable via env for replay caps, schedule run caps, and optional approval token checks (`WORKER_ADMIN_REPLAY_LIMIT_MAX`, `WORKER_ADMIN_REPLAY_IDS_MAX`, `WORKER_ADMIN_SCHEDULE_RUN_MAX`, `WORKER_ADMIN_SCHEDULE_RUN_WINDOW`, `WORKER_ADMIN_REQUIRE_APPROVAL`, `WORKER_ADMIN_APPROVAL_TOKEN`).
        - Artifact API parity: gateway now exposes job artifact metadata and download endpoints (`GET /admin/v1/jobs/{name}/artifact/meta`, `GET /admin/v1/jobs/{name}/artifact`) so UI no longer needs direct filesystem access for tarball-path jobs.
        - Audit export is available at `GET /admin/v1/audit/export` (`jsonl`, `json`, `csv`) with action/target filtering and bounded limits.
        - Audit retention is configurable via count (`WORKER_ADMIN_AUDIT_EVENT_LIMIT`) and optional age cutoff (`WORKER_ADMIN_AUDIT_RETENTION`); archived JSONL files can be enabled with `WORKER_ADMIN_AUDIT_ARCHIVE_DIR` (+ optional flush interval), and gateway export limits are capped by `WORKER_ADMIN_AUDIT_EXPORT_LIMIT_MAX`.
        - Gateway artifact handling now runs behind an internal artifact-store abstraction (filesystem implementation), reducing direct coupling in handlers.
- **Partial / gaps**
        - Event stream now supports `Last-Event-ID` with bounded in-memory replay; replay does not survive gateway restarts.
        - Metrics are in-memory snapshots with JSON + Prometheus text export; OTel/remote aggregation is still pending for cluster-wide long-term retention.
        - Central audit log supports count+age retention, bounded export, and file-based scheduled archival; external archive shipping/lifecycle policies are still pending.
        - No RBAC/authorization model beyond mTLS + perimeter controls.
        - Artifact download for `tarball_path` depends on gateway runtime mounts/config (`WORKER_ADMIN_JOB_TARBALL_DIR`) when worker and gateway are split.

### Admin UI

- **Implemented**
        - Operational pages for Overview, Queues, Schedules, DLQ, Jobs, Settings, and Docs.
        - Action flows for queue weight/pause, schedule management, DLQ replay, job creation/run/edit/delete.
        - Run-level page for jobs and event panes for schedules/jobs.
        - SSE + polling fallback for live updates.
        - Overview now includes a unified operations timeline (jobs + schedules + audit/queue/DLQ actions) with filters, paging, and run deep-links.
        - Timeline filter state persists locally between page visits.
        - Section state persistence is implemented for Queues, Schedules, Jobs, and DLQ (DLQ via URL query params).
        - Action/API failures now surface structured diagnostics in-page (mapped cause hints + request ID when available).
        - Gateway-facing Next API routes now emit a normalized error envelope (`error` + `errorDetail`) to keep diagnostics consistent across UI sections.
- **Partial / gaps**
        - Overview analytics are snapshot-focused; no built-in trend charts across day/week/month windows for all domains.
        - Timeline is implemented, but correlation is still basic (no causal graph/grouping by request/task chain).
        - Some high-value admin workflows are still split across pages (no global command palette / cross-page quick actions).
        - Artifact handling is source-aware but still operationally coupled to environment mounts for local tarballs.

## Gap Analysis (Production Focus)

### Must-have (service)

1. **Admin observability**: implemented as in-memory service metrics + gateway JSON/Prometheus endpoints; pending OTel export and durable retention backend.
1. **Policy enforcement**: implemented for DLQ replay caps, schedule run caps, and optional approval token checks in admin mutations.
1. **Artifact API parity**: implemented in gateway with an artifact-store abstraction; remaining work is non-filesystem providers (S3/object store) and signed URL policies.
1. **Audit retention/export**: implemented for count+time-window retention + export endpoints, plus file-based scheduled archival; remaining work is external archival lifecycle (e.g., object storage + retention policies).

### Must-have (admin UI)

1. **Unified run visibility**: show queued/running/completed transitions consistently with per-run immutable detail pages.
1. **Operational timelines**: implemented as a single filtered timeline; remaining work is richer correlation/grouping UX.
1. **Safety UX**: richer confirmation modals for risky actions with impact preview (counts, targets, scope).
1. **Error diagnostics**: structured error panels for gateway/API failures (request ID, mapped cause, recovery hint).
1. **State persistence**: partially implemented (timeline); extend to all sections for operator continuity.

### Nice-to-have (service)

1. S3/object-store event backend (in addition to file store) with retention lifecycle controls.
1. OpenAPI/JSON schema export for gateway endpoints.
1. WebSocket transport as optional alternative to SSE.
1. Multi-tenant authn/authz integration points (OIDC headers, signed tokens, policy hooks).

### Nice-to-have (admin UI)

1. Trend cards and small charts for throughput/failure/replay over configurable windows.
1. Bulk action workbench for queues/schedules/jobs with dry-run mode.
1. Advanced docs integration: contextual “why this failed” links from runtime errors to docs sections.
1. Exportable reports (CSV/JSON) for run history and DLQ operations.

## Milestones

1. Add AdminService proto + gateway routes.
1. Implement Redis-backed AdminBackend.
1. Update admin UI to call gateway (remove Redis direct access).
1. Provide Dockerfile + compose for worker + gateway + admin-ui.

## Risks & Mitigations

- **Gateway auth complexity**: use mTLS with clear docs and example certs.
- **Backend diversity**: isolate in `AdminBackend` interface.
- **Breaking changes**: version HTTP routes under `/admin/v1/`.
