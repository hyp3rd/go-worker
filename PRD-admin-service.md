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

## Implementation Status (February 9, 2026)

- **Overview/Queues/Queue detail**: Implemented (gRPC + gateway + UI).
- **Queue actions**: Implemented (set/reset weight + pause/resume via gRPC + gateway + UI).
- **DLQ pagination + filters**: Implemented in API; UI supports search + queue/handler filters with paging.
- **DLQ replay**: Implemented (bulk replay by limit + replay by ID/selection).
- **DLQ detail**: Implemented (gRPC + gateway + UI detail panel).
- **Schedules**: Implemented from in-memory cron registry; UI shows next/last run. Durable backends report schedules via TaskManager.
- **Schedule factories**: Implemented (gRPC + gateway).
- **Health/version**: Implemented (gateway + UI).
- **Observability counters**: Implemented (pause/resume/replay action counts in overview).
- **Schedule management endpoints**: Implemented (create/delete/pause/run + pause-all via gRPC + gateway + UI).
- **Events stream**: Implemented (gateway SSE + UI schedule + job event logs via `/api/events`).
- **Jobs (containerized runner)**: Implemented (API + worker-service + UI + event feed).
- **Job sources**: Implemented (Git tag, HTTPS tarball, local tarball path with allowlist + SHA256 validation).
- **Job event persistence**: Implemented (file-backed store via `WORKER_JOB_EVENT_DIR`).

## Milestones

1. Add AdminService proto + gateway routes.
1. Implement Redis-backed AdminBackend.
1. Update admin UI to call gateway (remove Redis direct access).
1. Provide Dockerfile + compose for worker + gateway + admin-ui.

## Risks & Mitigations

- **Gateway auth complexity**: use mTLS with clear docs and example certs.
- **Backend diversity**: isolate in `AdminBackend` interface.
- **Breaking changes**: version HTTP routes under `/admin/v1/`.
