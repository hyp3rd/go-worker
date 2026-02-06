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
- UI real-time streaming (future work).

## Functional Requirements

1. **Overview data**
         - Return queue counts, processing counts, and DLQ size.
         - Return coordination state (global rate limit, leader lock, pause state).
1. **Queue data**
         - Return per-queue ready/processing/dead counts and weights.
         - Return queue detail by name (single-queue view).
         - Allow updating queue weight and resetting to default.
1. **Schedules**
         - List cron schedules with spec and next/previous run.
         - Create/update schedules by name (requires a registered factory).
         - Pause/resume schedules without unregistering the factory.
         - Delete schedules by name.
1. **Health**
         - Report service health and build information (version, commit, Go version).
1. **DLQ**
         - List DLQ entries (bounded, paginated by limit/offset, with optional filters).
         - Replay DLQ entries (bounded limit).
1. **Actions**
         - Pause durable dequeue.
         - Resume durable dequeue.
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
- `ListSchedules(ListSchedulesRequest) returns (ListSchedulesResponse)`
- `CreateSchedule(CreateScheduleRequest) returns (CreateScheduleResponse)`
- `DeleteSchedule(DeleteScheduleRequest) returns (DeleteScheduleResponse)`
- `PauseSchedule(PauseScheduleRequest) returns (PauseScheduleResponse)`
- `ListDLQ(ListDLQRequest) returns (ListDLQResponse)`
- `PauseDequeue(PauseDequeueRequest) returns (PauseDequeueResponse)`
- `ResumeDequeue(ResumeDequeueRequest) returns (ResumeDequeueResponse)`
- `ReplayDLQ(ReplayDLQRequest) returns (ReplayDLQResponse)`

### HTTP Gateway

- `GET /admin/v1/health`
- `GET /admin/v1/overview`
- `GET /admin/v1/queues`
- `GET /admin/v1/queues/{name}`
- `POST /admin/v1/queues/{name}/weight` (body: `{ "weight": 3 }`)
- `DELETE /admin/v1/queues/{name}/weight`
- `GET /admin/v1/schedules`
- `POST /admin/v1/schedules` (body: `{ "name": "...", "spec": "...", "durable": false }`)
- `DELETE /admin/v1/schedules/{name}`
- `POST /admin/v1/schedules/{name}/pause` (body: `{ "paused": true }`)
- `GET /admin/v1/dlq?limit=100&offset=0&queue=default&handler=send_email&query=oops`
- `POST /admin/v1/pause`
- `POST /admin/v1/resume`
- `POST /admin/v1/dlq/replay` (body: `{ "limit": 100 }`)

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

## Observability

- Log errors and action outcomes at INFO/WARN levels.
- Add counters for admin actions (pause/resume/replay).

## Implementation Status (February 5, 2026)

- **Overview/Queues/Queue detail**: Implemented (gRPC + gateway + UI).
- **Queue actions**: Implemented (set/reset weight via gRPC + gateway + UI).
- **DLQ pagination + filters**: Implemented in API; UI supports search + queue/handler filters with paging.
- **DLQ replay**: Implemented (bulk replay by limit + replay by ID/selection).
- **Schedules**: Implemented from in-memory cron registry; UI shows next/last run. Durable backends report schedules via TaskManager.
- **Health/version**: Implemented (gateway + UI).
- **Observability counters**: Implemented (pause/resume/replay action counts in overview).
- **Schedule management endpoints**: Implemented (create/delete/pause via gRPC + gateway + UI).

## Milestones

1. Add AdminService proto + gateway routes.
1. Implement Redis-backed AdminBackend.
1. Update admin UI to call gateway (remove Redis direct access).
1. Provide Dockerfile + compose for worker + gateway + admin-ui.

## Risks & Mitigations

- **Gateway auth complexity**: use mTLS with clear docs and example certs.
- **Backend diversity**: isolate in `AdminBackend` interface.
- **Breaking changes**: version HTTP routes under `/admin/v1/`.
