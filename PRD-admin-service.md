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
1. **DLQ**
         - List DLQ entries (bounded, paginated by limit).
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
- `ListQueues(ListQueuesRequest) returns (ListQueuesResponse)`
- `ListDLQ(ListDLQRequest) returns (ListDLQResponse)`
- `PauseDequeue(PauseDequeueRequest) returns (PauseDequeueResponse)`
- `ResumeDequeue(ResumeDequeueRequest) returns (ResumeDequeueResponse)`
- `ReplayDLQ(ReplayDLQRequest) returns (ReplayDLQResponse)`

### HTTP Gateway

- `GET /admin/v1/overview`
- `GET /admin/v1/queues`
- `GET /admin/v1/dlq?limit=100`
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
- `WORKER_ADMIN_HTTP_ADDR` (default `127.0.0.1:8081`)
- `WORKER_ADMIN_TLS_CERT`, `WORKER_ADMIN_TLS_KEY`, `WORKER_ADMIN_TLS_CA`
- `WORKER_ADMIN_DEFAULT_QUEUE`, `WORKER_ADMIN_QUEUE_WEIGHTS` (backend defaults)

## Observability

- Log errors and action outcomes at INFO/WARN levels.
- Add counters for admin actions (pause/resume/replay).

## Milestones

1. Add AdminService proto + gateway routes.
1. Implement Redis-backed AdminBackend.
1. Update admin UI to call gateway (remove Redis direct access).
1. Provide Dockerfile + compose for worker + gateway + admin-ui.

## Risks & Mitigations

- **Gateway auth complexity**: use mTLS with clear docs and example certs.
- **Backend diversity**: isolate in `AdminBackend` interface.
- **Breaking changes**: version HTTP routes under `/admin/v1/`.
