# Admin UI & Gateway

The Admin UI is a Next.js app that talks to the worker **admin gateway** over
HTTP/JSON with mTLS. The gateway is a thin translation layer that fronts the
gRPC `AdminService` — all data flows through it. The UI does **not** access
Redis directly, so the durable backend remains pluggable.

## Quick start (local)

```bash
./scripts/gen-admin-certs.sh
docker compose -f compose.admin.yaml up --build
```

The compose file spins up Redis, the worker service, the admin gateway, and
the Next.js UI on `https://localhost:3000` (with mTLS).

## HTTP API (gateway)

All endpoints are under `/admin/v1/`. Content type is `application/json` for
request/response bodies. Errors come back with a
`{requestId, error: {code, message}}` envelope.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Service health + build info |
| GET | `/overview` | Stats, coordination state, admin action counters |
| GET | `/queues` | List all queues |
| GET | `/queues/{name}` | Queue summary |
| POST | `/queues/{name}/weight` | Update queue weight |
| DELETE | `/queues/{name}/weight` | Reset queue weight to default |
| POST | `/queues/{name}/pause` | Pause/resume one queue (**approval**) |
| GET | `/schedules` | List schedules |
| POST | `/schedules` | Create schedule |
| DELETE | `/schedules/{name}` | Delete schedule |
| POST | `/schedules/{name}/pause` | Pause/resume schedule (**approval**) |
| POST | `/schedules/{name}/run` | Trigger immediate run (**approval**) |
| POST | `/schedules/pause` | Pause/resume all schedules (**approval**) |
| GET | `/schedules/events` | Schedule execution history |
| GET | `/schedules/factories` | Registered schedule factories |
| GET | `/jobs` | List jobs |
| POST | `/jobs` | Upsert job |
| GET | `/jobs/{name}` | Job definition |
| DELETE | `/jobs/{name}` | Delete job |
| POST | `/jobs/{name}/run` | Trigger one-off run (**approval**) |
| GET | `/jobs/{name}/artifact/meta` | Tarball size/SHA256 metadata |
| GET | `/jobs/{name}/artifact` | Stream tarball (or 302 to remote) |
| GET | `/jobs/events` | Job execution history |
| GET | `/dlq` | List DLQ entries (`limit`, `offset`, `queue`, `handler`, `query`) |
| GET | `/dlq/{id}` | DLQ entry detail |
| POST | `/dlq/replay` | Replay DLQ entries up to limit (**approval**) |
| POST | `/dlq/replay/ids` | Replay specific IDs (**approval**) |
| POST | `/pause` | Pause global dequeue |
| POST | `/resume` | Resume global dequeue |
| GET | `/audit` | Audit events (`action`, `target`, `limit`) |
| GET | `/audit/export` | Export audit log as JSON / JSONL / CSV |
| GET | `/metrics` | JSON metrics snapshot |
| GET | `/metrics/prometheus` | Prometheus exposition format |
| GET | `/events` | Server-Sent Events stream |

Endpoints marked **approval** propagate the `X-Admin-Approval` header to the
backend as the gRPC metadata key `x-admin-approval`; the backend may reject
the call if the approval token is missing or invalid when approval is
required. Other endpoints ignore the header.

## Server-Sent Events (`/events`)

The SSE stream emits an event every 10 seconds while the client is connected.
Each tick flushes five event types in order:

| Event name | Payload |
| --- | --- |
| `overview` | `{stats, coordination, actions}` — same shape as `GET /overview` |
| `schedule_events` | `{events: [...]}` — latest schedule executions |
| `job_events` | `{events: [...]}` — latest job executions |
| `audit_events` | `{events: [...]}` — latest audit entries |
| `heartbeat` | `{t: <ms>}` — liveness tick for proxy/keep-alive |

Each event carries a monotonic `id:` header. Clients may resume after a
disconnect by sending `Last-Event-ID: <n>` (or the `?lastEventId=` query
param); the gateway replays events from an in-memory ring of the last 300
events and then resumes live emission. SSE requires a
flush-capable transport; when behind a proxy that buffers, the UI falls back
to polling every 15 seconds.

## Request headers

- **`X-Request-Id`** — optional. If present it is sanitized to
  `[A-Za-z0-9_.-]` and truncated to 128 chars before being forwarded to the
  backend as the gRPC metadata `x-request-id`. If absent or empty after
  sanitization the gateway generates a UUID. The effective value is echoed
  back in the response headers and included in every error envelope for
  end-to-end correlation.
- **`X-Admin-Approval`** — forwarded as `x-admin-approval` gRPC metadata on
  the destructive endpoints flagged above. Format is opaque to the gateway;
  the backend decides what constitutes a valid approval token.

## Environment variables

### UI container

- `WORKER_ADMIN_API_URL` (e.g. `https://127.0.0.1:8081`)
- `WORKER_ADMIN_MTLS_CERT`, `WORKER_ADMIN_MTLS_KEY`, `WORKER_ADMIN_MTLS_CA`
- `WORKER_ADMIN_PASSWORD` (required)
- `WORKER_ADMIN_ALLOW_MOCK=false`
- `NEXT_PUBLIC_WORKER_ADMIN_ORIGIN` (optional override for SSR fetch)

### Admin gateway (`worker-admin` binary)

- `WORKER_ADMIN_GRPC_ADDR` — gRPC backend address (default `127.0.0.1:50052`)
- `WORKER_ADMIN_HTTP_ADDR` — HTTP listen address (default `127.0.0.1:8081`)
- `WORKER_ADMIN_TLS_CERT`, `WORKER_ADMIN_TLS_KEY`, `WORKER_ADMIN_TLS_CA` —
  mTLS server cert/key and client CA used to verify the UI.
- `WORKER_ADMIN_JOB_TARBALL_DIR` — local root for artifact downloads; must
  match the worker-service setting when gateway and worker are split.
- `WORKER_ADMIN_AUDIT_EXPORT_MAX` — upper bound on `/audit/export` rows
  (default 5000).
- `WORKER_ADMIN_READ_TIMEOUT`, `WORKER_ADMIN_WRITE_TIMEOUT` — HTTP
  read/write timeouts.

### Worker-service job runner

- `WORKER_JOB_REPO_ALLOWLIST` (comma-separated; `*` to allow all)
- `WORKER_JOB_TARBALL_ALLOWLIST` (comma-separated hostnames for HTTPS tarballs)
- `WORKER_JOB_TARBALL_DIR` (root for local tarballs, default `/tmp`)
- `WORKER_JOB_TARBALL_MAX_BYTES` (default 64MiB)
- `WORKER_JOB_TARBALL_TIMEOUT` (default 30s)
- `WORKER_JOB_OUTPUT_BYTES` (max combined stdout+stderr)

## mTLS setup

`./scripts/gen-admin-certs.sh` generates a local CA plus server and client
certs under `./certs/`. The script writes:

- `ca.crt`, `ca.key` — local CA (keep out of production)
- `server.crt`, `server.key` — gateway server cert
- `client.crt`, `client.key` — UI client cert

Wiring:

| Side | File / env var |
| --- | --- |
| Gateway server cert | `WORKER_ADMIN_TLS_CERT=server.crt`, `WORKER_ADMIN_TLS_KEY=server.key` |
| Gateway verifies client | `WORKER_ADMIN_TLS_CA=ca.crt` |
| UI client cert | `WORKER_ADMIN_MTLS_CERT=client.crt`, `WORKER_ADMIN_MTLS_KEY=client.key` |
| UI verifies server | `WORKER_ADMIN_MTLS_CA=ca.crt` |

For production, replace the self-signed CA with your organization's PKI and
mount the files as secrets. The gateway refuses to start if the cert files
are unreadable; no plaintext HTTP mode is supported.

## UI feature coverage

- Overview + coordination health
- Queue list, weight update, pause/resume
- DLQ list, detail, replay (bulk or by ID)
- Schedules list, create/pause/run, event log
- Jobs list, create/run, job event log + artifact download
- Audit trail with filters and export (JSONL/JSON/CSV)

## Queues

- Weights control relative scheduling share across queues.
- Pausing a queue stops durable dequeue for that queue only.
- Creating a queue sets weight + metadata; it does not enqueue tasks.

## Schedules

- Cron specs accept 5 fields (min hour dom mon dow) or `@every` syntax.
- Run now triggers a single execution without changing the schedule.
- Pause keeps the schedule registered but stops new runs.

## Jobs

- Jobs can pull from a Git tag, an HTTPS tarball URL, or a local tarball path.
- Tarball URLs must be allowlisted with `WORKER_JOB_TARBALL_ALLOWLIST`.
- Tarball paths are resolved relative to `WORKER_JOB_TARBALL_DIR`.
- Provide `tarball_sha256` to validate archive integrity.
- Use `/jobs/{name}/artifact/meta` to fetch size + SHA256 without downloading;
  `/jobs/{name}/artifact` streams the tarball or 302-redirects to a remote
  URL when the source is off-host.
- Use the Crash-test preset in the Jobs form to populate a local tarball job
  that fails by default for validation. Build the tarball with
  `__examples/job_runner_dummy/create-tarball.sh` so the Dockerfile is at the
  root.
- Command overrides the image entrypoint; env keys come from the worker
  service. Use `KEY=VALUE` to pass explicit overrides per job.
- Output is truncated to the configured max bytes for safety.

## Event persistence

By default, job events are held in memory. To retain job events across
restarts, configure the worker-service file store:

- `WORKER_JOB_EVENT_DIR` (required, e.g. `/var/lib/go-worker/job-events`)
- `WORKER_JOB_EVENT_MAX_ENTRIES` (per key; default 10000)
- `WORKER_JOB_EVENT_CACHE_TTL` (e.g. `5s`; default 10s)

Mount a persistent volume at `WORKER_JOB_EVENT_DIR` in production so restarts
do not wipe the history. The storage interface is pluggable; future backends
can target object storage (S3) without changing the UI contract.

Schedule events follow a different path: when the worker uses a durable
backend, schedule execution history is recorded in that backend and is
therefore shared across worker instances. The in-memory fallback only applies
when no durable admin backend is available.

Audit events are durable-backend-first (Redis `LPUSH` + `LTRIM`) and can be
archived to disk via `WORKER_ADMIN_AUDIT_ARCHIVE_DIR` when enabled on the
worker service.

## DLQ

- Replay is at-least-once; ensure handlers are idempotent.
- Use filters to narrow by queue, handler, or search text.
- DLQ detail exposes payload size, metadata, and last error.
- Bulk replay (`/dlq/replay`) and replay-by-ID (`/dlq/replay/ids`) both
  require the `X-Admin-Approval` header to be forwarded to the backend.

## Troubleshooting

- **Gateway unreachable:** confirm the gateway URL and mTLS cert paths in
  the container. Curl the gateway with `--cert`, `--key`, `--cacert` to
  isolate whether the issue is TLS or routing.
- **403 / Unauthenticated responses:** verify the client cert is signed by
  the CA that the gateway trusts (`WORKER_ADMIN_TLS_CA`).
- **No data:** ensure the worker service is running and handlers are
  registered.
- **Events stale:** SSE blocked by proxy; UI falls back to polling every
  15s. Check `X-Accel-Buffering: no` is preserved end-to-end and that no
  proxy is buffering `text/event-stream`.
- **Last-Event-ID replay empty:** the in-memory ring holds only the last 300
  events; clients offline longer than that cannot fully catch up via replay
  and will see current state only.
- **Job events missing after restart:** set `WORKER_JOB_EVENT_DIR` so events
  persist.
- **Schedule events only show local runs:** use a durable backend; shared
  schedule history is persisted through the durable admin backend, not the
  local process.
- **Artifact download 404:** the gateway's `WORKER_ADMIN_JOB_TARBALL_DIR`
  must match the worker-service value when the two run in separate
  processes; otherwise the gateway cannot locate tarballs produced by the
  worker.
- **Approval rejected:** the backend expects `X-Admin-Approval` on every
  destructive endpoint marked above: DLQ replay, DLQ replay-by-ID,
  `/jobs/{name}/run`, `/schedules/{name}/run`, `/queues/{name}/pause`,
  `/schedules/pause`, and `/schedules/{name}/pause`. Make sure the UI and
  any direct API clients forward the header; the gateway propagates it to
  the backend as gRPC metadata `x-admin-approval` but does not generate one
  on its own.
