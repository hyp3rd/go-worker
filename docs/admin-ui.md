# Admin UI

The Admin UI is a Next.js app that talks to the worker admin gateway over HTTP/JSON with mTLS. It does **not** access Redis directly, so multiple durable backends remain supported.

## Quick start (local)

```bash
./scripts/gen-admin-certs.sh
docker compose -f compose.admin.yaml up --build
```

## Environment variables

These are read by the UI container:

- `WORKER_ADMIN_API_URL` (e.g. `https://127.0.0.1:8081`)
- `WORKER_ADMIN_MTLS_CERT`, `WORKER_ADMIN_MTLS_KEY`, `WORKER_ADMIN_MTLS_CA`
- `WORKER_ADMIN_PASSWORD` (required)
- `WORKER_ADMIN_ALLOW_MOCK=false`
- `NEXT_PUBLIC_WORKER_ADMIN_ORIGIN` (optional override for SSR fetch)

Worker-service job runner configuration:

- `WORKER_JOB_REPO_ALLOWLIST` (comma-separated; `*` to allow all)
- `WORKER_JOB_TARBALL_ALLOWLIST` (comma-separated hostnames for HTTPS tarballs)
- `WORKER_JOB_TARBALL_DIR` (root for local tarballs, default `/tmp`)
- `WORKER_JOB_TARBALL_MAX_BYTES` (default 64MiB)
- `WORKER_JOB_TARBALL_TIMEOUT` (default 30s)
- `WORKER_JOB_OUTPUT_BYTES` (max combined stdout+stderr)

## What the UI supports

- Overview + coordination health
- Queue list, weight update, pause/resume
- DLQ list, detail, replay (bulk or by ID)
- Schedules list, create/pause/run, event log
- Jobs list, create/run, job event log

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
- Job event history can be persisted across restarts by configuring a
  file-backed store (see below).
- Use the Crash-test preset in the Jobs form to populate a local tarball job
  that fails by default for validation. Build the tarball with
  `__examples/job_runner_dummy/create-tarball.sh` so Dockerfile is at the root.
- Command overrides the image entrypoint; env keys come from the worker service.
  Use `KEY=VALUE` to pass explicit overrides per job.
- Output is truncated to the configured max bytes for safety.

## Job event persistence (worker-service)

By default, job events are held in memory. To retain job events across restarts,
configure the worker-service file store:

- `WORKER_JOB_EVENT_DIR` (required, e.g. `/var/lib/go-worker/job-events`)
- `WORKER_JOB_EVENT_MAX_ENTRIES` (per key; default 10000)
- `WORKER_JOB_EVENT_CACHE_TTL` (e.g. `5s`; default 10s)

Mount a persistent volume at `WORKER_JOB_EVENT_DIR` in production so restarts
do not wipe the history. The storage interface is pluggable; future backends
can target object storage (S3) without changing the UI contract.

## DLQ

- Replay is at-least-once; ensure handlers are idempotent.
- Use filters to narrow by queue, handler, or search text.
- DLQ detail exposes payload size, metadata, and last error.

## Troubleshooting

- **Gateway unreachable:** confirm the gateway URL and mTLS cert paths in the container.
- **No data:** ensure the worker service is running and handlers are registered.
- **Events stale:** SSE blocked by proxy; UI falls back to polling every 15s.
- **Events missing after restart:** set `WORKER_JOB_EVENT_DIR` so events persist.
