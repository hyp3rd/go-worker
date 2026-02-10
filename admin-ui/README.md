# Go Worker Admin UI

Next.js app for operating `go-worker` via the Admin Gateway (HTTP/JSON over mTLS).

## Quick start

```bash
npm install
npm run dev
```

Open [<http://localhost:3000>](http://localhost:3000).

To run the full stack (worker + gateway + UI) with mTLS:

```bash
./scripts/gen-admin-certs.sh
docker compose -f compose.admin.yaml up --build
```

## Configuration

Create `admin-ui/.env.local` (or copy `admin-ui/.env.example`). Next.js loads
`.env.local` automatically.

UI env vars:

- `WORKER_ADMIN_API_URL` (e.g. `https://127.0.0.1:8081`)
- `WORKER_ADMIN_MTLS_CERT`, `WORKER_ADMIN_MTLS_KEY`, `WORKER_ADMIN_MTLS_CA`
- `WORKER_ADMIN_PASSWORD` (required)
- `WORKER_ADMIN_ALLOW_MOCK=false`
- `NEXT_PUBLIC_WORKER_ADMIN_ORIGIN` (optional override for SSR fetch)

Gateway env vars (set on `worker-admin`):

- `WORKER_ADMIN_JOB_TARBALL_DIR` (optional; enables local tarball download proxy)
- `WORKER_ADMIN_AUDIT_EXPORT_LIMIT_MAX` (optional cap for `GET /admin/v1/audit/export`)

Worker-service job runner (for Jobs + events):

- `WORKER_JOB_REPO_ALLOWLIST` (comma-separated; `*` to allow all)
- `WORKER_JOB_TARBALL_ALLOWLIST` (comma-separated hostnames for HTTPS tarballs)
- `WORKER_JOB_TARBALL_DIR` (root for local tarballs, default `/tmp`)
- `WORKER_JOB_TARBALL_MAX_BYTES` (default 64MiB)
- `WORKER_JOB_TARBALL_TIMEOUT` (default 30s)
- `WORKER_JOB_OUTPUT_BYTES` (max combined stdout+stderr)
- `WORKER_JOB_EVENT_DIR` (required to persist job events)
- `WORKER_JOB_EVENT_MAX_ENTRIES` (per key; default 10000)
- `WORKER_JOB_EVENT_CACHE_TTL` (default 10s)

## Audit export

The Overview runbook now includes an `Export audit` control that downloads
gateway audit records with optional filters.

- API route: `GET /api/audit/export`
- Supported query params:
      - `format`: `jsonl` (default), `json`, `csv`
      - `limit`: max records (gateway-enforced cap)
      - `action`: optional action filter
      - `target`: optional target filter

## Operations timeline

Overview includes a single timeline that merges:

- Job run events
- Schedule run events
- Audit events (including queue and DLQ actions)

Features:

- Live updates via SSE (`/api/events`) with polling fallback.
- Filters by source/status/time window + text search.
- Pagination + page-size selector.
- Deep-link to per-run job detail from timeline rows.
- Timeline filter state persisted in browser `localStorage`.

## Crash-test preset

The Jobs page includes a "Crash-test preset" button that fills the form with a
local tarball job. Build the tarball with
`__examples/job_runner_dummy/create-tarball.sh` and place it under
`WORKER_JOB_TARBALL_DIR` (default `/tmp`). The job fails by default unless you
set `DUMMY_SHOULD_FAIL=0` in the worker-service environment or provide
`DUMMY_SHOULD_FAIL=0` directly in the job env list.

## More docs

See `docs/admin-ui.md` for full usage and troubleshooting.
