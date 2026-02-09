import { SectionHeader } from "@/components/section-header";

export const dynamic = "force-dynamic";

const envRows = [
  {
    key: "WORKER_ADMIN_API_URL",
    desc: "Gateway base URL (https).",
    example: "https://127.0.0.1:8081",
  },
  {
    key: "WORKER_ADMIN_MTLS_CERT",
    desc: "Client certificate for mTLS.",
    example: "/app/certs/admin-client.pem",
  },
  {
    key: "WORKER_ADMIN_MTLS_KEY",
    desc: "Client key for mTLS.",
    example: "/app/certs/admin-client-key.pem",
  },
  {
    key: "WORKER_ADMIN_MTLS_CA",
    desc: "CA certificate for mTLS.",
    example: "/app/certs/admin-ca.pem",
  },
  {
    key: "WORKER_ADMIN_PASSWORD",
    desc: "UI login password (required).",
    example: "change-me",
  },
  {
    key: "WORKER_ADMIN_ALLOW_MOCK",
    desc: "Allow mock fallback in non-production.",
    example: "false",
  },
  {
    key: "NEXT_PUBLIC_WORKER_ADMIN_ORIGIN",
    desc: "Override fetch origin for SSR.",
    example: "https://admin-ui.example.com",
  },
];

const workerEnvRows = [
  {
    key: "WORKER_JOB_REPO_ALLOWLIST",
    desc: "Allowed Git repos for job runner (comma-separated).",
    example: "github.com/acme/ops-jobs",
  },
  {
    key: "WORKER_JOB_TARBALL_ALLOWLIST",
    desc: "Allowed HTTPS tarball hosts (comma-separated).",
    example: "artifacts.example.com",
  },
  {
    key: "WORKER_JOB_TARBALL_DIR",
    desc: "Root directory for local tarball paths.",
    example: "/var/lib/go-worker/tarballs",
  },
  {
    key: "WORKER_JOB_TARBALL_MAX_BYTES",
    desc: "Maximum tarball size (bytes).",
    example: "67108864",
  },
  {
    key: "WORKER_JOB_TARBALL_TIMEOUT",
    desc: "Timeout for downloading tarballs.",
    example: "30s",
  },
  {
    key: "WORKER_JOB_OUTPUT_BYTES",
    desc: "Max combined stdout+stderr for job runs.",
    example: "65536",
  },
  {
    key: "WORKER_JOB_EVENT_DIR",
    desc: "Persist job events to a file-backed store.",
    example: "/var/lib/go-worker/job-events",
  },
  {
    key: "WORKER_JOB_EVENT_MAX_ENTRIES",
    desc: "Retention per key for job events.",
    example: "10000",
  },
  {
    key: "WORKER_JOB_EVENT_CACHE_TTL",
    desc: "Cache TTL for event reads.",
    example: "5s",
  },
];

const actions = [
  "Queue weight and pause/resume",
  "DLQ replay + per-item detail",
  "Schedule create/pause/run",
  "Schedule + job event logs",
  "Job runner create/run + output view",
  "Gateway health + coordination status",
];

export default function DocsPage() {
  return (
    <div className="space-y-6">
      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Admin UI Docs"
          description="How to connect, operate, and troubleshoot the admin console."
        />

        <div className="mt-6 grid gap-4 lg:grid-cols-3">
          <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">
              Purpose
            </p>
            <p className="mt-3 text-sm text-slate-700">
              The UI talks to the Admin Gateway (HTTP/JSON) which fronts the
              worker gRPC AdminService. All data comes from the gateway, not
              Redis, so multiple backends remain supported.
            </p>
          </div>
          <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">
              Streaming
            </p>
            <p className="mt-3 text-sm text-slate-700">
              Live updates use SSE (`/admin/v1/events`). If SSE drops, the UI
              falls back to polling every 15s.
            </p>
          </div>
          <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">
              Security
            </p>
            <p className="mt-3 text-sm text-slate-700">
              mTLS is required. Use `scripts/gen-admin-certs.sh` for local
              dev certificates. In production, issue certs from your CA.
            </p>
          </div>
        </div>
      </section>

      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Quick start"
          description="Bring up the gateway and UI locally."
        />
        <div className="mt-4 rounded-2xl border border-soft bg-[var(--card)] p-4">
          <pre className="whitespace-pre-wrap text-xs text-slate-700">
{`# generate dev certs
./scripts/gen-admin-certs.sh

# run the stack
docker compose -f compose.admin.yaml up --build`}
          </pre>
        </div>
      </section>

      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Environment"
          description="Configure the UI and gateway connectivity."
        />
        <div className="mt-4 overflow-hidden rounded-2xl border border-soft">
          <table className="w-full text-left text-sm">
            <thead className="bg-[var(--card)] text-xs uppercase tracking-[0.2em] text-muted">
              <tr>
                <th className="px-4 py-3">Variable</th>
                <th className="px-4 py-3">Description</th>
                <th className="px-4 py-3">Example</th>
              </tr>
            </thead>
            <tbody>
              {envRows.map((row) => (
                <tr key={row.key} className="border-t border-soft bg-white/90">
                  <td className="px-4 py-3 font-mono text-xs text-slate-700">
                    {row.key}
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-600">
                    {row.desc}
                  </td>
                  <td className="px-4 py-3 text-xs text-slate-500">
                    {row.example}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-6">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Worker service (jobs + event retention)
          </p>
          <div className="mt-3 overflow-hidden rounded-2xl border border-soft">
            <table className="w-full text-left text-sm">
              <thead className="bg-[var(--card)] text-xs uppercase tracking-[0.2em] text-muted">
                <tr>
                  <th className="px-4 py-3">Variable</th>
                  <th className="px-4 py-3">Description</th>
                  <th className="px-4 py-3">Example</th>
                </tr>
              </thead>
              <tbody>
                {workerEnvRows.map((row) => (
                  <tr key={row.key} className="border-t border-soft bg-white/90">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700">
                      {row.key}
                    </td>
                    <td className="px-4 py-3 text-sm text-slate-600">
                      {row.desc}
                    </td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {row.example}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Operations"
          description="What the admin UI supports today."
        />
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {actions.map((item) => (
            <div
              key={item}
              className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm text-slate-700"
            >
              {item}
            </div>
          ))}
        </div>
      </section>

      <section
        id="queues"
        className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft"
      >
        <SectionHeader
          title="Queues"
          description="Weights, pause controls, and safety notes."
        />
        <ul className="mt-4 space-y-3 text-sm text-slate-700">
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Weight controls relative scheduling share across queues; higher
            weight gets more throughput.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Pausing a queue stops durable dequeue for that queue only.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Creating a queue sets weight + metadata; it does not enqueue tasks.
          </li>
        </ul>
      </section>

      <section
        id="schedules"
        className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft"
      >
        <SectionHeader
          title="Schedules"
          description="Cron specs, on-demand runs, and pause semantics."
        />
        <ul className="mt-4 space-y-3 text-sm text-slate-700">
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Cron specs accept 5 fields (min hour dom mon dow) or `@every` syntax.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Run now triggers a single execution without changing the schedule.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Pause keeps the schedule registered but stops new runs.
          </li>
        </ul>
      </section>

      <section
        id="jobs"
        className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft"
      >
        <SectionHeader
          title="Jobs"
          description="Containerized job runner behavior and limits."
        />
        <ul className="mt-4 space-y-3 text-sm text-slate-700">
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Jobs can pull from a Git tag, an HTTPS tarball URL, or a local
            tarball path and then build a Docker image.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Tarball URLs must be allowlisted in WORKER_JOB_TARBALL_ALLOWLIST,
            and local tarballs are resolved under WORKER_JOB_TARBALL_DIR.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Provide tarball SHA256 to validate archive integrity before running.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Use the Crash‑test preset in the Jobs form to populate a local
            tarball job that fails by default for validation. Build the tarball
            with __examples/job_runner_dummy/create-tarball.sh so Dockerfile is
            at the tarball root.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Command overrides the image entrypoint; env keys are read from the
            worker service environment. Use KEY=VALUE to pass explicit overrides
            per job.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Output is truncated to the configured max bytes for safety.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Job run output is stored with the run event; use the output view for
            the latest run details.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Persist job events by setting WORKER_JOB_EVENT_DIR on the worker
            service; otherwise events reset on restart.
          </li>
        </ul>
      </section>

      <section
        id="dlq"
        className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft"
      >
        <SectionHeader
          title="DLQ"
          description="Replay safety and inspection tips."
        />
        <ul className="mt-4 space-y-3 text-sm text-slate-700">
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Replay is at-least-once; ensure handlers are idempotent.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            Use filters to narrow by queue, handler, or search text.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            DLQ detail exposes payload size, metadata, and last error.
          </li>
        </ul>
      </section>

      <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
        <SectionHeader
          title="Troubleshooting"
          description="Fast checks before opening an incident."
        />
        <ul className="mt-4 space-y-3 text-sm text-slate-700">
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            <span className="font-semibold">Gateway unreachable:</span> verify
            `WORKER_ADMIN_API_URL` and mTLS cert paths inside the container.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            <span className="font-semibold">Empty data:</span> check the admin
            service logs and ensure the worker service is running with cron/job
            handlers registered.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            <span className="font-semibold">Events not updating:</span> SSE
            might be blocked by a proxy. The UI falls back to 15s polling.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            <span className="font-semibold">Events missing after restart:</span>{" "}
            set WORKER_JOB_EVENT_DIR so job events persist to disk.
          </li>
          <li className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3">
            <span className="font-semibold">Tarball jobs failing:</span> verify
            WORKER_JOB_TARBALL_ALLOWLIST, WORKER_JOB_TARBALL_DIR, and SHA256.
          </li>
        </ul>
      </section>
    </div>
  );
}
