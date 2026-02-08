"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import type { AdminJob, JobEvent } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { Table, TableCell, TableRow } from "@/components/table";
import { RelativeTime } from "@/components/relative-time";

const parseList = (value: string) =>
  value
    .split(/[\n,]/)
    .map((entry) => entry.trim())
    .filter(Boolean);

const formatList = (values?: string[]) =>
  values && values.length > 0 ? values.join(", ") : "";

const statusOptions = [
  "all",
  "completed",
  "failed",
  "running",
  "queued",
  "rate_limited",
  "deadline",
  "cancelled",
  "invalid",
];

const statusStyles: Record<string, string> = {
  completed: "bg-emerald-100 text-emerald-700",
  failed: "bg-rose-100 text-rose-700",
  running: "bg-sky-100 text-sky-700",
  queued: "bg-slate-100 text-slate-700",
  rate_limited: "bg-slate-100 text-slate-700",
  deadline: "bg-amber-100 text-amber-700",
  cancelled: "bg-zinc-200 text-zinc-700",
  invalid: "bg-amber-100 text-amber-700",
  unknown: "bg-slate-200 text-slate-700",
};

const statusLabels: Record<string, string> = {
  completed: "completed",
  failed: "failed",
  running: "running",
  queued: "queued",
  rate_limited: "rate limited",
  deadline: "deadline",
  cancelled: "cancelled",
  invalid: "invalid",
  unknown: "n/a",
};

const jobSourceOptions = [
  { value: "git_tag", label: "Git tag" },
  { value: "tarball_url", label: "Tarball URL" },
  { value: "tarball_path", label: "Tarball path" },
];

const normalizeSource = (value?: string) => {
  const key = value?.trim().toLowerCase();
  if (!key) {
    return "git_tag";
  }
  if (key === "git_tag" || key === "tarball_url" || key === "tarball_path") {
    return key;
  }
  return "git_tag";
};

const isTarballSource = (source: string) =>
  source === "tarball_url" || source === "tarball_path";

const jobSourceSummary = (job?: AdminJob) => {
  if (!job) {
    return { title: "Git tag", detail: "n/a" };
  }

  const source = normalizeSource(job.source);
  if (source === "tarball_url") {
    return {
      title: "Tarball URL",
      detail: job.tarballUrl || "n/a",
    };
  }
  if (source === "tarball_path") {
    return {
      title: "Tarball path",
      detail: job.tarballPath || "n/a",
    };
  }

  const repo = job.repo || "n/a";
  const tag = job.tag ? `tag ${job.tag}` : "tag n/a";
  return { title: "Git tag", detail: `${repo} · ${tag}` };
};

const normalizeEventStatus = (status?: string) => {
  if (!status) {
    return "unknown";
  }
  const key = status.toLowerCase();
  return statusStyles[key] ? key : "unknown";
};

const eventTimestamp = (event?: JobEvent) =>
  event?.finishedAtMs ?? event?.startedAtMs ?? 0;

const healthStyles: Record<string, string> = {
  healthy: "bg-emerald-100 text-emerald-700",
  watch: "bg-amber-100 text-amber-700",
  risk: "bg-rose-100 text-rose-700",
  learning: "bg-slate-100 text-slate-600",
};

export function JobsGrid({
  jobs,
  events = [],
}: {
  jobs: AdminJob[];
  events?: JobEvent[];
}) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();
  const [outputEvent, setOutputEvent] = useState<JobEvent | null>(null);

  const [createOpen, setCreateOpen] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null);
  const [form, setForm] = useState({
    name: "",
    description: "",
    repo: "",
    tag: "",
    source: "git_tag",
    tarballUrl: "",
    tarballPath: "",
    tarballSha256: "",
    path: "",
    dockerfile: "",
    command: "",
    env: "",
    queue: "",
    retries: "0",
    timeoutSeconds: "",
  });

  const lastEvents = useMemo(() => {
    const map = new Map<string, JobEvent>();
    for (const event of events) {
      const name = event.name?.trim();
      if (!name) {
        continue;
      }
      const stamp = eventTimestamp(event);
      const current = map.get(name);
      if (!current || stamp > eventTimestamp(current)) {
        map.set(name, event);
      }
    }
    return map;
  }, [events]);

  const source = normalizeSource(form.source);

  const eventBuckets = useMemo(() => {
    const map = new Map<string, JobEvent[]>();
    for (const event of events) {
      const name = event.name?.trim();
      if (!name) {
        continue;
      }
      const list = map.get(name) ?? [];
      list.push(event);
      map.set(name, list);
    }
    for (const list of map.values()) {
      list.sort((a, b) => eventTimestamp(b) - eventTimestamp(a));
    }
    return map;
  }, [events]);

  const healthFor = (jobName: string) => {
    const list = eventBuckets.get(jobName) ?? [];
    const sample = list.slice(0, 10);
    let completed = 0;
    let failed = 0;
    for (const event of sample) {
      const status = normalizeEventStatus(event.status);
      if (status === "completed") {
        completed += 1;
      } else if (
        status === "failed" ||
        status === "deadline" ||
        status === "cancelled" ||
        status === "invalid"
      ) {
        failed += 1;
      }
    }

    const total = completed + failed;
    if (total < 3) {
      return { key: "learning", label: "learning" };
    }

    const ratio = completed / total;
    if (ratio >= 0.9) {
      return { key: "healthy", label: "healthy" };
    }
    if (ratio >= 0.6) {
      return { key: "watch", label: "watch" };
    }

    return { key: "risk", label: "risk" };
  };

  const summary = useMemo(() => {
    let withRuns = 0;
    let running = 0;
    let failed = 0;
    let completed = 0;

    for (const job of jobs) {
      const last = lastEvents.get(job.name);
      if (!last) {
        continue;
      }
      withRuns += 1;
      const status = normalizeEventStatus(last.status);
      if (status === "running") {
        running += 1;
      } else if (status === "failed") {
        failed += 1;
      } else if (status === "completed") {
        completed += 1;
      }
    }

    return {
      total: jobs.length,
      withRuns,
      running,
      failed,
      completed,
    };
  }, [jobs, lastEvents]);

  const filtered = useMemo(() => {
    const lower = query.toLowerCase();
    return jobs.filter((job) => {
      const haystack = [
        job.name,
        job.repo,
        job.tag,
        job.source ?? "",
        job.tarballUrl ?? "",
        job.tarballPath ?? "",
        job.description ?? "",
      ]
        .join(" ")
        .toLowerCase();

      if (lower && !haystack.includes(lower)) {
        return false;
      }

      if (statusFilter !== "all") {
        const last = lastEvents.get(job.name);
        const status = normalizeEventStatus(last?.status);
        if (status !== statusFilter) {
          return false;
        }
      }

      return true;
    });
  }, [jobs, lastEvents, query, statusFilter]);

  const outputJob = outputEvent
    ? jobs.find((job) => job.name === outputEvent.name)
    : undefined;
  const outputSource = outputJob
    ? jobSourceSummary(outputJob)
    : {
        title: "Git tag",
        detail: outputEvent
          ? `${outputEvent.repo || "n/a"}@${outputEvent.tag || "n/a"}`
          : "n/a",
      };

  const resetForm = (job?: AdminJob) => {
    setEditingName(job?.name ?? null);
    setForm({
      name: job?.name ?? "",
      description: job?.description ?? "",
      repo: job?.repo ?? "",
      tag: job?.tag ?? "",
      source: normalizeSource(job?.source),
      tarballUrl: job?.tarballUrl ?? "",
      tarballPath: job?.tarballPath ?? "",
      tarballSha256: job?.tarballSha256 ?? "",
      path: job?.path ?? "",
      dockerfile: job?.dockerfile ?? "",
      command: formatList(job?.command),
      env: formatList(job?.env),
      queue: job?.queue ?? "",
      retries: job?.retries?.toString() ?? "0",
      timeoutSeconds: job?.timeoutSeconds?.toString() ?? "",
    });
  };

  const toggleCreate = () => {
    if (createOpen) {
      setCreateOpen(false);
      resetForm();
      return;
    }
    resetForm();
    setCreateOpen(true);
  };

  const startEdit = (job: AdminJob) => {
    resetForm(job);
    setCreateOpen(true);
  };

  const saveJob = async () => {
    const name = form.name.trim();
    const source = normalizeSource(form.source);
    const repo = form.repo.trim();
    const tag = form.tag.trim();
    const tarballUrl = form.tarballUrl.trim();
    const tarballPath = form.tarballPath.trim();
    const tarballSha256 = form.tarballSha256.trim();

    if (!name) {
      setMessage({
        tone: "error",
        text: "Job name is required.",
      });
      return;
    }

    if (source === "git_tag") {
      if (!repo || !tag) {
        setMessage({
          tone: "error",
          text: "Repo and tag are required for git tag jobs.",
        });
        return;
      }
    } else if (source === "tarball_url") {
      if (!tarballUrl) {
        setMessage({
          tone: "error",
          text: "Tarball URL is required for tarball jobs.",
        });
        return;
      }
    } else if (source === "tarball_path") {
      if (!tarballPath) {
        setMessage({
          tone: "error",
          text: "Tarball path is required for tarball jobs.",
        });
        return;
      }
    } else {
      setMessage({ tone: "error", text: "Unknown job source." });
      return;
    }

    if (tarballSha256 && tarballSha256.length !== 64) {
      setMessage({
        tone: "error",
        text: "Tarball SHA256 must be a 64-character hex string.",
      });
      return;
    }

    const retries = Number(form.retries);
    const timeoutSeconds = form.timeoutSeconds
      ? Number(form.timeoutSeconds)
      : 0;

    if (!Number.isFinite(retries) || retries < 0) {
      setMessage({ tone: "error", text: "Retries must be 0 or higher." });
      return;
    }

    if (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 0) {
      setMessage({
        tone: "error",
        text: "Timeout must be 0 or higher.",
      });
      return;
    }

    const repoValue = source === "git_tag" ? repo : "";
    const tagValue = source === "git_tag" ? tag : "";
    const tarballUrlValue = source === "tarball_url" ? tarballUrl : "";
    const tarballPathValue = source === "tarball_path" ? tarballPath : "";
    const tarballShaValue = isTarballSource(source) ? tarballSha256 : "";

    setMessage(null);
    try {
      const res = await fetch("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          description: form.description.trim(),
          repo: repoValue,
          tag: tagValue,
          source,
          tarballUrl: tarballUrlValue,
          tarballPath: tarballPathValue,
          tarballSha256: tarballShaValue,
          path: form.path.trim(),
          dockerfile: form.dockerfile.trim(),
          command: parseList(form.command),
          env: parseList(form.env),
          queue: form.queue.trim(),
          retries,
          timeoutSeconds,
        }),
      });

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Job save failed");
      }

      setMessage({
        tone: "success",
        text: editingName ? "Job updated." : "Job created.",
      });
      setCreateOpen(false);
      resetForm();
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Job save failed",
      });
    }
  };

  const deleteJob = async (job: AdminJob) => {
    const ok = window.confirm(`Delete job "${job.name}"?`);
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(job.name)}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Job delete failed");
      }
      setMessage({ tone: "success", text: "Job deleted." });
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Job delete failed",
      });
    }
  };

  const runJob = async (job: AdminJob) => {
    const ok = window.confirm(`Run job "${job.name}" now?`);
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(
        `/api/jobs/${encodeURIComponent(job.name)}/run`,
        { method: "POST" }
      );
      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Job run failed");
      }
      setMessage({ tone: "success", text: "Job enqueued." });
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Job run failed",
      });
    }
  };

  const openOutput = (event: JobEvent | null) => {
    if (!event) {
      return;
    }
    setOutputEvent(event);
  };

  const closeOutput = () => {
    setOutputEvent(null);
  };

  return (
    <div className="mt-6 space-y-4">
      {message ? (
        <div
          className={`rounded-2xl border px-4 py-3 text-sm ${
            message.tone === "success"
              ? "border-emerald-200 bg-emerald-50 text-emerald-800"
              : "border-rose-200 bg-rose-50 text-rose-800"
          }`}
        >
          {message.text}
        </div>
      ) : null}
      <div className="grid gap-3 md:grid-cols-4">
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            total jobs
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-slate-900">
            {summary.total}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            running
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-slate-900">
            {summary.running}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            failed
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-rose-700">
            {summary.failed}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            completed
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-emerald-700">
            {summary.completed}
          </p>
        </div>
      </div>
      <FilterBar
        placeholder="Search job name"
        statusOptions={statusOptions}
        onQuery={setQuery}
        onStatus={setStatusFilter}
        rightSlot={
          <button
            onClick={toggleCreate}
            className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white"
          >
            {createOpen ? "Close" : "New job"}
          </button>
        }
      />

      {createOpen ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Name
              </label>
              <input
                value={form.name}
                disabled={Boolean(editingName)}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, name: event.target.value }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm disabled:bg-slate-100"
                placeholder="metrics_rollup"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Source
              </label>
              <select
                value={form.source}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    source: event.target.value,
                  }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
              >
                {jobSourceOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
            {source === "git_tag" ? (
              <>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Repo
                  </label>
                  <input
                    value={form.repo}
                    onChange={(event) =>
                      setForm((prev) => ({ ...prev, repo: event.target.value }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="git@github.com:org/jobs.git"
                  />
                </div>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Tag
                  </label>
                  <input
                    value={form.tag}
                    onChange={(event) =>
                      setForm((prev) => ({ ...prev, tag: event.target.value }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="v1.2.3"
                  />
                </div>
              </>
            ) : null}
            {source === "tarball_url" ? (
              <>
                <div className="md:col-span-2">
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Tarball URL
                  </label>
                  <input
                    value={form.tarballUrl}
                    onChange={(event) =>
                      setForm((prev) => ({
                        ...prev,
                        tarballUrl: event.target.value,
                      }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="https://example.com/jobs/metrics.tar.gz"
                  />
                </div>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Tarball SHA256 (optional)
                  </label>
                  <input
                    value={form.tarballSha256}
                    onChange={(event) =>
                      setForm((prev) => ({
                        ...prev,
                        tarballSha256: event.target.value,
                      }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="64-char hex"
                  />
                </div>
              </>
            ) : null}
            {source === "tarball_path" ? (
              <>
                <div className="md:col-span-2">
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Tarball path
                  </label>
                  <input
                    value={form.tarballPath}
                    onChange={(event) =>
                      setForm((prev) => ({
                        ...prev,
                        tarballPath: event.target.value,
                      }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="jobs/metrics.tar.gz"
                  />
                  <p className="mt-1 text-xs text-muted">
                    Path is resolved against WORKER_JOB_TARBALL_DIR.
                  </p>
                </div>
                <div>
                  <label className="text-xs uppercase tracking-[0.2em] text-muted">
                    Tarball SHA256 (optional)
                  </label>
                  <input
                    value={form.tarballSha256}
                    onChange={(event) =>
                      setForm((prev) => ({
                        ...prev,
                        tarballSha256: event.target.value,
                      }))
                    }
                    className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                    placeholder="64-char hex"
                  />
                </div>
              </>
            ) : null}
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Queue
              </label>
              <input
                value={form.queue}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, queue: event.target.value }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="default"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Path
              </label>
              <input
                value={form.path}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, path: event.target.value }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="jobs/metrics"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Dockerfile
              </label>
              <input
                value={form.dockerfile}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    dockerfile: event.target.value,
                  }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="Dockerfile"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Retries
              </label>
              <input
                value={form.retries}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    retries: event.target.value,
                  }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                type="number"
                min={0}
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Timeout (seconds)
              </label>
              <input
                value={form.timeoutSeconds}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    timeoutSeconds: event.target.value,
                  }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                type="number"
                min={0}
              />
            </div>
            <div className="md:col-span-2">
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Description
              </label>
              <input
                value={form.description}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    description: event.target.value,
                  }))
                }
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="What this job does."
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Command (comma or newline)
              </label>
              <textarea
                value={form.command}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    command: event.target.value,
                  }))
                }
                className="mt-2 h-24 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="./run.sh, --full"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Env (comma or newline)
              </label>
              <textarea
                value={form.env}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, env: event.target.value }))
                }
                className="mt-2 h-24 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="WORKER_ADMIN_API_URL"
              />
            </div>
          </div>
          <div className="mt-4 flex flex-wrap gap-2">
            <button
              onClick={saveJob}
              disabled={pending}
              className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white disabled:cursor-not-allowed disabled:opacity-60"
            >
              {editingName ? "Update job" : "Create job"}
            </button>
            {editingName ? (
              <button
                onClick={() => {
                  setCreateOpen(false);
                  resetForm();
                }}
                className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
              >
                Cancel
              </button>
            ) : null}
          </div>
        </div>
      ) : null}

      <Table
        columns={[
          { key: "job", label: "Job" },
          { key: "source", label: "Source" },
          { key: "queue", label: "Queue" },
          { key: "retries", label: "Retries" },
          { key: "timeout", label: "Timeout" },
          { key: "status", label: "Status" },
          { key: "updated", label: "Updated" },
          { key: "actions", label: "" },
        ]}
      >
        {filtered.length === 0 ? (
          <TableRow>
            <TableCell align="left">
              <p className="text-sm text-muted">No jobs yet.</p>
            </TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
            <TableCell align="left">{"\u00a0"}</TableCell>
          </TableRow>
        ) : (
          filtered.map((job) => {
            const lastEvent = lastEvents.get(job.name);
            const statusKey = normalizeEventStatus(lastEvent?.status);
            const displayTime = eventTimestamp(lastEvent);
            const health = healthFor(job.name);
            const preview = lastEvent?.error ?? lastEvent?.result ?? "";
            const previewLabel = lastEvent?.error ? "last error" : "last output";
            const sourceSummary = jobSourceSummary(job);

            return (
            <TableRow key={job.name}>
              <TableCell align="left">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <p className="font-semibold text-slate-900">{job.name}</p>
                    <span
                      className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${
                        healthStyles[health.key] ?? healthStyles.learning
                      }`}
                    >
                      {health.label}
                    </span>
                  </div>
                  <p className="text-xs text-muted">
                    {job.description || "No description"}
                  </p>
                </div>
              </TableCell>
              <TableCell align="left">
                <div className="text-xs text-muted">
                  <p className="text-sm text-slate-700">{sourceSummary.title}</p>
                  <p className="mt-1 truncate">{sourceSummary.detail}</p>
                </div>
              </TableCell>
              <TableCell align="left">
                <span className="text-sm text-slate-700">
                  {job.queue || "default"}
                </span>
              </TableCell>
              <TableCell align="left">
                <span className="text-sm text-slate-700">
                  {job.retries ?? 0}
                </span>
              </TableCell>
              <TableCell align="left">
                <span className="text-sm text-slate-700">
                  {job.timeoutSeconds ? `${job.timeoutSeconds}s` : "n/a"}
                </span>
              </TableCell>
              <TableCell align="left">
                <div className="relative group">
                  <div className="flex flex-col gap-1">
                    <span
                      className={`w-fit rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${
                        statusStyles[statusKey] ?? statusStyles.unknown
                      }`}
                    >
                      {statusLabels[statusKey] ?? statusKey}
                    </span>
                    {displayTime ? (
                      <RelativeTime valueMs={displayTime} mode="past" />
                    ) : (
                      <span className="text-xs text-muted">n/a</span>
                    )}
                  </div>
                  {preview ? (
                    <div className="pointer-events-none absolute left-0 top-full z-10 mt-2 w-64 rounded-xl border border-soft bg-white/95 p-3 text-xs text-slate-700 opacity-0 shadow-soft transition group-hover:opacity-100">
                      <p className="text-[10px] uppercase tracking-[0.2em] text-muted">
                        {previewLabel}
                      </p>
                      <p className="mt-1 max-h-20 overflow-hidden break-words">
                        {preview}
                      </p>
                    </div>
                  ) : null}
                </div>
              </TableCell>
              <TableCell align="left">
                {job.updatedAtMs ? (
                  <RelativeTime valueMs={job.updatedAtMs} mode="past" />
                ) : (
                  <span className="text-xs text-muted">n/a</span>
                )}
              </TableCell>
              <TableCell align="right">
                <div className="flex flex-wrap justify-end gap-2">
                  {lastEvent ? (
                    <Link
                      href={`/jobs?job=${encodeURIComponent(job.name)}#job-events`}
                      className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                    >
                      View run
                    </Link>
                  ) : (
                    <span className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-slate-400">
                      View run
                    </span>
                  )}
                  {lastEvent ? (
                    <button
                      onClick={() => openOutput(lastEvent)}
                      className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                    >
                      View output
                    </button>
                  ) : (
                    <span className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-slate-400">
                      View output
                    </span>
                  )}
                  <button
                    onClick={() => startEdit(job)}
                    className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                  >
                    Edit
                  </button>
                  <button
                    onClick={() => runJob(job)}
                    className="rounded-full border border-soft bg-black px-3 py-1 text-xs font-semibold text-white"
                  >
                    Run now
                  </button>
                  <button
                    onClick={() => deleteJob(job)}
                    className="rounded-full border border-rose-200 px-3 py-1 text-xs font-semibold text-rose-600"
                  >
                    Delete
                  </button>
                </div>
              </TableCell>
            </TableRow>
            );
          })
        )}
      </Table>
      {outputEvent ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4">
          <div className="max-h-[80vh] w-full max-w-3xl overflow-y-auto rounded-3xl border border-soft bg-white p-6 shadow-soft">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.2em] text-muted">
                  job output
                </p>
                <p className="mt-1 font-display text-xl font-semibold text-slate-900">
                  {outputEvent.name}
                </p>
                <p className="text-sm text-slate-600">
                  {outputSource.title}: {outputSource.detail} ·{" "}
                  {outputEvent.queue || "default"}
                </p>
              </div>
              <button
                onClick={closeOutput}
                className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
              >
                Close
              </button>
            </div>

            <div className="mt-4 grid gap-3 md:grid-cols-3">
              <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
                <p className="uppercase tracking-[0.2em] text-muted">status</p>
                <p className="mt-2 text-sm font-semibold text-slate-900">
                  {statusLabels[normalizeEventStatus(outputEvent.status)] ?? "n/a"}
                </p>
              </div>
              <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
                <p className="uppercase tracking-[0.2em] text-muted">duration</p>
                <p className="mt-2 text-sm font-semibold text-slate-900">
                  {outputEvent.durationMs ? `${outputEvent.durationMs}ms` : "n/a"}
                </p>
              </div>
              <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
                <p className="uppercase tracking-[0.2em] text-muted">command</p>
                <p className="mt-2 text-sm font-semibold text-slate-900">
                  {outputEvent.command || "n/a"}
                </p>
              </div>
            </div>

            <div className="mt-4 rounded-2xl border border-soft bg-[var(--card)] p-4 text-sm text-slate-700">
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                output
              </p>
              <p className="mt-2 whitespace-pre-wrap break-words">
                {outputEvent.error || outputEvent.result || "No output captured."}
              </p>
              <p className="mt-3 text-xs text-muted">
                Output may be truncated by the worker service configuration.
              </p>
            </div>

            {outputEvent.metadata &&
            Object.keys(outputEvent.metadata).length > 0 ? (
              <div className="mt-4 grid gap-2 md:grid-cols-2">
                {Object.entries(outputEvent.metadata)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, value]) => (
                    <div
                      key={key}
                      className="rounded-xl border border-soft bg-white/90 px-3 py-2 text-xs text-slate-600"
                    >
                      <span className="uppercase tracking-[0.16em] text-muted">
                        {key}
                      </span>
                      <p className="mt-1 break-words text-sm text-slate-800">
                        {value}
                      </p>
                    </div>
                  ))}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
