"use client";

import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { RelativeTime } from "@/components/relative-time";
import { Table, TableCell, TableRow } from "@/components/table";
import type { AdminJob, JobEvent } from "@/lib/types";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Fragment, useEffect, useMemo, useState, useTransition } from "react";

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

const statusCounts = (events: JobEvent[]) => {
  const counts = {
    running: 0,
    queued: 0,
    failed: 0,
    completed: 0,
  };

  for (const event of events) {
    const status = normalizeEventStatus(event.status);
    if (status === "running") {
      counts.running += 1;
    } else if (status === "queued") {
      counts.queued += 1;
    } else if (status === "failed" || status === "deadline" || status === "invalid" || status === "cancelled") {
      counts.failed += 1;
    } else if (status === "completed") {
      counts.completed += 1;
    }
  }

  return counts;
};

const optimisticTTL = 10 * 60 * 1000;
const refreshIntervalMs = 15000;
const eventsFetchLimit = 200;

const mergeEvents = (primary: JobEvent[], secondary: JobEvent[]) => {
  const byId = new Map<string, JobEvent>();
  for (const event of secondary) {
    const existing = byId.get(event.taskId);
    if (!existing || eventTimestamp(event) >= eventTimestamp(existing)) {
      byId.set(event.taskId, event);
    }
  }
  for (const event of primary) {
    const existing = byId.get(event.taskId);
    if (!existing || eventTimestamp(event) >= eventTimestamp(existing)) {
      byId.set(event.taskId, event);
    }
  }

  const latestByName = new Map<string, JobEvent>();
  for (const event of primary) {
    const stamp = eventTimestamp(event);
    const current = latestByName.get(event.name);
    if (!current || stamp > eventTimestamp(current)) {
      latestByName.set(event.name, event);
    }
  }

  const now = Date.now();
  const merged: JobEvent[] = [];
  for (const event of byId.values()) {
    const isOptimistic = event.taskId.startsWith("pending-");
    if (isOptimistic && now - eventTimestamp(event) > optimisticTTL) {
      continue;
    }
    const latest = latestByName.get(event.name);
    if (isOptimistic && latest && eventTimestamp(latest) >= eventTimestamp(event)) {
      continue;
    }
    merged.push(event);
  }

  return merged;
};

const healthStyles: Record<string, string> = {
  healthy: "bg-emerald-100 text-emerald-700",
  watch: "bg-amber-100 text-amber-700",
  risk: "bg-rose-100 text-rose-700",
  learning: "bg-slate-100 text-slate-600",
};

const crashTestPreset = {
  name: "job_runner_dummy",
  description: "Crash-test dummy loaded from a local tarball.",
  repo: "",
  tag: "",
  source: "tarball_path",
  tarballUrl: "",
  tarballPath: "job_runner_dummy.tar.gz",
  tarballSha256: "",
  path: "",
  dockerfile: "Dockerfile",
  command: "",
  env: "DUMMY_SHOULD_FAIL",
  queue: "default",
  retries: "0",
  timeoutSeconds: "",
};

type IconProps = { className?: string };

const iconBaseClass = "h-3.5 w-3.5";

const EyeIcon = ({ className = iconBaseClass }: IconProps) => (
  <svg
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.9"
    className={className}
  >
    <path d="M2 12s3.8-7 10-7 10 7 10 7-3.8 7-10 7-10-7-10-7Z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);

const PencilIcon = ({ className = iconBaseClass }: IconProps) => (
  <svg
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.9"
    className={className}
  >
    <path d="m4 20 4.4-1 10-10a2.1 2.1 0 0 0-3-3l-10 10L4 20Z" />
    <path d="m13.5 6.5 4 4" />
  </svg>
);

const PlayIcon = ({ className = iconBaseClass }: IconProps) => (
  <svg
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="currentColor"
    className={className}
  >
    <path d="M8 5.7c0-1 1-1.6 1.9-1.1l9.4 6.3c.8.6.8 1.7 0 2.3l-9.4 6.3c-.9.6-1.9 0-1.9-1.1V5.7Z" />
  </svg>
);

const TrashIcon = ({ className = iconBaseClass }: IconProps) => (
  <svg
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.9"
    className={className}
  >
    <path d="M3 6h18" />
    <path d="M8 6V4h8v2" />
    <path d="M6 6l1 14h10l1-14" />
    <path d="M10 10v7M14 10v7" />
  </svg>
);

const DownloadIcon = ({ className = iconBaseClass }: IconProps) => (
  <svg
    aria-hidden="true"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.9"
    className={className}
  >
    <path d="M12 3v12" />
    <path d="m7 10 5 5 5-5" />
    <path d="M4 21h16" />
  </svg>
);

export function JobsGrid({
  jobs,
  events = [],
}: {
  jobs: AdminJob[];
  events?: JobEvent[];
}) {
  const router = useRouter();
  const [liveEvents, setLiveEvents] = useState<JobEvent[]>(events);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();
  const [optimisticEvents, setOptimisticEvents] = useState<JobEvent[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const { confirm, dialogProps } = useConfirmDialog();

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

  useEffect(() => {
    setLiveEvents(events);
  }, [events]);

  useEffect(() => {
    let source: EventSource | null = null;
    let interval: ReturnType<typeof setInterval> | null = null;

    const poll = async () => {
      try {
        const res = await fetch(`/api/jobs/events?limit=${eventsFetchLimit}`, {
          cache: "no-store",
        });
        if (!res.ok) {
          return;
        }
        const payload = (await res.json()) as { events?: JobEvent[] };
        if (payload?.events) {
          setLiveEvents((prev) => mergeEvents(payload.events ?? [], prev));
        }
      } catch {
        // ignore polling errors
      }
    };

    poll();
    interval = setInterval(poll, refreshIntervalMs);

    source = new EventSource("/api/events");
    source.addEventListener("job_events", (event) => {
      try {
        const payload = JSON.parse(event.data) as { events?: JobEvent[] };
        if (payload?.events) {
          setLiveEvents((prev) => mergeEvents(payload.events ?? [], prev));
        }
      } catch {
        // ignore malformed SSE payloads
      }
    });

    return () => {
      if (interval) {
        clearInterval(interval);
      }
      if (source) {
        source.close();
      }
    };
  }, []);

  const lastEvents = useMemo(() => {
    const allEvents = mergeEvents(liveEvents, optimisticEvents);
    const map = new Map<string, JobEvent>();
    for (const event of allEvents) {
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
  }, [liveEvents, optimisticEvents]);

  const source = normalizeSource(form.source);

  const loadCrashTest = () => {
    setEditingName(null);
    setCreateOpen(true);
    setForm(crashTestPreset);
  };

  const eventBuckets = useMemo(() => {
    const allEvents = mergeEvents(liveEvents, optimisticEvents);
    const map = new Map<string, JobEvent[]>();
    for (const event of allEvents) {
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
  }, [liveEvents, optimisticEvents]);

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

  const paged = useMemo(() => {
    const start = (page - 1) * pageSize;
    return filtered.slice(start, start + pageSize);
  }, [filtered, page, pageSize]);


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

  const handleQuery = (value: string) => {
    setQuery(value);
    setPage(1);
  };

  const handleStatus = (value: string) => {
    setStatusFilter(value);
    setPage(1);
  };

  const handlePageSize = (value: number) => {
    setPageSize(value);
    setPage(1);
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
    const ok = await confirm({
      title: `Delete job "${job.name}"?`,
      message: "This removes the job definition and its schedule factory.",
      confirmLabel: "Delete job",
      tone: "danger",
    });
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
    const ok = await confirm({
      title: `Run job "${job.name}" now?`,
      message: "A new durable task will be enqueued immediately.",
      confirmLabel: "Run now",
    });
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const optimistic: JobEvent = {
        taskId: `pending-${job.name}-${Date.now()}`,
        name: job.name,
        status: "queued",
        queue: job.queue || "default",
        repo: job.repo ?? "",
        tag: job.tag ?? "",
        path: job.path,
        dockerfile: job.dockerfile,
        command: formatList(job.command),
        startedAtMs: Date.now(),
        metadata: {
          "job.source": job.source ?? "",
          "job.tarball_url": job.tarballUrl ?? "",
          "job.tarball_path": job.tarballPath ?? "",
          "job.tarball_sha256": job.tarballSha256 ?? "",
          "job.optimistic": "true",
        },
      };

      const res = await fetch(
        `/api/jobs/${encodeURIComponent(job.name)}/run`,
        { method: "POST" }
      );
      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Job run failed");
      }
      setOptimisticEvents((prev) => mergeEvents(prev, [optimistic]));
      window.dispatchEvent(new CustomEvent("job-run-start", { detail: optimistic }));
      setMessage({ tone: "success", text: "Job enqueued." });
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Job run failed",
      });
    }
  };


  return (
    <div className="mt-6 space-y-4">
      {message ? (
        <div
          className={`rounded-2xl border px-4 py-3 text-sm ${message.tone === "success"
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
        onQuery={handleQuery}
        onStatus={handleStatus}
        rightSlot={
          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={loadCrashTest}
              className="rounded-full border border-soft bg-white px-4 py-2 text-xs font-semibold text-slate-700"
            >
              Crash-test preset
            </button>
            <button
              onClick={toggleCreate}
              className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white"
            >
              {createOpen ? "Close" : "New job"}
            </button>
          </div>
        }
      />

      {createOpen ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          {form.name === crashTestPreset.name ? (
            <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-900">
              Crash-test preset loaded. Ensure the tarball exists under
              WORKER_JOB_TARBALL_DIR (default: /tmp). Build it with
              __examples/job_runner_dummy/create-tarball.sh so Dockerfile is at
              the tarball root.
            </div>
          ) : null}
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
                placeholder="WORKER_ADMIN_API_URL or KEY=VALUE"
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
          </TableRow>
        ) : (
          paged.map((job) => {
            const lastEvent = lastEvents.get(job.name);
            const statusKey = normalizeEventStatus(lastEvent?.status);
            const displayTime = eventTimestamp(lastEvent);
            const health = healthFor(job.name);
            const preview = lastEvent?.error ?? lastEvent?.result ?? "";
            const previewLabel = lastEvent?.error ? "last error" : "last output";
            const sourceSummary = jobSourceSummary(job);
            const sourceKey = normalizeSource(job.source);
            const tarballPathDownloadHref = `/api/jobs/${encodeURIComponent(job.name)}/artifact`;
            const bucket = eventBuckets.get(job.name) ?? [];
            const counts = statusCounts(bucket);
            const activityParts = [
              counts.running ? `running ${counts.running}` : "",
              counts.queued ? `queued ${counts.queued}` : "",
              counts.failed ? `failed ${counts.failed}` : "",
            ].filter(Boolean);

            return (
              <Fragment key={job.name}>
                <TableRow>
                  <TableCell align="left">
                    <div>
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="font-semibold text-slate-900">{job.name}</p>
                        <span
                          className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${healthStyles[health.key] ?? healthStyles.learning
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
                      {sourceKey === "tarball_url" && job.tarballUrl ? (
                        <a
                          href={job.tarballUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="mt-1 inline-flex max-w-full items-center gap-1 truncate text-xs font-medium text-blue-700 hover:text-blue-800 hover:underline"
                        >
                          <DownloadIcon />
                          <span className="truncate">{sourceSummary.detail}</span>
                        </a>
                      ) : null}
                      {sourceKey === "tarball_path" && job.tarballPath ? (
                        <a
                          href={tarballPathDownloadHref}
                          className="mt-1 inline-flex max-w-full items-center gap-1 truncate text-xs font-medium text-blue-700 hover:text-blue-800 hover:underline"
                        >
                          <DownloadIcon />
                          <span className="truncate">{sourceSummary.detail}</span>
                        </a>
                      ) : null}
                      {sourceKey === "git_tag" ? (
                        <p className="mt-1 truncate">{sourceSummary.detail}</p>
                      ) : null}
                      {isTarballSource(sourceKey) ? (
                        <p className="mt-1 truncate font-mono text-[10px] text-muted">
                          sha256 {job.tarballSha256 || "n/a"}
                        </p>
                      ) : null}
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
                          className={`w-fit rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${statusStyles[statusKey] ?? statusStyles.unknown
                            }`}
                        >
                          {statusLabels[statusKey] ?? statusKey}
                        </span>
                        <span className="text-[11px] text-muted">
                          {activityParts.length > 0
                            ? activityParts.join(" · ")
                            : "no active runs"}
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
                </TableRow>
                <tr className="border-t border-soft bg-[var(--card)]/30">
                  <td colSpan={7} className="px-4 pb-4 pt-2">
                    <div className="flex flex-wrap justify-end gap-2">
                      {lastEvent ? (
                        <Link
                          href={`/jobs/runs/${encodeURIComponent(lastEvent.taskId)}`}
                          className="inline-flex cursor-pointer items-center gap-1 rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                        >
                          <EyeIcon />
                          View run
                        </Link>
                      ) : (
                        <span className="inline-flex cursor-not-allowed items-center gap-1 rounded-full border border-soft px-3 py-1 text-xs font-semibold text-slate-400">
                          <EyeIcon />
                          View run
                        </span>
                      )}
                      <button
                        onClick={() => startEdit(job)}
                        className="inline-flex cursor-pointer items-center gap-1 rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                      >
                        <PencilIcon />
                        Edit
                      </button>
                      <button
                        onClick={() => runJob(job)}
                        className="inline-flex cursor-pointer items-center gap-1 rounded-full border border-soft bg-black px-3 py-1 text-xs font-semibold text-white"
                      >
                        <PlayIcon />
                        Run now
                      </button>
                      <button
                        onClick={() => deleteJob(job)}
                        className="inline-flex cursor-pointer items-center gap-1 rounded-full border border-rose-200 px-3 py-1 text-xs font-semibold text-rose-600"
                      >
                        <TrashIcon />
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              </Fragment>
            );
          })
        )}
      </Table>
      <Pagination
        page={page}
        total={filtered.length}
        pageSize={pageSize}
        onNext={() => setPage((prev) => prev + 1)}
        onPrev={() => setPage((prev) => Math.max(1, prev - 1))}
        onPageSizeChange={handlePageSize}
        pageSizeOptions={[5, 10, 25, 50]}
      />
      <ConfirmDialog {...dialogProps} />
    </div>
  );
}
