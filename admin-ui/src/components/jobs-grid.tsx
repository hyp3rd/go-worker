"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { AdminJob } from "@/lib/types";
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

export function JobsGrid({ jobs }: { jobs: AdminJob[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();

  const [createOpen, setCreateOpen] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null);
  const [form, setForm] = useState({
    name: "",
    description: "",
    repo: "",
    tag: "",
    path: "",
    dockerfile: "",
    command: "",
    env: "",
    queue: "",
    retries: "0",
    timeoutSeconds: "",
  });

  const filtered = useMemo(() => {
    const lower = query.toLowerCase();
    return jobs.filter((job) => job.name.toLowerCase().includes(lower));
  }, [jobs, query]);

  const resetForm = (job?: AdminJob) => {
    setEditingName(job?.name ?? null);
    setForm({
      name: job?.name ?? "",
      description: job?.description ?? "",
      repo: job?.repo ?? "",
      tag: job?.tag ?? "",
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
    const repo = form.repo.trim();
    const tag = form.tag.trim();
    if (!name || !repo || !tag) {
      setMessage({
        tone: "error",
        text: "Name, repo, and tag are required.",
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

    setMessage(null);
    try {
      const res = await fetch("/api/jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          description: form.description.trim(),
          repo,
          tag,
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
      <FilterBar
        placeholder="Search job name"
        onQuery={setQuery}
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
          { key: "repo", label: "Repo" },
          { key: "queue", label: "Queue" },
          { key: "retries", label: "Retries" },
          { key: "timeout", label: "Timeout" },
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
          </TableRow>
        ) : (
          filtered.map((job) => (
            <TableRow key={job.name}>
              <TableCell align="left">
                <div>
                  <p className="font-semibold text-slate-900">{job.name}</p>
                  <p className="text-xs text-muted">
                    {job.description || "No description"}
                  </p>
                </div>
              </TableCell>
              <TableCell align="left">
                <div className="text-xs text-muted">
                  <p className="text-sm text-slate-700">{job.repo}</p>
                  <p className="mt-1">tag {job.tag}</p>
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
                {job.updatedAtMs ? (
                  <RelativeTime valueMs={job.updatedAtMs} mode="past" />
                ) : (
                  <span className="text-xs text-muted">n/a</span>
                )}
              </TableCell>
              <TableCell align="right">
                <div className="flex flex-wrap justify-end gap-2">
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
          ))
        )}
      </Table>
    </div>
  );
}
