"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { JobSchedule, ScheduleFactory } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { RelativeTime } from "@/components/relative-time";
import { StatusPill } from "@/components/status-pill";

export function SchedulesGrid({
  schedules,
  factories,
}: {
  schedules: JobSchedule[];
  factories: ScheduleFactory[];
}) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createSpec, setCreateSpec] = useState("");
  const [createDurable, setCreateDurable] = useState(false);
  const [selectedFactory, setSelectedFactory] = useState("");
  const [editing, setEditing] = useState<string | null>(null);
  const [editSpec, setEditSpec] = useState("");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();

  const existingSchedule = useMemo(() => {
    const name = createName.trim();
    if (!name) {
      return null;
    }
    return schedules.find((schedule) => schedule.name === name) ?? null;
  }, [createName, schedules]);

  const filtered = useMemo(() => {
    return schedules.filter((job) => {
      const matchesName = job.name.toLowerCase().includes(query.toLowerCase());
      const matchesStatus = status === "all" || job.status === status;
      return matchesName && matchesStatus;
    });
  }, [query, schedules, status]);

  const createSchedule = async () => {
    if (!createName.trim() || !createSpec.trim()) {
      setMessage({ tone: "error", text: "Name and cron spec are required." });
      return;
    }

    setMessage(null);
    try {
      const res = await fetch("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: createName.trim(),
          spec: createSpec.trim(),
          durable: createDurable,
        }),
      });

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Schedule create failed");
      }

      setMessage({
        tone: "success",
        text: existingSchedule ? "Schedule updated." : "Schedule created.",
      });
      setCreateOpen(false);
      setCreateName("");
      setCreateSpec("");
      setCreateDurable(false);
      setSelectedFactory("");
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Schedule create failed",
      });
    }
  };

  const togglePause = async (job: JobSchedule) => {
    setMessage(null);
    try {
      const res = await fetch(
        `/api/schedules/${encodeURIComponent(job.name)}/pause`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ paused: !job.paused }),
        }
      );

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Schedule update failed");
      }

      setMessage({
        tone: "success",
        text: job.paused ? "Schedule resumed." : "Schedule paused.",
      });
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Schedule update failed",
      });
    }
  };

  const deleteSchedule = async (job: JobSchedule) => {
    const ok = window.confirm(`Delete schedule "${job.name}"?`);
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(
        `/api/schedules/${encodeURIComponent(job.name)}`,
        { method: "DELETE" }
      );

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Schedule delete failed");
      }

      setMessage({ tone: "success", text: "Schedule deleted." });
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Schedule delete failed",
      });
    }
  };

  const startEdit = (job: JobSchedule) => {
    setEditing(job.name);
    setEditSpec(job.schedule);
    setMessage(null);
  };

  const handleFactorySelect = (value: string) => {
    setSelectedFactory(value);
    const factory = factories.find((entry) => entry.name === value);
    if (factory) {
      setCreateName(factory.name);
      setCreateDurable(factory.durable);
      const existing = schedules.find((schedule) => schedule.name === factory.name);
      setCreateSpec(existing?.schedule ?? "");
      return;
    }
    setCreateName("");
    setCreateDurable(false);
    setCreateSpec("");
  };

  const saveEdit = async (job: JobSchedule) => {
    const specValue = editSpec.trim();
    if (!specValue) {
      setMessage({ tone: "error", text: "Cron spec is required." });
      return;
    }

    setMessage(null);
    try {
      const res = await fetch("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: job.name,
          spec: specValue,
          durable: job.durable,
        }),
      });

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Schedule update failed");
      }

      setMessage({ tone: "success", text: "Schedule updated." });
      setEditing(null);
      setEditSpec("");
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Schedule update failed",
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
        placeholder="Search schedule"
        statusOptions={["all", "healthy", "lagging", "paused"]}
        onQuery={setQuery}
        onStatus={setStatus}
        rightSlot={
          <button
            onClick={() => setCreateOpen((prev) => !prev)}
            className="rounded-full bg-[var(--accent)] px-4 py-2 text-xs font-semibold text-[var(--accent-ink)]"
          >
            {createOpen ? "Close" : "New schedule"}
          </button>
        }
      />
      {createOpen ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <div className="grid gap-3 md:grid-cols-[2fr_2fr_1fr] md:items-end">
            <div className="md:col-span-3">
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Factory
              </label>
              <select
                value={selectedFactory}
                onChange={(event) => handleFactorySelect(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
              >
                <option value="">Select a registered factory</option>
                {factories.map((factory) => (
                  <option key={factory.name} value={factory.name}>
                    {factory.name}
                    {factory.durable ? " · durable" : ""}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Name
              </label>
              <input
                value={createName}
                onChange={(event) => setCreateName(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="billing-hourly"
                readOnly={factories.length > 0}
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Cron spec
              </label>
              <input
                value={createSpec}
                onChange={(event) => setCreateSpec(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="*/5 * * * *"
              />
            </div>
            <div className="flex items-center gap-2">
              <input
                id="schedule-durable"
                type="checkbox"
                checked={createDurable}
                onChange={(event) => setCreateDurable(event.target.checked)}
                disabled={factories.length > 0}
              />
              <label htmlFor="schedule-durable" className="text-xs text-muted">
                Durable
              </label>
            </div>
          </div>
          {factories.length > 0 ? (
            <p className="mt-3 text-xs text-muted">
              Schedule names must match a registered factory.
            </p>
          ) : null}
          {existingSchedule ? (
            <p className="mt-2 text-xs text-muted">
              Schedule already exists. Submitting will update its spec.
            </p>
          ) : null}
          <div className="mt-4 flex justify-end">
            <button
              onClick={createSchedule}
              disabled={pending}
              className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white disabled:opacity-50"
            >
              {existingSchedule ? "Update schedule" : "Create schedule"}
            </button>
          </div>
        </div>
      ) : null}
      <div className="grid gap-4 md:grid-cols-2">
        {filtered.map((job) => (
          <div
            key={job.name}
            className="rounded-2xl border border-soft bg-[var(--card)] p-4"
          >
            <div className="flex items-center justify-between">
              <p className="font-display text-base font-semibold">{job.name}</p>
              <StatusPill status={job.status} />
            </div>
            <p className="mt-2 text-xs text-muted">
              {job.schedule} · {job.durable ? "durable" : "in‑memory"}
            </p>
            <p className="mt-4 text-sm font-medium">
              Next run{" "}
              <span className="text-muted">
                <RelativeTime
                  valueMs={job.nextRunMs}
                  fallback={job.nextRun}
                  mode="future"
                />
              </span>
            </p>
            <p className="mt-1 text-xs text-muted">
              Last run{" "}
              <RelativeTime
                valueMs={job.lastRunMs}
                fallback={job.lastRun}
                mode="past"
              />
            </p>
            {editing === job.name ? (
              <div className="mt-4 rounded-2xl border border-soft bg-white/80 p-3">
                <label className="text-xs uppercase tracking-[0.2em] text-muted">
                  Cron spec
                </label>
                <input
                  value={editSpec}
                  onChange={(event) => setEditSpec(event.target.value)}
                  className="mt-2 w-full rounded-2xl border border-soft bg-white px-3 py-2 text-sm"
                />
                <div className="mt-3 flex flex-wrap gap-2">
                  <button
                    onClick={() => saveEdit(job)}
                    disabled={pending}
                    className="rounded-full bg-black px-3 py-1 text-xs font-semibold text-white disabled:opacity-50"
                  >
                    Save
                  </button>
                  <button
                    onClick={() => setEditing(null)}
                    className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            ) : null}
            <div className="mt-4 flex flex-wrap gap-2">
              <button
                onClick={() => togglePause(job)}
                disabled={pending}
                className="rounded-full border border-soft px-3 py-1 text-xs font-semibold disabled:opacity-50"
              >
                {job.paused ? "Resume" : "Pause"}
              </button>
              <button
                onClick={() => startEdit(job)}
                disabled={pending}
                className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted disabled:opacity-50"
              >
                Edit spec
              </button>
              <button
                onClick={() => deleteSchedule(job)}
                disabled={pending}
                className="rounded-full border border-rose-200 px-3 py-1 text-xs font-semibold text-rose-700 disabled:opacity-50"
              >
                Delete
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
