"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { JobSchedule } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { RelativeTime } from "@/components/relative-time";
import { StatusPill } from "@/components/status-pill";

export function SchedulesGrid({ schedules }: { schedules: JobSchedule[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createSpec, setCreateSpec] = useState("");
  const [createDurable, setCreateDurable] = useState(false);
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();

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

      setMessage({ tone: "success", text: "Schedule created." });
      setCreateOpen(false);
      setCreateName("");
      setCreateSpec("");
      setCreateDurable(false);
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
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Name
              </label>
              <input
                value={createName}
                onChange={(event) => setCreateName(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="billing-hourly"
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
              />
              <label htmlFor="schedule-durable" className="text-xs text-muted">
                Durable
              </label>
            </div>
          </div>
          <div className="mt-4 flex justify-end">
            <button
              onClick={createSchedule}
              disabled={pending}
              className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white disabled:opacity-50"
            >
              Create schedule
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
            <div className="mt-4 flex flex-wrap gap-2">
              <button
                onClick={() => togglePause(job)}
                disabled={pending}
                className="rounded-full border border-soft px-3 py-1 text-xs font-semibold disabled:opacity-50"
              >
                {job.paused ? "Resume" : "Pause"}
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
