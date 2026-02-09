"use client";

import { useMemo, useState } from "react";
import type { JobEvent } from "@/lib/types";

const windows = [
  { value: "day", label: "Day", ms: 24 * 60 * 60 * 1000 },
  { value: "week", label: "Week", ms: 7 * 24 * 60 * 60 * 1000 },
  { value: "month", label: "Month", ms: 30 * 24 * 60 * 60 * 1000 },
  { value: "quarter", label: "Quarter", ms: 90 * 24 * 60 * 60 * 1000 },
  { value: "year", label: "Year", ms: 365 * 24 * 60 * 60 * 1000 },
];

const normalizeStatus = (value?: string) =>
  value ? value.toLowerCase() : "unknown";

const eventTimestamp = (event: JobEvent) =>
  event.finishedAtMs ?? event.startedAtMs ?? 0;

export function JobOverview({ events }: { events: JobEvent[] }) {
  const [windowValue, setWindowValue] = useState("week");

  const summary = useMemo(() => {
    const selected =
      windows.find((entry) => entry.value === windowValue) ?? windows[1];
    const cutoff = Date.now() - selected.ms;

    let running = 0;
    let success = 0;
    let failed = 0;
    let queued = 0;

    for (const event of events) {
      if (eventTimestamp(event) < cutoff) {
        continue;
      }
      const status = normalizeStatus(event.status);
      if (status === "running") {
        running += 1;
      } else if (status === "queued") {
        queued += 1;
      } else if (status === "completed") {
        success += 1;
      } else if (
        status === "failed" ||
        status === "deadline" ||
        status === "cancelled" ||
        status === "invalid"
      ) {
        failed += 1;
      }
    }

    return { running, success, failed, queued };
  }, [events, windowValue]);

  return (
    <div className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Job execution
          </p>
          <p className="mt-1 text-sm text-slate-600">
            Running, successful, and failed jobs by time window.
          </p>
        </div>
        <label className="flex items-center gap-2 text-xs text-muted">
          <span className="uppercase tracking-[0.2em]">Window</span>
          <select
            value={windowValue}
            onChange={(event) => setWindowValue(event.target.value)}
            className="rounded-full border border-soft bg-white px-3 py-2 text-xs font-semibold text-slate-700"
          >
            {windows.map((entry) => (
              <option key={entry.value} value={entry.value}>
                {entry.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-4">
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Running
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-slate-900">
            {summary.running}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Queued
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-slate-900">
            {summary.queued}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Successful
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-emerald-700">
            {summary.success}
          </p>
        </div>
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Failed
          </p>
          <p className="mt-2 font-display text-lg font-semibold text-rose-700">
            {summary.failed}
          </p>
        </div>
      </div>
    </div>
  );
}
