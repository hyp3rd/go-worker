"use client";

import { useMemo, useState } from "react";
import type { JobSchedule } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { StatusPill } from "@/components/status-pill";

export function SchedulesGrid({ schedules }: { schedules: JobSchedule[] }) {
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");

  const filtered = useMemo(() => {
    return schedules.filter((job) => {
      const matchesName = job.name.toLowerCase().includes(query.toLowerCase());
      const matchesStatus = status === "all" || job.status === status;
      return matchesName && matchesStatus;
    });
  }, [query, schedules, status]);

  return (
    <div className="mt-6 space-y-4">
      <FilterBar
        placeholder="Search schedule"
        statusOptions={["all", "healthy", "lagging", "paused"]}
        onQuery={setQuery}
        onStatus={setStatus}
        rightSlot={
          <button className="rounded-full bg-[var(--accent)] px-4 py-2 text-xs font-semibold text-[var(--accent-ink)]">
            New schedule
          </button>
        }
      />
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
            <p className="mt-2 text-xs text-muted">{job.schedule}</p>
            <p className="mt-4 text-sm font-medium">
              Next run <span className="text-muted">{job.nextRun}</span>
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
