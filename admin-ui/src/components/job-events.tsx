"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { RelativeTime } from "@/components/relative-time";
import { formatDuration } from "@/lib/format";
import type { JobEvent } from "@/lib/types";

const refreshIntervalMs = 15000;

const statusStyles: Record<string, string> = {
  completed: "bg-emerald-100 text-emerald-700",
  failed: "bg-rose-100 text-rose-700",
  cancelled: "bg-zinc-200 text-zinc-700",
  deadline: "bg-amber-100 text-amber-700",
  invalid: "bg-amber-100 text-amber-700",
  running: "bg-sky-100 text-sky-700",
  queued: "bg-slate-100 text-slate-700",
  rate_limited: "bg-slate-100 text-slate-700",
  unknown: "bg-slate-200 text-slate-700",
};

const normalizeStatus = (status: string) => {
  const key = status.toLowerCase();
  return statusStyles[key] ? key : "unknown";
};

export function JobEvents({ events }: { events: JobEvent[] }) {
  const [items, setItems] = useState<JobEvent[]>(events);
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const router = useRouter();

  const filterName = useMemo(() => {
    return searchParams.get("job")?.trim() ?? "";
  }, [searchParams]);

  useEffect(() => {
    setItems(events);
  }, [events]);

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;
    let source: EventSource | null = null;
    let active = true;

    const refresh = async () => {
      try {
        const res = await fetch(`/api/jobs/events?limit=25`, {
          cache: "no-store",
        });
        if (!res.ok) {
          return;
        }
        const payload = (await res.json()) as { events?: JobEvent[] };
        if (payload?.events) {
          setItems(payload.events);
        }
      } catch {
        // ignore polling failures
      }
    };

    const startPolling = () => {
      if (timer) {
        return;
      }
      timer = setInterval(refresh, refreshIntervalMs);
    };

    const startSSE = () => {
      source = new EventSource("/api/events");
      source.addEventListener("job_events", (event) => {
        try {
          const payload = JSON.parse(event.data) as {
            events?: JobEvent[];
          };
          if (payload?.events) {
            setItems(payload.events);
          }
        } catch {
          // ignore parsing errors
        }
      });
      source.onerror = () => {
        if (!active) {
          return;
        }
        source?.close();
        source = null;
        startPolling();
      };
    };

    refresh();
    startSSE();

    return () => {
      active = false;
      if (source) {
        source.close();
      }
      if (timer) {
        clearInterval(timer);
      }
    };
  }, []);

  const visibleItems = useMemo(() => {
    if (!filterName) {
      return items;
    }
    return items.filter((event) => event.name === filterName);
  }, [filterName, items]);

  const summary = useMemo(() => {
    const counts = {
      completed: 0,
      failed: 0,
      running: 0,
      total: visibleItems.length,
    };
    for (const event of visibleItems) {
      const status = normalizeStatus(event.status ?? "unknown");
      if (status === "completed") {
        counts.completed += 1;
      } else if (status === "failed") {
        counts.failed += 1;
      } else if (status === "running") {
        counts.running += 1;
      }
    }
    return counts;
  }, [visibleItems]);

  const clearFilter = () => {
    const params = new URLSearchParams(searchParams.toString());
    params.delete("job");
    const suffix = params.toString();
    const href = suffix ? `${pathname}?${suffix}#job-events` : `${pathname}#job-events`;
    router.replace(href, { scroll: false });
  };

  return (
    <section
      id="job-events"
      className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            job events
          </p>
          <p className="mt-1 text-sm text-slate-600">
            Execution logs for containerized jobs.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted">
          <span>{summary.total} recent</span>
          <span className="text-emerald-700">ok {summary.completed}</span>
          <span className="text-rose-600">failed {summary.failed}</span>
          <span className="text-slate-600">running {summary.running}</span>
        </div>
      </div>

      {filterName ? (
        <div className="mt-3 flex flex-wrap items-center gap-2 rounded-2xl border border-soft bg-white/80 px-3 py-2 text-xs text-slate-700">
          <span className="uppercase tracking-[0.2em] text-muted">
            filtered
          </span>
          <span className="rounded-full border border-soft bg-[var(--card)] px-2 py-1 text-[11px] font-semibold">
            {filterName}
          </span>
          <button
            type="button"
            onClick={clearFilter}
            className="rounded-full border border-soft px-2 py-1 text-[11px] font-semibold text-muted"
          >
            Clear
          </button>
        </div>
      ) : null}

      <div className="mt-4 space-y-3">
        {visibleItems.length === 0 ? (
          <p className="rounded-xl border border-soft bg-white/80 px-4 py-3 text-sm text-muted">
            {filterName ? "No runs for this job yet." : "No job runs yet."}
          </p>
        ) : (
          visibleItems.map((event) => {
            const statusKey = normalizeStatus(event.status ?? "unknown");
            const metadata = event.metadata
              ? Object.entries(event.metadata).sort(([a], [b]) =>
                  a.localeCompare(b)
                )
              : [];
            const finishedAt = event.finishedAtMs ?? 0;
            const startedAt = event.startedAtMs ?? 0;
            const displayTime = finishedAt || startedAt;

            return (
              <div
                key={`${event.taskId}-${displayTime}`}
                className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-slate-600"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-semibold text-slate-900">
                      {event.name}
                    </span>
                    <span
                      className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] ${statusStyles[statusKey]}`}
                    >
                      {statusKey}
                    </span>
                  </div>
                  <span className="text-[11px] uppercase tracking-[0.2em] text-muted">
                    {displayTime ? (
                      <RelativeTime valueMs={displayTime} mode="past" />
                    ) : (
                      "n/a"
                    )}
                  </span>
                </div>

                <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-muted">
                  <span>
                    {event.repo}@{event.tag}
                  </span>
                  <span>{event.path || "repo root"}</span>
                  <span>queue {event.queue || "default"}</span>
                  <span>
                    duration {formatDuration(event.durationMs ?? 0)}
                  </span>
                </div>

                {event.command ? (
                  <p className="mt-2 break-words text-sm text-slate-700">
                    command: <span className="font-medium">{event.command}</span>
                  </p>
                ) : null}

                {event.scheduleName || event.scheduleSpec ? (
                  <p className="mt-2 text-[11px] text-muted">
                    schedule{" "}
                    {event.scheduleName ? `${event.scheduleName}` : "n/a"} ·{" "}
                    {event.scheduleSpec ? event.scheduleSpec : "n/a"}
                  </p>
                ) : null}

                {event.error ? (
                  <p className="mt-2 break-words text-sm text-rose-700">
                    error: {event.error}
                  </p>
                ) : event.result ? (
                  <p className="mt-2 break-words text-sm text-slate-700">
                    result: {event.result}
                  </p>
                ) : null}

                {metadata.length > 0 ? (
                  <div className="mt-3 grid gap-2 md:grid-cols-2">
                    {metadata.map(([key, value]) => (
                      <div
                        key={`${event.taskId}-${key}`}
                        className="rounded-lg border border-soft bg-[var(--card)] px-3 py-2 text-[11px] text-slate-600"
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
            );
          })
        )}
      </div>
    </section>
  );
}
