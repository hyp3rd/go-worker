"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
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

const sourceSummary = (event: JobEvent) => {
  const meta = event.metadata ?? {};
  const source = (meta["job.source"] ?? "").toLowerCase();
  if (source === "tarball_url") {
    return meta["job.tarball_url"] || "tarball url";
  }
  if (source === "tarball_path") {
    return meta["job.tarball_path"] || "tarball path";
  }

  if (event.repo || event.tag) {
    return `${event.repo}@${event.tag}`;
  }

  return "n/a";
};

const eventTimestamp = (event: JobEvent) =>
  event.finishedAtMs ?? event.startedAtMs ?? 0;

const optimisticTTL = 10 * 60 * 1000;

const mergeEventList = (primary: JobEvent[], secondary: JobEvent[]) => {
  const byId = new Map<string, JobEvent>();
  for (const event of secondary) {
    byId.set(event.taskId, event);
  }
  for (const event of primary) {
    byId.set(event.taskId, event);
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
          setItems((prev) => mergeEventList(payload.events ?? [], prev));
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
            setItems((prev) => mergeEventList(payload.events ?? [], prev));
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

  useEffect(() => {
    const handler = (event: Event) => {
      const detail = (event as CustomEvent<JobEvent>).detail;
      if (!detail) {
        return;
      }
      setItems((prev) => mergeEventList(prev, [detail]));
    };

    window.addEventListener("job-run-start", handler as EventListener);
    return () => {
      window.removeEventListener("job-run-start", handler as EventListener);
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
                  <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-muted">
                    <span>
                      {displayTime ? (
                        <RelativeTime valueMs={displayTime} mode="past" />
                      ) : (
                        "n/a"
                      )}
                    </span>
                    <Link
                      href={`/jobs/runs/${encodeURIComponent(event.taskId)}`}
                      className="rounded-full border border-soft px-2 py-1 text-[10px] font-semibold text-muted"
                    >
                      View output
                    </Link>
                  </div>
                </div>

                <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-muted">
                  <span>{sourceSummary(event)}</span>
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
