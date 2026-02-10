"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { Pagination } from "@/components/pagination";
import { RelativeTime } from "@/components/relative-time";
import { formatDuration } from "@/lib/format";
import type { AdminAuditEvent, JobEvent, ScheduleEvent } from "@/lib/types";

const refreshIntervalMs = 15000;
const timelinePageSizes = [10, 25, 50];
const timelineFetchLimit = 200;
const timelineStateKey = "workerctl_ops_timeline_v1";

type TimelineSource = "job" | "schedule" | "audit" | "queue" | "dlq";

type TimelineItem = {
  id: string;
  source: TimelineSource;
  status: string;
  atMs: number;
  title: string;
  detail: string;
  meta: string;
  taskId?: string;
  requestId?: string;
};

type TimelineState = {
  sourceFilter: string;
  statusFilter: string;
  rangeFilter: string;
  searchFilter: string;
  pageSize: number;
};

const sourceLabel: Record<TimelineSource, string> = {
  job: "job",
  schedule: "schedule",
  audit: "audit",
  queue: "queue",
  dlq: "dlq",
};

const loadTimelineState = (): TimelineState | null => {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(timelineStateKey);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Partial<TimelineState>;
    const pageSize = timelinePageSizes.includes(parsed.pageSize ?? 0)
      ? (parsed.pageSize as number)
      : timelinePageSizes[0];
    return {
      sourceFilter: parsed.sourceFilter ?? "all",
      statusFilter: parsed.statusFilter ?? "all",
      rangeFilter: parsed.rangeFilter ?? "7d",
      searchFilter: parsed.searchFilter ?? "",
      pageSize,
    };
  } catch {
    return null;
  }
};

const statusStyles: Record<string, string> = {
  success: "bg-emerald-100 text-emerald-700",
  failed: "bg-rose-100 text-rose-700",
  running: "bg-sky-100 text-sky-700",
  queued: "bg-slate-100 text-slate-700",
  other: "bg-zinc-100 text-zinc-700",
};

const normalizeSourceFromAuditAction = (action: string): TimelineSource => {
  if (action.startsWith("queue.")) {
    return "queue";
  }
  if (action.startsWith("dlq.")) {
    return "dlq";
  }
  return "audit";
};

const normalizeStatusBucket = (status: string): string => {
  const key = status.toLowerCase();
  if (key === "ok" || key === "completed" || key === "success") {
    return "success";
  }
  if (key === "failed" || key === "error" || key === "deadline" || key === "invalid") {
    return "failed";
  }
  if (key === "running") {
    return "running";
  }
  if (key === "queued" || key === "rate_limited") {
    return "queued";
  }
  return "other";
};

const eventTime = (item: TimelineItem) => item.atMs;

const mergeByID = <T extends { id: string; atMs: number }>(incoming: T[], existing: T[], maxItems: number) => {
  const byID = new Map<string, T>();
  for (const item of existing) {
    byID.set(item.id, item);
  }
  for (const item of incoming) {
    const current = byID.get(item.id);
    if (!current || item.atMs >= current.atMs) {
      byID.set(item.id, item);
    }
  }
  return Array.from(byID.values())
    .sort((left, right) => right.atMs - left.atMs)
    .slice(0, maxItems);
};

const mergeJobEvents = (incoming: JobEvent[], existing: JobEvent[], maxItems: number) => {
  const normalizedIncoming = incoming.map((event) => ({
    ...event,
    id: event.taskId,
    atMs: event.finishedAtMs ?? event.startedAtMs ?? 0,
  }));
  const normalizedExisting = existing.map((event) => ({
    ...event,
    id: event.taskId,
    atMs: event.finishedAtMs ?? event.startedAtMs ?? 0,
  }));
  return mergeByID(normalizedIncoming, normalizedExisting, maxItems).map((item) => {
    const event = { ...item };
    delete (event as { id?: string }).id;
    delete (event as { atMs?: number }).atMs;
    return event;
  });
};

const mergeScheduleEvents = (incoming: ScheduleEvent[], existing: ScheduleEvent[], maxItems: number) => {
  const normalizedIncoming = incoming.map((event) => ({
    ...event,
    id: event.taskId,
    atMs: event.finishedAtMs ?? event.startedAtMs ?? 0,
  }));
  const normalizedExisting = existing.map((event) => ({
    ...event,
    id: event.taskId,
    atMs: event.finishedAtMs ?? event.startedAtMs ?? 0,
  }));
  return mergeByID(normalizedIncoming, normalizedExisting, maxItems).map((item) => {
    const event = { ...item };
    delete (event as { id?: string }).id;
    delete (event as { atMs?: number }).atMs;
    return event;
  });
};

const mergeAuditEvents = (incoming: AdminAuditEvent[], existing: AdminAuditEvent[], maxItems: number) => {
  const normalizedIncoming = incoming.map((event) => ({
    ...event,
    id: `${event.requestId}:${event.atMs}:${event.action}:${event.target}`,
  }));
  const normalizedExisting = existing.map((event) => ({
    ...event,
    id: `${event.requestId}:${event.atMs}:${event.action}:${event.target}`,
  }));
  return mergeByID(normalizedIncoming, normalizedExisting, maxItems).map((item) => {
    const event = { ...item };
    delete (event as { id?: string }).id;
    return event;
  });
};

const toJobTimeline = (event: JobEvent): TimelineItem => {
  const atMs = event.finishedAtMs ?? event.startedAtMs ?? 0;
  const status = normalizeStatusBucket(event.status ?? "other");
  const duration = event.durationMs ? formatDuration(event.durationMs) : "n/a";
  const queue = event.queue || "default";
  const detail = event.error
    ? `error: ${event.error}`
    : event.result
      ? `result: ${event.result}`
      : "no output";
  return {
    id: `job:${event.taskId}`,
    source: "job",
    status,
    atMs,
    title: event.name || "job",
    detail,
    meta: `queue ${queue} · duration ${duration}`,
    taskId: event.taskId,
  };
};

const toScheduleTimeline = (event: ScheduleEvent): TimelineItem => {
  const atMs = event.finishedAtMs ?? event.startedAtMs ?? 0;
  const status = normalizeStatusBucket(event.status ?? "other");
  const duration = event.durationMs ? formatDuration(event.durationMs) : "n/a";
  const queue = event.queue || "default";
  const detail = event.error
    ? `error: ${event.error}`
    : event.result
      ? `result: ${event.result}`
      : "no output";
  return {
    id: `schedule:${event.taskId}`,
    source: "schedule",
    status,
    atMs,
    title: event.name || "schedule",
    detail,
    meta: `${event.spec} · queue ${queue} · duration ${duration}`,
    taskId: event.taskId,
  };
};

const toAuditTimeline = (event: AdminAuditEvent): TimelineItem => {
  const source = normalizeSourceFromAuditAction(event.action);
  return {
    id: `audit:${event.requestId}:${event.atMs}:${event.action}:${event.target}`,
    source,
    status: normalizeStatusBucket(event.status),
    atMs: event.atMs,
    title: event.action || "audit",
    detail: event.detail || "no detail",
    meta: `${event.target || "n/a"} · actor ${event.actor || "unknown"}`,
    requestId: event.requestId,
  };
};

export function OperationsTimeline({
  jobs,
  schedules,
  audits,
}: {
  jobs: JobEvent[];
  schedules: ScheduleEvent[];
  audits: AdminAuditEvent[];
}) {
  const [initialState] = useState<TimelineState | null>(() => loadTimelineState());
  const [jobItems, setJobItems] = useState<JobEvent[]>(jobs);
  const [scheduleItems, setScheduleItems] = useState<ScheduleEvent[]>(schedules);
  const [auditItems, setAuditItems] = useState<AdminAuditEvent[]>(audits);
  const [sourceFilter, setSourceFilter] = useState(initialState?.sourceFilter ?? "all");
  const [statusFilter, setStatusFilter] = useState(initialState?.statusFilter ?? "all");
  const [rangeFilter, setRangeFilter] = useState(initialState?.rangeFilter ?? "7d");
  const [searchFilter, setSearchFilter] = useState(initialState?.searchFilter ?? "");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialState?.pageSize ?? timelinePageSizes[0]);
  const [nowMs, setNowMs] = useState(0);

  useEffect(() => {
    setJobItems(jobs);
  }, [jobs]);

  useEffect(() => {
    setScheduleItems(schedules);
  }, [schedules]);

  useEffect(() => {
    setAuditItems(audits);
  }, [audits]);

  useEffect(() => {
    const bootstrapID = window.setTimeout(() => setNowMs(Date.now()), 0);
    const intervalID = window.setInterval(() => setNowMs(Date.now()), refreshIntervalMs);
    return () => {
      window.clearTimeout(bootstrapID);
      window.clearInterval(intervalID);
    };
  }, []);

  useEffect(() => {
    let active = true;
    let source: EventSource | null = null;
    let timer: number | null = null;

    const refresh = async () => {
      try {
        const [jobsRes, schedulesRes, auditRes] = await Promise.all([
          fetch(`/api/jobs/events?limit=${timelineFetchLimit}`, { cache: "no-store" }),
          fetch(`/api/schedules/events?limit=${timelineFetchLimit}`, { cache: "no-store" }),
          fetch(`/api/audit?limit=${timelineFetchLimit}`, { cache: "no-store" }),
        ]);

        if (jobsRes.ok) {
          const payload = (await jobsRes.json()) as { events?: JobEvent[] };
          if (payload.events) {
            setJobItems(payload.events);
          }
        }
        if (schedulesRes.ok) {
          const payload = (await schedulesRes.json()) as { events?: ScheduleEvent[] };
          if (payload.events) {
            setScheduleItems(payload.events);
          }
        }
        if (auditRes.ok) {
          const payload = (await auditRes.json()) as { events?: AdminAuditEvent[] };
          if (payload.events) {
            setAuditItems(payload.events);
          }
        }
      } catch {
        // ignore polling failures
      }
    };

    const startPolling = () => {
      if (timer) {
        return;
      }
      timer = window.setInterval(refresh, refreshIntervalMs);
    };

    const mergeJobPayload = (events: JobEvent[]) => {
      setJobItems((previous) => mergeJobEvents(events, previous, timelineFetchLimit));
    };

    const mergeSchedulePayload = (events: ScheduleEvent[]) => {
      setScheduleItems((previous) =>
        mergeScheduleEvents(events, previous, timelineFetchLimit)
      );
    };

    const mergeAuditPayload = (events: AdminAuditEvent[]) => {
      setAuditItems((previous) => mergeAuditEvents(events, previous, timelineFetchLimit));
    };

    const startSSE = () => {
      source = new EventSource("/api/events");
      source.addEventListener("job_events", (event) => {
        try {
          const payload = JSON.parse(event.data) as { events?: JobEvent[] };
          if (payload.events) {
            mergeJobPayload(payload.events);
          }
        } catch {
          // ignore malformed events
        }
      });
      source.addEventListener("schedule_events", (event) => {
        try {
          const payload = JSON.parse(event.data) as { events?: ScheduleEvent[] };
          if (payload.events) {
            mergeSchedulePayload(payload.events);
          }
        } catch {
          // ignore malformed events
        }
      });
      source.addEventListener("audit_events", (event) => {
        try {
          const payload = JSON.parse(event.data) as { events?: AdminAuditEvent[] };
          if (payload.events) {
            mergeAuditPayload(payload.events);
          }
        } catch {
          // ignore malformed events
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
        window.clearInterval(timer);
      }
    };
  }, []);

  const timeline = useMemo(() => {
    const items: TimelineItem[] = [];
    jobItems.forEach((event) => items.push(toJobTimeline(event)));
    scheduleItems.forEach((event) => items.push(toScheduleTimeline(event)));
    auditItems.forEach((event) => items.push(toAuditTimeline(event)));
    return items.sort((left, right) => eventTime(right) - eventTime(left));
  }, [auditItems, jobItems, scheduleItems]);

  const filtered = useMemo(() => {
    let list = timeline;

    if (sourceFilter !== "all") {
      list = list.filter((item) => item.source === sourceFilter);
    }

    if (statusFilter !== "all") {
      list = list.filter((item) => item.status === statusFilter);
    }

    if (searchFilter.trim() !== "") {
      const query = searchFilter.trim().toLowerCase();
      list = list.filter((item) => {
        const haystack = `${item.title} ${item.detail} ${item.meta}`.toLowerCase();
        return haystack.includes(query);
      });
    }

    if (rangeFilter !== "all" && nowMs > 0) {
      let windowMs = 0;
      if (rangeFilter === "24h") {
        windowMs = 24 * 60 * 60 * 1000;
      } else if (rangeFilter === "7d") {
        windowMs = 7 * 24 * 60 * 60 * 1000;
      } else if (rangeFilter === "30d") {
        windowMs = 30 * 24 * 60 * 60 * 1000;
      }
      const cutoff = nowMs - windowMs;
      list = list.filter((item) => item.atMs >= cutoff);
    }

    return list;
  }, [nowMs, rangeFilter, searchFilter, sourceFilter, statusFilter, timeline]);

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, statusFilter, searchFilter, rangeFilter]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const state: TimelineState = {
      sourceFilter,
      statusFilter,
      rangeFilter,
      searchFilter,
      pageSize,
    };
    window.localStorage.setItem(timelineStateKey, JSON.stringify(state));
  }, [pageSize, rangeFilter, searchFilter, sourceFilter, statusFilter]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const paged = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return filtered.slice(start, start + pageSize);
  }, [currentPage, filtered, pageSize]);

  return (
    <section className="rounded-3xl border border-soft bg-white/95 p-6 shadow-soft">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="font-display text-2xl font-semibold">Operations timeline</h2>
          <p className="text-sm text-muted">
            Correlated job, schedule, queue, DLQ, and audit events in one stream.
          </p>
        </div>
        <div className="text-xs text-muted">
          {filtered.length} events · {jobItems.length} job · {scheduleItems.length} schedule ·{" "}
          {auditItems.length} audit
        </div>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-4">
        <input
          value={searchFilter}
          onChange={(event) => setSearchFilter(event.target.value)}
          placeholder="Search title, detail, or metadata"
          className="rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
        />
        <select
          value={sourceFilter}
          onChange={(event) => setSourceFilter(event.target.value)}
          className="rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
        >
          <option value="all">all sources</option>
          <option value="job">job</option>
          <option value="schedule">schedule</option>
          <option value="queue">queue</option>
          <option value="dlq">dlq</option>
          <option value="audit">audit</option>
        </select>
        <select
          value={statusFilter}
          onChange={(event) => setStatusFilter(event.target.value)}
          className="rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
        >
          <option value="all">all status</option>
          <option value="success">success</option>
          <option value="running">running</option>
          <option value="queued">queued</option>
          <option value="failed">failed</option>
          <option value="other">other</option>
        </select>
        <select
          value={rangeFilter}
          onChange={(event) => setRangeFilter(event.target.value)}
          className="rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
        >
          <option value="all">all time</option>
          <option value="24h">last 24h</option>
          <option value="7d">last 7d</option>
          <option value="30d">last 30d</option>
        </select>
      </div>

      <div className="mt-4 space-y-3">
        {paged.length === 0 ? (
          <p className="rounded-xl border border-soft bg-white/80 px-4 py-3 text-sm text-muted">
            No events matched the current filters.
          </p>
        ) : (
          paged.map((item) => {
            const style = statusStyles[item.status] ?? statusStyles.other;
            const canViewRun = item.source === "job" && item.taskId;
            return (
              <article
                key={item.id}
                className="rounded-2xl border border-soft bg-[var(--card)] p-4"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-full bg-white px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-muted">
                      {sourceLabel[item.source]}
                    </span>
                    <span className={`rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${style}`}>
                      {item.status}
                    </span>
                    <span className="text-sm font-semibold text-slate-900">{item.title}</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs">
                    <span className="uppercase tracking-[0.16em] text-muted">
                      <RelativeTime valueMs={item.atMs} mode="past" />
                    </span>
                    {canViewRun ? (
                      <Link
                        href={`/jobs/runs/${item.taskId}`}
                        className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                      >
                        View run
                      </Link>
                    ) : null}
                  </div>
                </div>
                <p className="mt-2 break-words text-sm text-slate-700">{item.detail}</p>
                <p className="mt-1 text-xs text-muted">{item.meta}</p>
                {item.requestId ? (
                  <p className="mt-1 text-[11px] text-muted">request {item.requestId}</p>
                ) : null}
              </article>
            );
          })
        )}
      </div>

      <div className="mt-4">
        <Pagination
          page={currentPage}
          total={filtered.length}
          pageSize={pageSize}
          pageSizeOptions={timelinePageSizes}
          onNext={() => setPage((value) => Math.min(pageCount, value + 1))}
          onPrev={() => setPage((value) => Math.max(1, value - 1))}
          onPageSizeChange={(value) => {
            setPageSize(value);
            setPage(1);
          }}
        />
      </div>
    </section>
  );
}
