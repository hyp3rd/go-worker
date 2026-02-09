"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { RelativeTime } from "@/components/relative-time";
import type { JobEvent } from "@/lib/types";

const normalizeStatus = (value?: string) =>
  value ? value.toLowerCase() : "unknown";

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

export function JobRunDetail({
  taskId,
  events,
}: {
  taskId: string;
  events: JobEvent[];
}) {
  const [event, setEvent] = useState<JobEvent | null>(() => {
    return events.find((entry) => entry.taskId === taskId) ?? null;
  });

  useEffect(() => {
    let source: EventSource | null = null;
    let active = true;

    const refresh = async () => {
      try {
        const res = await fetch(`/api/jobs/events?limit=200`, {
          cache: "no-store",
        });
        if (!res.ok) {
          return;
        }
        const payload = (await res.json()) as { events?: JobEvent[] };
        if (payload?.events) {
          const found =
            payload.events.find((entry) => entry.taskId === taskId) ?? null;
          if (found) {
            setEvent(found);
          }
        }
      } catch {
        // ignore
      }
    };

    source = new EventSource("/api/events");
    source.addEventListener("job_events", (raw) => {
      try {
        const payload = JSON.parse(raw.data) as { events?: JobEvent[] };
        const found =
          payload?.events?.find((entry) => entry.taskId === taskId) ?? null;
        if (found) {
          setEvent(found);
        }
      } catch {
        // ignore
      }
    });
    source.onerror = () => {
      if (!active) {
        return;
      }
      source?.close();
      source = null;
    };

    refresh();

    return () => {
      active = false;
      source?.close();
    };
  }, [taskId]);

  const output = event?.error || event?.result || "";
  const statusKey = normalizeStatus(event?.status);
  const startedAt = event?.startedAtMs ?? 0;
  const finishedAt = event?.finishedAtMs ?? 0;
  const durationMs = event?.durationMs ?? 0;

  const metadata = useMemo(() => {
    if (!event?.metadata) {
      return [];
    }
    return Object.entries(event.metadata).sort(([a], [b]) =>
      a.localeCompare(b)
    );
  }, [event?.metadata]);

  return (
    <section className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            job run
          </p>
          <h1 className="mt-2 font-display text-2xl font-semibold text-slate-900">
            {event?.name ?? "Run not found"}
          </h1>
          <p className="mt-1 text-sm text-slate-600">Run ID: {taskId}</p>
        </div>
        <Link
          href="/jobs#job-events"
          className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
        >
          Back to jobs
        </Link>
      </div>

      {!event ? (
        <p className="mt-6 rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm text-muted">
          No run data yet. The run may still be starting or has expired from the
          event window.
        </p>
      ) : (
        <>
          <div className="mt-6 grid gap-3 md:grid-cols-4">
            <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
              <p className="uppercase tracking-[0.2em] text-muted">status</p>
              <p
                className={`mt-2 inline-flex rounded-full px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${
                  statusStyles[statusKey] ?? statusStyles.unknown
                }`}
              >
                {statusKey}
              </p>
            </div>
            <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
              <p className="uppercase tracking-[0.2em] text-muted">duration</p>
              <p className="mt-2 text-sm font-semibold text-slate-900">
                {durationMs ? `${durationMs}ms` : "n/a"}
              </p>
            </div>
            <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
              <p className="uppercase tracking-[0.2em] text-muted">started</p>
              <p className="mt-2 text-sm font-semibold text-slate-900">
                {startedAt ? <RelativeTime valueMs={startedAt} mode="past" /> : "n/a"}
              </p>
            </div>
            <div className="rounded-2xl border border-soft bg-[var(--card)] p-3 text-xs text-slate-600">
              <p className="uppercase tracking-[0.2em] text-muted">finished</p>
              <p className="mt-2 text-sm font-semibold text-slate-900">
                {finishedAt ? (
                  <RelativeTime valueMs={finishedAt} mode="past" />
                ) : (
                  "n/a"
                )}
              </p>
            </div>
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-2">
            <div className="rounded-2xl border border-soft bg-[var(--card)] p-4 text-sm text-slate-700">
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                run details
              </p>
              <div className="mt-3 space-y-2 text-sm">
                <p>
                  <span className="text-muted">Queue:</span>{" "}
                  {event.queue || "default"}
                </p>
                <p>
                  <span className="text-muted">Repo:</span>{" "}
                  {event.repo || "n/a"}
                </p>
                <p>
                  <span className="text-muted">Tag:</span>{" "}
                  {event.tag || "n/a"}
                </p>
                <p>
                  <span className="text-muted">Path:</span>{" "}
                  {event.path || "repo root"}
                </p>
                <p>
                  <span className="text-muted">Dockerfile:</span>{" "}
                  {event.dockerfile || "Dockerfile"}
                </p>
                <p>
                  <span className="text-muted">Command:</span>{" "}
                  {event.command || "default"}
                </p>
                {event.scheduleName ? (
                  <p>
                    <span className="text-muted">Schedule:</span>{" "}
                    {event.scheduleName} · {event.scheduleSpec || "n/a"}
                  </p>
                ) : null}
              </div>
            </div>

            <div className="rounded-2xl border border-soft bg-[var(--card)] p-4 text-sm text-slate-700">
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                output
              </p>
              {event.error ? (
                <div className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-rose-900">
                  <p className="text-[10px] uppercase tracking-[0.2em] text-rose-700">
                    error
                  </p>
                  <p className="mt-1 whitespace-pre-wrap break-words text-sm">
                    {event.error}
                  </p>
                </div>
              ) : null}
              {event.result ? (
                <div className="mt-3 rounded-xl border border-soft bg-white/80 px-3 py-2">
                  <p className="text-[10px] uppercase tracking-[0.2em] text-muted">
                    output
                  </p>
                  <p className="mt-1 whitespace-pre-wrap break-words text-sm text-slate-700">
                    {event.result}
                  </p>
                </div>
              ) : null}
              {!event.error && !event.result ? (
                <p className="mt-3 text-sm text-muted">No output captured.</p>
              ) : null}

              <div className="mt-4 flex flex-wrap gap-2">
                <button
                  onClick={() => navigator.clipboard.writeText(output)}
                  className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                >
                  Copy output
                </button>
                <button
                  onClick={() => {
                    const blob = new Blob([output], { type: "text/plain" });
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement("a");
                    link.href = url;
                    link.download = `${event.name}-${event.taskId}.log`;
                    link.click();
                    URL.revokeObjectURL(url);
                  }}
                  className="rounded-full border border-soft px-3 py-1 text-xs font-semibold text-muted"
                >
                  Download
                </button>
              </div>
            </div>
          </div>

          {metadata.length > 0 ? (
            <div className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4 text-xs text-slate-600">
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                metadata
              </p>
              <div className="mt-3 grid gap-2 md:grid-cols-2">
                {metadata.map(([key, value]) => (
                  <div
                    key={key}
                    className="rounded-xl border border-soft bg-white/80 px-3 py-2"
                  >
                    <span className="uppercase tracking-[0.16em] text-muted">
                      {key}
                    </span>
                    <p className="mt-1 break-words text-sm text-slate-700">
                      {value}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </>
      )}
    </section>
  );
}
