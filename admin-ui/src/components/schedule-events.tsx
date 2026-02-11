"use client";

import { useEffect, useState } from "react";
import { throwAPIResponseError } from "@/lib/fetch-api-error";
import { NoticeBanner } from "@/components/notice-banner";
import type { ErrorDiagnostic } from "@/lib/error-diagnostics";
import { RelativeTime } from "@/components/relative-time";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";
import { formatDuration } from "@/lib/format";
import type { ScheduleEvent } from "@/lib/types";

const refreshIntervalMs = 15000;

const statusStyles: Record<string, string> = {
  completed: "bg-emerald-100 text-emerald-700",
  failed: "bg-rose-100 text-rose-700",
  cancelled: "bg-zinc-200 text-zinc-700",
  deadline: "bg-amber-100 text-amber-700",
  invalid: "bg-amber-100 text-amber-700",
  unknown: "bg-slate-200 text-slate-700",
};

const normalizeStatus = (status: string) => {
  const key = status.toLowerCase();
  return statusStyles[key] ? key : "unknown";
};

export function ScheduleEvents({ events }: { events: ScheduleEvent[] }) {
  const [items, setItems] = useState<ScheduleEvent[]>(events);
  const [message, setMessage] = useState<{
    tone: "error" | "info";
    text: string;
    diagnostic?: ErrorDiagnostic;
  } | null>(null);

  useEffect(() => {
    setItems(events);
  }, [events]);

  useEffect(() => {
    let timer: ReturnType<typeof setInterval> | null = null;
    let source: EventSource | null = null;
    let active = true;

    const refresh = async () => {
      try {
        const res = await fetch(`/api/schedules/events?limit=25`, {
          cache: "no-store",
        });
        if (!res.ok) {
          await throwAPIResponseError(res, "Failed to load schedule events");
        }
        const payload = (await res.json()) as { events?: ScheduleEvent[] };
        if (payload?.events) {
          setItems(payload.events);
        }
        setMessage((current) => (current?.tone === "error" ? null : current));
      } catch (error) {
        const diagnostic = parseErrorDiagnostic(
          error,
          "Failed to load schedule events"
        );
        setMessage({
          tone: "error",
          text: diagnostic.message,
          diagnostic,
        });
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
      source.addEventListener("schedule_events", (event) => {
        try {
          const payload = JSON.parse(event.data) as {
            events?: ScheduleEvent[];
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
        setMessage({
          tone: "info",
          text: "Live stream disconnected. Falling back to polling.",
        });
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

  return (
    <section
      id="schedule-events"
      className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            schedule events
          </p>
          <p className="mt-1 text-sm text-slate-600">
            Live execution results for scheduled jobs.
          </p>
        </div>
        <span className="text-xs text-muted">
          {items.length} recent
        </span>
      </div>
      {message ? (
        <div className="mt-3">
          <NoticeBanner
            tone={message.tone}
            text={message.text}
            diagnostic={message.diagnostic}
          />
        </div>
      ) : null}

      <div className="mt-4 space-y-3">
        {items.length === 0 ? (
          <p className="rounded-xl border border-soft bg-white/80 px-4 py-3 text-sm text-muted">
            No schedule runs yet.
          </p>
        ) : (
          items.map((event) => {
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
                  <span>{event.spec}</span>
                  <span>{event.durable ? "durable" : "in-memory"}</span>
                  <span>queue {event.queue || "default"}</span>
                  <span>
                    duration {formatDuration(event.durationMs ?? 0)}
                  </span>
                </div>

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
