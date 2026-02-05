"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { readAuditEvents, recordAuditEvent, type AuditEvent } from "@/lib/audit";

type RunbookActionsProps = {
  paused: boolean;
};

const replayDefault = 100;
const replayMax = 1000;

const formatError = (error: unknown) => {
  if (error instanceof Error) {
    return error.message;
  }
  return "Action failed";
};

export function RunbookActions({ paused }: RunbookActionsProps) {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();
  const [auditEvents, setAuditEvents] = useState<AuditEvent[]>([]);
  const [replayLimit, setReplayLimit] = useState(replayDefault);

  useEffect(() => {
    setAuditEvents(readAuditEvents());
  }, []);

  const runAction = async (action: "pause" | "resume" | "replay") => {
    setMessage(null);

    try {
      if (action === "pause") {
        const ok = window.confirm(
          "Pause durable dequeue? Active workers will stop pulling new durable tasks."
        );
        if (!ok) {
          return;
        }
      }
      if (action === "resume") {
        const ok = window.confirm(
          "Resume durable dequeue? Workers will start processing durable tasks again."
        );
        if (!ok) {
          return;
        }
      }
      if (action === "replay") {
        const ok = window.confirm(
          `Replay up to ${replayLimit} DLQ item(s)? Replays are at-least-once.`
        );
        if (!ok) {
          return;
        }
      }

      const res = await fetch(
        action === "replay" ? "/api/actions/dlq/replay" : `/api/actions/${action}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body:
            action === "replay"
              ? JSON.stringify({ limit: replayLimit })
              : undefined,
        }
      );

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Action failed");
      }

      const payload = (await res.json()) as { message?: string };
      const detail = payload.message ?? "Action complete";
      setMessage(detail);
      const event: AuditEvent = {
        action,
        at: Date.now(),
        detail,
      };
      recordAuditEvent(event);
      setAuditEvents((current) => [event, ...current].slice(0, 20));

      startTransition(() => {
        router.refresh();
      });
    } catch (error) {
      setMessage(formatError(error));
    }
  };

  return (
    <div className="mt-6 space-y-3">
      <button
        onClick={() => runAction(paused ? "resume" : "pause")}
        disabled={pending}
        className="flex w-full items-center justify-between rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-60"
      >
        {paused ? "Resume durable dequeue" : "Pause durable dequeue"}
        <span className="text-xs text-muted">cmd</span>
      </button>
      <div className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm">
        <div className="flex items-center justify-between">
          <span className="font-semibold">Replay DLQ</span>
          <span className="text-xs text-muted">cmd</span>
        </div>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <label className="text-xs text-muted" htmlFor="dlq-limit">
            Limit
          </label>
          <input
            id="dlq-limit"
            type="number"
            min={1}
            max={replayMax}
            value={replayLimit}
            onChange={(event) =>
              setReplayLimit(
                Math.max(1, Math.min(replayMax, Number(event.target.value || 1)))
              )
            }
            className="w-24 rounded-xl border border-soft bg-white px-3 py-2 text-xs"
          />
          <button
            onClick={() => runAction("replay")}
            disabled={pending}
            className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-60"
          >
            Replay
          </button>
          <span className="text-[11px] text-muted">
            At-least-once. Max {replayMax}.
          </span>
        </div>
      </div>
      <button
        disabled
        className="flex w-full items-center justify-between rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm font-semibold opacity-40"
      >
        Export snapshot
        <span className="text-xs text-muted">cmd</span>
      </button>
      {message ? <p className="text-xs text-muted">{message}</p> : null}
      <div className="rounded-2xl border border-soft bg-white/60 p-3">
        <p className="text-xs uppercase tracking-[0.2em] text-muted">
          audit trail
        </p>
        <div className="mt-2 space-y-2 text-xs text-muted">
          {auditEvents.length === 0 ? (
            <p>No runbook actions yet.</p>
          ) : (
            auditEvents.slice(0, 5).map((event) => (
              <div
                key={`${event.action}-${event.at}`}
                className="flex items-center justify-between"
              >
                <span className="font-semibold text-slate-700">
                  {event.action}
                </span>
                <span>
                  {new Date(event.at).toLocaleTimeString()}
                  {event.detail ? ` · ${event.detail}` : ""}
                </span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
