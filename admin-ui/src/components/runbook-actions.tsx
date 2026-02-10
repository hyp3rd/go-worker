"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { AdminAuditEvent } from "@/lib/types";
import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";

type RunbookActionsProps = {
  paused: boolean;
  initialAuditEvents: AdminAuditEvent[];
};

const replayDefault = 100;
const replayMax = 1000;
const auditExportDefault = 500;
const auditExportMax = 5000;
type AuditExportFormat = "jsonl" | "json" | "csv";

const formatError = (error: unknown) => {
  if (error instanceof Error) {
    return error.message;
  }
  return "Action failed";
};

const maxAuditItems = 20;

const summarizeAction = (action: string) => {
  const normalized = action.replaceAll("_", " ").replaceAll(".", " ").trim();
  if (!normalized) {
    return "action";
  }
  return normalized;
};

export function RunbookActions({ paused, initialAuditEvents }: RunbookActionsProps) {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();
  const [auditEvents, setAuditEvents] = useState<AdminAuditEvent[]>(initialAuditEvents);
  const [replayLimit, setReplayLimit] = useState(replayDefault);
  const [exportFormat, setExportFormat] = useState<AuditExportFormat>("jsonl");
  const [exportLimit, setExportLimit] = useState(auditExportDefault);
  const [exportAction, setExportAction] = useState("");
  const [exportTarget, setExportTarget] = useState("");
  const { confirm, dialogProps } = useConfirmDialog();

  useEffect(() => {
    setAuditEvents(initialAuditEvents);
  }, [initialAuditEvents]);

  useEffect(() => {
    const source = new EventSource("/api/events");
    source.addEventListener("audit_events", (event) => {
      try {
        const payload = JSON.parse(event.data) as { events?: AdminAuditEvent[] };
        if (payload.events) {
          setAuditEvents(payload.events.slice(0, maxAuditItems));
        }
      } catch {
        // keep previous audit events on parse failure
      }
    });
    return () => {
      source.close();
    };
  }, []);

  const runAction = async (action: "pause" | "resume" | "replay") => {
    setMessage(null);

    try {
      if (action === "pause") {
        const ok = await confirm({
          title: "Pause durable dequeue?",
          message:
            "Active workers will stop pulling new durable tasks until resumed.",
          confirmLabel: "Pause dequeue",
        });
        if (!ok) {
          return;
        }
      }
      if (action === "resume") {
        const ok = await confirm({
          title: "Resume durable dequeue?",
          message: "Workers will start processing durable tasks again.",
          confirmLabel: "Resume dequeue",
        });
        if (!ok) {
          return;
        }
      }
      if (action === "replay") {
        const ok = await confirm({
          title: "Replay DLQ items?",
          message: `Replay up to ${replayLimit} DLQ item(s). Replays are at-least-once.`,
          confirmLabel: "Replay DLQ",
        });
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
      const optimistic: AdminAuditEvent = {
        action,
        status: "ok",
        target: "*",
        actor: "admin-ui",
        requestId: "pending",
        payloadHash: "",
        metadata: {},
        atMs: Date.now(),
        detail,
      };
      setAuditEvents((current) => [optimistic, ...current].slice(0, maxAuditItems));

      startTransition(() => {
        router.refresh();
      });
    } catch (error) {
      setMessage(formatError(error));
    }
  };

  const triggerAuditExport = () => {
    const query = new URLSearchParams();
    query.set("format", exportFormat);
    query.set(
      "limit",
      String(Math.max(1, Math.min(auditExportMax, exportLimit || auditExportDefault)))
    );

    const action = exportAction.trim();
    if (action) {
      query.set("action", action);
    }

    const target = exportTarget.trim();
    if (target) {
      query.set("target", target);
    }

    const link = document.createElement("a");
    link.href = `/api/audit/export?${query.toString()}`;
    link.style.display = "none";
    document.body.append(link);
    link.click();
    link.remove();
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
      <div className="rounded-2xl border border-soft bg-[var(--card)] px-4 py-3 text-sm">
        <div className="flex items-center justify-between">
          <span className="font-semibold">Export audit</span>
          <span className="text-xs text-muted">csv/json/jsonl</span>
        </div>
        <div className="mt-2 grid gap-2 md:grid-cols-2">
          <input
            value={exportAction}
            onChange={(event) => setExportAction(event.target.value)}
            className="rounded-xl border border-soft bg-white px-3 py-2 text-xs"
            placeholder="action filter (optional)"
          />
          <input
            value={exportTarget}
            onChange={(event) => setExportTarget(event.target.value)}
            className="rounded-xl border border-soft bg-white px-3 py-2 text-xs"
            placeholder="target filter (optional)"
          />
        </div>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <select
            value={exportFormat}
            onChange={(event) =>
              setExportFormat(event.target.value as AuditExportFormat)
            }
            className="rounded-xl border border-soft bg-white px-3 py-2 text-xs"
          >
            <option value="jsonl">jsonl</option>
            <option value="json">json</option>
            <option value="csv">csv</option>
          </select>
          <input
            type="number"
            min={1}
            max={auditExportMax}
            value={exportLimit}
            onChange={(event) =>
              setExportLimit(
                Math.max(1, Math.min(auditExportMax, Number(event.target.value || 1)))
              )
            }
            className="w-24 rounded-xl border border-soft bg-white px-3 py-2 text-xs"
          />
          <button
            type="button"
            onClick={triggerAuditExport}
            className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-semibold"
          >
            Download
          </button>
          <span className="text-[11px] text-muted">max {auditExportMax}</span>
        </div>
      </div>
      {message ? <p className="text-xs text-muted">{message}</p> : null}
      <div className="rounded-2xl border border-soft bg-white/60 p-3">
        <p className="text-xs uppercase tracking-[0.2em] text-muted">
          audit trail
        </p>
        <div className="mt-3 space-y-3">
          {auditEvents.length === 0 ? (
            <p className="text-xs text-muted">No runbook actions yet.</p>
          ) : (
            auditEvents.slice(0, 5).map((event) => (
              <div
                key={`${event.requestId}-${event.atMs}-${event.action}`}
                className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-slate-600"
              >
                <div className="flex items-center justify-between text-[11px] uppercase tracking-[0.18em] text-muted">
                  <span>{new Date(event.atMs).toLocaleTimeString()}</span>
                  <span className="rounded-full border border-soft bg-[var(--card)] px-2 py-0.5 text-[10px] font-semibold text-slate-600">
                    {summarizeAction(event.action)}
                  </span>
                </div>
                <p className="mt-2 text-sm font-medium text-slate-800">
                  {event.detail ?? "Action completed."}
                </p>
              </div>
            ))
          )}
        </div>
      </div>
      <ConfirmDialog {...dialogProps} />
    </div>
  );
}
