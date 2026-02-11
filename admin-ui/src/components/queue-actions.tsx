"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";
import { throwAPIResponseError } from "@/lib/fetch-api-error";
import { NoticeBanner } from "@/components/notice-banner";
import type { ErrorDiagnostic } from "@/lib/error-diagnostics";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";

export function QueueActions({
  name,
  paused,
}: {
  name: string;
  paused: boolean;
}) {
  const router = useRouter();
  const [isPaused, setIsPaused] = useState(paused);
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
    diagnostic?: ErrorDiagnostic;
  } | null>(null);
  const [pending, startTransition] = useTransition();
  const { confirm, dialogProps } = useConfirmDialog();

  useEffect(() => {
    setIsPaused(paused);
  }, [paused]);

  const togglePause = async () => {
    const next = !isPaused;
    const ok = await confirm({
      title: next ? `Pause queue "${name}"?` : `Resume queue "${name}"?`,
      message: next
        ? "Pending tasks will remain queued until resumed."
        : "Tasks will be eligible for dequeue immediately.",
      confirmLabel: next ? "Pause queue" : "Resume queue",
    });
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(
        `/api/queues/${encodeURIComponent(name)}/pause`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ paused: next }),
        }
      );

      if (!res.ok) {
        await throwAPIResponseError(res, "Failed to update queue state");
      }

      const payload = (await res.json()) as {
        queue?: { paused?: boolean };
      };
      const nextPaused =
        typeof payload.queue?.paused === "boolean" ? payload.queue.paused : next;
      setIsPaused(nextPaused);
      setMessage({
        tone: "success",
        text: nextPaused ? "Queue paused." : "Queue resumed.",
      });
      startTransition(() => router.refresh());
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Failed to update queue");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
  };

  return (
    <div className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">
            Queue state
          </p>
          <p className="mt-2 text-sm font-semibold text-slate-900">
            {isPaused ? "Paused" : "Active"}
          </p>
          <p className="mt-1 text-xs text-muted">
            {isPaused
              ? "Dequeue is disabled for this queue."
              : "Tasks can be dequeued normally."}
          </p>
        </div>
        <button
          onClick={togglePause}
          disabled={pending}
          className={`rounded-full px-4 py-2 text-xs font-semibold transition disabled:cursor-not-allowed disabled:opacity-60 ${
            isPaused
              ? "border border-soft bg-white text-slate-700"
              : "bg-amber-500 text-white"
          }`}
        >
          {isPaused ? "Resume queue" : "Pause queue"}
        </button>
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
      <ConfirmDialog {...dialogProps} />
    </div>
  );
}
