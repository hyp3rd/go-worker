"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";

type QueueWeightEditorProps = {
  name: string;
  weight: number;
};

export function QueueWeightEditor({ name, weight }: QueueWeightEditorProps) {
  const router = useRouter();
  const [value, setValue] = useState(String(weight));
  const [message, setMessage] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();
  const { confirm, dialogProps } = useConfirmDialog();

  const submit = async () => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      setMessage("Weight must be a positive number.");
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(`/api/queues/${encodeURIComponent(name)}/weight`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ weight: parsed }),
      });

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Failed to update weight");
      }

      setMessage("Queue weight updated.");
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Update failed");
    }
  };

  const reset = async () => {
    const ok = await confirm({
      title: "Reset queue weight to default?",
      message: "This resets the queue weight to the default value.",
      confirmLabel: "Reset weight",
      tone: "danger",
    });
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(`/api/queues/${encodeURIComponent(name)}/weight`, {
        method: "DELETE",
      });

      if (!res.ok) {
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Failed to reset weight");
      }

      setMessage("Queue weight reset to default.");
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Reset failed");
    }
  };

  return (
    <div className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4">
      <div className="flex flex-wrap items-end gap-3">
        <div>
          <label className="text-xs uppercase tracking-[0.2em] text-muted">
            Queue weight
          </label>
          <input
            value={value}
            onChange={(event) => setValue(event.target.value)}
            className="mt-2 w-32 rounded-2xl border border-soft bg-white px-3 py-2 text-sm"
            type="number"
            min={1}
          />
        </div>
        <button
          onClick={submit}
          disabled={pending}
          className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white disabled:cursor-not-allowed disabled:opacity-60"
        >
          Update weight
        </button>
        <button
          onClick={reset}
          disabled={pending}
          className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted disabled:cursor-not-allowed disabled:opacity-60"
        >
          Reset to default
        </button>
      </div>
      {message ? <p className="mt-2 text-xs text-muted">{message}</p> : null}
      <ConfirmDialog {...dialogProps} />
    </div>
  );
}
