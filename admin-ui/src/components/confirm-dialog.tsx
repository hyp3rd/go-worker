"use client";

import { useMemo, useRef, useState } from "react";

type ConfirmTone = "default" | "danger";

type ConfirmState = {
  open: boolean;
  title: string;
  message: string;
  confirmLabel: string;
  cancelLabel: string;
  tone: ConfirmTone;
};

type ConfirmOptions = Partial<Omit<ConfirmState, "open">> & {
  title: string;
  message: string;
};

type Resolver = (value: boolean) => void;

export function useConfirmDialog() {
  const [state, setState] = useState<ConfirmState | null>(null);
  const resolverRef = useRef<Resolver | null>(null);

  const confirm = (options: ConfirmOptions) =>
    new Promise<boolean>((resolve) => {
      resolverRef.current = resolve;
      setState({
        open: true,
        title: options.title,
        message: options.message,
        confirmLabel: options.confirmLabel ?? "Confirm",
        cancelLabel: options.cancelLabel ?? "Cancel",
        tone: options.tone ?? "default",
      });
    });

  const close = (value: boolean) => {
    resolverRef.current?.(value);
    resolverRef.current = null;
    setState(null);
  };

  const dialogProps = useMemo(() => {
    if (!state) {
      return {
        open: false,
        title: "",
        message: "",
        confirmLabel: "Confirm",
        cancelLabel: "Cancel",
        tone: "default" as ConfirmTone,
        onConfirm: () => {},
        onCancel: () => {},
      };
    }
    return {
      ...state,
      onConfirm: () => close(true),
      onCancel: () => close(false),
    };
  }, [state]);

  return { confirm, dialogProps };
}

export function ConfirmDialog({
  open,
  title,
  message,
  confirmLabel,
  cancelLabel,
  tone,
  onConfirm,
  onCancel,
}: {
  open: boolean;
  title: string;
  message: string;
  confirmLabel: string;
  cancelLabel: string;
  tone: ConfirmTone;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  if (!open) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4">
      <div className="w-full max-w-lg rounded-3xl border border-soft bg-white p-6 shadow-soft">
        <p className="text-xs uppercase tracking-[0.2em] text-muted">Confirm</p>
        <h2 className="mt-2 text-lg font-semibold text-slate-900">{title}</h2>
        <p className="mt-2 text-sm text-slate-600">{message}</p>
        <div className="mt-6 flex flex-wrap justify-end gap-2">
          <button
            onClick={onCancel}
            className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
          >
            {cancelLabel}
          </button>
          <button
            onClick={onConfirm}
            className={`rounded-full px-4 py-2 text-xs font-semibold text-white ${
              tone === "danger"
                ? "bg-rose-600"
                : "bg-black"
            }`}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
