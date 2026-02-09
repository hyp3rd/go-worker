"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";

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
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const cancelButtonRef = useRef<HTMLButtonElement | null>(null);
  const restoreFocusRef = useRef<HTMLElement | null>(null);
  const titleID = useId();
  const descriptionID = useId();

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    restoreFocusRef.current = document.activeElement as HTMLElement | null;
    cancelButtonRef.current?.focus();

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onCancel();

        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const root = dialogRef.current;
      if (!root) {
        return;
      }

      const focusableElements = root.querySelectorAll<HTMLElement>(
        'button,[href],input,select,textarea,[tabindex]:not([tabindex="-1"])',
      );
      if (focusableElements.length === 0) {
        event.preventDefault();
        root.focus();

        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];
      const activeElement = document.activeElement;

      if (event.shiftKey && activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();

        return;
      }

      if (!event.shiftKey && activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    };

    document.addEventListener("keydown", onKeyDown);

    return () => {
      document.removeEventListener("keydown", onKeyDown);
      restoreFocusRef.current?.focus();
    };
  }, [onCancel, open]);

  if (!open) {
    return null;
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 px-4"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onCancel();
        }
      }}
    >
      <div
        ref={dialogRef}
        role={tone === "danger" ? "alertdialog" : "dialog"}
        aria-modal="true"
        aria-labelledby={titleID}
        aria-describedby={descriptionID}
        tabIndex={-1}
        className="w-full max-w-lg rounded-3xl border border-soft bg-white p-6 shadow-soft"
      >
        <p className="text-xs uppercase tracking-[0.2em] text-muted">Confirm</p>
        <h2 id={titleID} className="mt-2 text-lg font-semibold text-slate-900">
          {title}
        </h2>
        <p id={descriptionID} className="mt-2 text-sm text-slate-600">
          {message}
        </p>
        <div className="mt-6 flex flex-wrap justify-end gap-2">
          <button
            ref={cancelButtonRef}
            type="button"
            onClick={onCancel}
            className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
          >
            {cancelLabel}
          </button>
          <button
            type="button"
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
