"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useState, useTransition } from "react";
import type { DlqEntry, DlqEntryDetail } from "@/lib/types";
import { formatDuration, formatNumber } from "@/lib/format";
import { throwAPIResponseError } from "@/lib/fetch-api-error";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";
import { RelativeTime } from "@/components/relative-time";
import { ConfirmDialog, useConfirmDialog } from "@/components/confirm-dialog";
import { NoticeBanner } from "@/components/notice-banner";
import type { ErrorDiagnostic } from "@/lib/error-diagnostics";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";

export function DlqTable({
  entries,
  total,
  page,
  pageSize,
  query,
  queueFilter,
  handlerFilter,
}: {
  entries: DlqEntry[];
  total: number;
  page: number;
  pageSize: number;
  query: string;
  queueFilter: string;
  handlerFilter: string;
}) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [queueValue, setQueueValue] = useState(queueFilter);
  const [handlerValue, setHandlerValue] = useState(handlerFilter);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [message, setMessage] = useState<{
    tone: "success" | "error" | "info";
    text: string;
    diagnostic?: ErrorDiagnostic;
  } | null>(null);
  const [pending, startTransition] = useTransition();
  const [detail, setDetail] = useState<DlqEntryDetail | null>(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const { confirm, dialogProps } = useConfirmDialog();

  useEffect(() => {
    setQueueValue(queueFilter);
  }, [queueFilter]);

  useEffect(() => {
    setHandlerValue(handlerFilter);
  }, [handlerFilter]);

  useEffect(() => {
    setSelected((current) => {
      if (entries.length === 0) {
        return new Set();
      }
      const next = new Set<string>();
      entries.forEach((entry) => {
        if (current.has(entry.id)) {
          next.add(entry.id);
        }
      });
      return next;
    });
  }, [entries]);

  const updateParams = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());
    Object.entries(updates).forEach(([key, value]) => {
      if (!value) {
        params.delete(key);
      } else {
        params.set(key, value);
      }
    });

    const queryString = params.toString();
    router.push(queryString ? `/dlq?${queryString}` : "/dlq");
  };

  const handleQuery = (value: string) => {
    if (value === query) {
      return;
    }

    updateParams({ query: value.trim() || null, page: "1" });
  };

  const applyFilters = () => {
    updateParams({
      queue: queueValue.trim() || null,
      handler: handlerValue.trim() || null,
      page: "1",
    });
  };

  const clearFilters = () => {
    setQueueValue("");
    setHandlerValue("");
    updateParams({ queue: null, handler: null, page: "1" });
  };

  const handleNext = () => {
    updateParams({ page: String(page + 1) });
  };

  const handlePrev = () => {
    updateParams({ page: String(Math.max(1, page - 1)) });
  };

  const handlePageSize = (value: number) => {
    updateParams({ limit: String(value), page: "1" });
  };

  const toggleSelect = (id: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const allSelected = entries.length > 0 && selected.size === entries.length;

  const toggleAll = () => {
    if (allSelected) {
      setSelected(new Set());
      return;
    }

    setSelected(new Set(entries.map((entry) => entry.id)));
  };

  const replaySelected = async () => {
    if (selected.size === 0) {
      return;
    }

    const ok = await confirm({
      title: `Replay ${selected.size} DLQ item(s)?`,
      message: "Replays are at-least-once. Ensure handlers are idempotent.",
      confirmLabel: "Replay selected",
    });
    if (!ok) {
      return;
    }

    setMessage(null);
    const ids = Array.from(selected);

    try {
      const res = await fetch("/api/actions/dlq/replay-ids", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids }),
      });

      if (!res.ok) {
        await throwAPIResponseError(res, "Replay failed");
      }

      const payload = (await res.json()) as { message?: string };
      setMessage({
        tone: "success",
        text: payload.message ?? "Replay complete",
      });
      setSelected(new Set());
      startTransition(() => {
        router.refresh();
      });
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Replay failed");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
  };

  const copySelected = async () => {
    if (selected.size === 0) {
      return;
    }

    try {
      await navigator.clipboard.writeText(Array.from(selected).join("\n"));
      setMessage({
        tone: "info",
        text: `Copied ${selected.size} ID(s) to clipboard.`,
      });
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Copy failed");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
  };

  const replayOne = async (id: string) => {
    const ok = await confirm({
      title: "Replay this DLQ item?",
      message: "Replays are at-least-once. Ensure handlers are idempotent.",
      confirmLabel: "Replay item",
    });
    if (!ok) {
      return;
    }

    setMessage(null);
    try {
      const res = await fetch("/api/actions/dlq/replay-ids", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids: [id] }),
      });

      if (!res.ok) {
        await throwAPIResponseError(res, "Replay failed");
      }

      const payload = (await res.json()) as { message?: string };
      setMessage({
        tone: "success",
        text: payload.message ?? "Replay complete",
      });
      startTransition(() => {
        router.refresh();
      });
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Replay failed");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
  };

  const copyOne = async (id: string) => {
    try {
      await navigator.clipboard.writeText(id);
      setMessage({ tone: "info", text: "Copied ID to clipboard." });
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Copy failed");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
  };

  const openDetail = async (id: string) => {
    setDetailOpen(true);
    setDetailLoading(true);
    setDetail(null);

    try {
      const res = await fetch(`/api/dlq/${encodeURIComponent(id)}`, {
        cache: "no-store",
      });
      if (!res.ok) {
        await throwAPIResponseError(res, "Failed to load DLQ entry");
      }
      const payload = (await res.json()) as { entry?: DlqEntryDetail };
      if (payload?.entry) {
        setDetail(payload.entry);
      } else {
        throw new Error("DLQ entry unavailable");
      }
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Failed to load DLQ entry");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    } finally {
      setDetailLoading(false);
    }
  };

  const closeDetail = () => {
    setDetailOpen(false);
    setDetail(null);
  };

  return (
    <div className="mt-6 space-y-4">
      {message ? (
        <NoticeBanner
          tone={message.tone}
          text={message.text}
          diagnostic={message.diagnostic}
        />
      ) : null}
      <FilterBar
        placeholder="Search by id, queue, handler"
        initialQuery={query}
        onQuery={handleQuery}
        rightSlot={
          <div className="flex flex-wrap gap-2">
            <button
              onClick={copySelected}
              disabled={selected.size === 0}
              className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted disabled:cursor-not-allowed disabled:opacity-50"
            >
              Copy IDs
            </button>
            <button
              onClick={replaySelected}
              disabled={selected.size === 0 || pending}
              className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white disabled:cursor-not-allowed disabled:opacity-50"
            >
              Replay selected {selected.size > 0 ? `(${selected.size})` : ""}
            </button>
          </div>
        }
      />
      <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
        <div className="grid gap-3 md:grid-cols-3 md:items-end">
          <div>
            <label className="text-xs uppercase tracking-[0.2em] text-muted">
              Queue
            </label>
            <input
              value={queueValue}
              onChange={(event) => setQueueValue(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
              placeholder="default"
            />
          </div>
          <div>
            <label className="text-xs uppercase tracking-[0.2em] text-muted">
              Handler
            </label>
            <input
              value={handlerValue}
              onChange={(event) => setHandlerValue(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
              placeholder="send_email"
            />
          </div>
          <div className="flex gap-2">
            <button
              onClick={applyFilters}
              className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white"
            >
              Apply
            </button>
            <button
              onClick={clearFilters}
              className="rounded-full border border-soft px-4 py-2 text-xs font-semibold text-muted"
            >
              Clear
            </button>
          </div>
        </div>
      </div>
      <Table
        columns={[
          {
            key: "select",
            label: (
              <input
                type="checkbox"
                checked={allSelected}
                onChange={toggleAll}
                aria-label="Select all DLQ entries"
              />
            ),
          },
          { key: "id", label: "ID" },
          { key: "queue", label: "Queue" },
          { key: "handler", label: "Handler" },
          { key: "age", label: "Age" },
          { key: "attempts", label: "Attempts" },
          { key: "actions", label: "Actions" },
        ]}
      >
        {entries.map((entry) => (
          <TableRow key={entry.id}>
            <TableCell>
              <input
                type="checkbox"
                checked={selected.has(entry.id)}
                onChange={() => toggleSelect(entry.id)}
                aria-label={`Select ${entry.id}`}
              />
            </TableCell>
            <TableCell>
              <span className="font-mono text-xs">{entry.id}</span>
            </TableCell>
            <TableCell>{entry.queue}</TableCell>
            <TableCell>{entry.handler}</TableCell>
            <TableCell>{entry.age}</TableCell>
            <TableCell>{entry.attempts}</TableCell>
            <TableCell>
              <div className="flex flex-wrap gap-2">
                <button
                  onClick={() => openDetail(entry.id)}
                  className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-slate-700"
                >
                  Inspect
                </button>
                <button
                  onClick={() => replayOne(entry.id)}
                  className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-slate-700"
                >
                  Replay
                </button>
                <button
                  onClick={() => copyOne(entry.id)}
                  className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-muted"
                >
                  Copy
                </button>
              </div>
            </TableCell>
          </TableRow>
        ))}
      </Table>
      {detailOpen ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                DLQ entry detail
              </p>
              <p className="mt-1 font-mono text-xs text-slate-700">
                {detail?.id ?? "loading..."}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                onClick={() => detail?.id && copyOne(detail.id)}
                className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-muted"
              >
                Copy ID
              </button>
              <button
                onClick={() => detail?.id && replayOne(detail.id)}
                className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-slate-700"
              >
                Replay
              </button>
              <button
                onClick={closeDetail}
                className="rounded-full border border-soft px-3 py-1 text-[11px] font-semibold text-muted"
              >
                Close
              </button>
            </div>
          </div>

          {detailLoading ? (
            <p className="mt-4 text-sm text-muted">Loading detail...</p>
          ) : detail ? (
            <>
              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Queue</p>
                  <p className="mt-1 text-sm text-slate-900">
                    {detail.queue || "default"}
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Handler</p>
                  <p className="mt-1 text-sm text-slate-900">
                    {detail.handler || "n/a"}
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Attempts</p>
                  <p className="mt-1 text-sm text-slate-900">
                    {detail.attempts}
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Age</p>
                  <p className="mt-1 text-sm text-slate-900">
                    {formatDuration(detail.ageMs)}
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Failed</p>
                  <p className="mt-1 text-sm text-slate-900">
                    <RelativeTime valueMs={detail.failedAtMs} mode="past" />
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Updated</p>
                  <p className="mt-1 text-sm text-slate-900">
                    <RelativeTime valueMs={detail.updatedAtMs} mode="past" />
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted md:col-span-2">
                  <p className="uppercase tracking-[0.16em]">Last error</p>
                  <p className="mt-1 break-words text-sm text-slate-900">
                    {detail.lastError || "n/a"}
                  </p>
                </div>
                <div className="rounded-xl border border-soft bg-white/80 p-3 text-xs text-muted">
                  <p className="uppercase tracking-[0.16em]">Payload size</p>
                  <p className="mt-1 text-sm text-slate-900">
                    {formatNumber(detail.payloadSize)} bytes
                  </p>
                </div>
              </div>

              {detail.metadata && Object.keys(detail.metadata).length > 0 ? (
                <div className="mt-4 grid gap-2 md:grid-cols-2">
                  {Object.entries(detail.metadata).map(([key, value]) => (
                    <div
                      key={`${detail.id}-${key}`}
                      className="rounded-lg border border-soft bg-white/80 px-3 py-2 text-[11px] text-slate-600"
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
            </>
          ) : (
            <p className="mt-4 text-sm text-muted">No detail available.</p>
          )}
        </div>
      ) : null}
      {total === 0 ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-6 text-sm text-muted">
          No DLQ entries found for the current filters.
        </div>
      ) : null}
      <Pagination
        page={page}
        total={total}
        pageSize={pageSize}
        onNext={handleNext}
        onPrev={handlePrev}
        onPageSizeChange={handlePageSize}
        pageSizeOptions={[5, 10, 25, 50]}
      />
      <ConfirmDialog {...dialogProps} />
    </div>
  );
}
