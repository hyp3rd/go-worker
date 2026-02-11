"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import type { QueueSummary } from "@/lib/types";
import { formatNumber } from "@/lib/format";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";
import { NoticeBanner } from "@/components/notice-banner";
import type { ErrorDiagnostic } from "@/lib/error-diagnostics";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";
import { loadSectionState, persistSectionState } from "@/lib/section-state";
import { throwAPIResponseError } from "@/lib/fetch-api-error";

const defaultPageSize = 5;
const queuesStateKey = "workerctl_queues_state_v1";

type QueuesViewState = {
  query: string;
  page: number;
  pageSize: number;
};

const isQueuesViewState = (value: unknown): value is QueuesViewState => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Partial<QueuesViewState>;
  return (
    typeof candidate.query === "string" &&
    typeof candidate.page === "number" &&
    Number.isFinite(candidate.page) &&
    candidate.page >= 1 &&
    typeof candidate.pageSize === "number" &&
    Number.isFinite(candidate.pageSize) &&
    candidate.pageSize > 0
  );
};

export function QueuesTable({ queues }: { queues: QueueSummary[] }) {
  const initialState = loadSectionState<QueuesViewState>(
    queuesStateKey,
    { query: "", page: 1, pageSize: defaultPageSize },
    isQueuesViewState
  );
  const router = useRouter();
  const searchParams = useSearchParams();
  const [query, setQuery] = useState(initialState.query);
  const [page, setPage] = useState(initialState.page);
  const [pageSize, setPageSize] = useState(initialState.pageSize);
  const [createOpen, setCreateOpen] = useState(
    () => searchParams.get("create") === "1"
  );
  const [createName, setCreateName] = useState("");
  const [createWeight, setCreateWeight] = useState("1");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
    diagnostic?: ErrorDiagnostic;
  } | null>(null);
  const [pending, startTransition] = useTransition();

  const filtered = useMemo(() => {
    return queues.filter((queue) =>
      queue.name.toLowerCase().includes(query.toLowerCase())
    );
  }, [queues, query]);

  const paged = useMemo(() => {
    const start = (page - 1) * pageSize;
    return filtered.slice(start, start + pageSize);
  }, [filtered, page, pageSize]);

  const handleQuery = (value: string) => {
    setQuery(value);
    setPage(1);
  };

  const handlePageSize = (value: number) => {
    setPageSize(value);
    setPage(1);
  };

  useEffect(() => {
    persistSectionState<QueuesViewState>(queuesStateKey, {
      query,
      page,
      pageSize,
    });
  }, [page, pageSize, query]);

  const createQueue = async () => {
    const name = createName.trim();
    const weightValue = Number(createWeight);
    if (!name) {
      setMessage({ tone: "error", text: "Queue name is required." });
      return;
    }
    if (!Number.isFinite(weightValue) || weightValue <= 0) {
      setMessage({ tone: "error", text: "Weight must be a positive number." });
      return;
    }

    setMessage(null);
    try {
      const res = await fetch(
        `/api/queues/${encodeURIComponent(name)}/weight`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ weight: weightValue }),
        }
      );

      if (!res.ok) {
        await throwAPIResponseError(res, "Queue create failed");
      }

      setMessage({ tone: "success", text: "Queue created." });
      setCreateOpen(false);
      setCreateName("");
      setCreateWeight("1");
      startTransition(() => router.refresh());
    } catch (error) {
      const diagnostic = parseErrorDiagnostic(error, "Queue create failed");
      setMessage({
        tone: "error",
        text: diagnostic.message,
        diagnostic,
      });
    }
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
        placeholder="Search queue name"
        initialQuery={query}
        onQuery={handleQuery}
        rightSlot={
          <button
            onClick={() => setCreateOpen((prev) => !prev)}
            className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white"
          >
            {createOpen ? "Close" : "New queue"}
          </button>
        }
      />
      {createOpen ? (
        <div className="rounded-2xl border border-soft bg-[var(--card)] p-4">
          <div className="grid gap-3 md:grid-cols-[2fr_1fr] md:items-end">
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Queue name
              </label>
              <input
                value={createName}
                onChange={(event) => setCreateName(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                placeholder="critical"
              />
            </div>
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Weight
              </label>
              <input
                value={createWeight}
                onChange={(event) => setCreateWeight(event.target.value)}
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
                type="number"
                min={1}
              />
            </div>
          </div>
          <div className="mt-4 flex justify-end">
            <button
              onClick={createQueue}
              disabled={pending}
              className="rounded-full bg-black px-4 py-2 text-xs font-semibold text-white disabled:opacity-50"
            >
              Create queue
            </button>
          </div>
        </div>
      ) : null}
      <Table
        columns={[
          { key: "queue", label: "Queue" },
          { key: "ready", label: "Ready" },
          { key: "processing", label: "Processing" },
          { key: "dead", label: "Dead" },
          { key: "weight", label: "Weight" },
        ]}
      >
        {paged.map((queue) => (
          <TableRow key={queue.name}>
            <TableCell>
              <Link
                href={`/queues/${encodeURIComponent(queue.name)}`}
                className="font-semibold text-black hover:text-black/70"
              >
                {queue.name}
              </Link>
              {queue.paused ? (
                <span className="ml-2 rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-700">
                  paused
                </span>
              ) : null}
            </TableCell>
            <TableCell>{formatNumber(queue.ready)}</TableCell>
            <TableCell>{formatNumber(queue.processing)}</TableCell>
            <TableCell>{formatNumber(queue.dead)}</TableCell>
            <TableCell>{queue.weight}</TableCell>
          </TableRow>
        ))}
      </Table>
      <Pagination
        page={page}
        total={filtered.length}
        pageSize={pageSize}
        onNext={() => setPage((prev) => prev + 1)}
        onPrev={() => setPage((prev) => Math.max(1, prev - 1))}
        onPageSizeChange={handlePageSize}
        pageSizeOptions={[5, 10, 25]}
      />
    </div>
  );
}
