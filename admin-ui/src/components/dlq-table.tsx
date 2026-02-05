"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import type { DlqEntry } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";

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

  useEffect(() => {
    setQueueValue(queueFilter);
  }, [queueFilter]);

  useEffect(() => {
    setHandlerValue(handlerFilter);
  }, [handlerFilter]);

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

  return (
    <div className="mt-6 space-y-4">
      <FilterBar
        placeholder="Search by id, queue, handler"
        initialQuery={query}
        onQuery={handleQuery}
        rightSlot={
          <button className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white">
            Replay selected
          </button>
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
      <Table columns={["ID", "Queue", "Handler", "Age", "Attempts"]}>
        {entries.map((entry) => (
          <TableRow key={entry.id}>
            <TableCell>
              <span className="font-mono text-xs">{entry.id}</span>
            </TableCell>
            <TableCell>{entry.queue}</TableCell>
            <TableCell>{entry.handler}</TableCell>
            <TableCell>{entry.age}</TableCell>
            <TableCell>{entry.attempts}</TableCell>
          </TableRow>
        ))}
      </Table>
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
      />
    </div>
  );
}
