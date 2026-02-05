"use client";

import { useMemo, useState } from "react";
import type { QueueSummary } from "@/lib/types";
import { formatNumber } from "@/lib/format";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";

const PAGE_SIZE = 5;

export function QueuesTable({ queues }: { queues: QueueSummary[] }) {
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);

  const filtered = useMemo(() => {
    return queues.filter((queue) =>
      queue.name.toLowerCase().includes(query.toLowerCase())
    );
  }, [queues, query]);

  const paged = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return filtered.slice(start, start + PAGE_SIZE);
  }, [filtered, page]);

  const handleQuery = (value: string) => {
    setQuery(value);
    setPage(1);
  };

  return (
    <div className="mt-6 space-y-4">
      <FilterBar
        placeholder="Search queue name"
        onQuery={handleQuery}
        rightSlot={
          <button className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white">
            New queue
          </button>
        }
      />
      <Table columns={["Queue", "Ready", "Processing", "Dead", "Weight"]}>
        {paged.map((queue) => (
          <TableRow key={queue.name}>
            <TableCell>
              <span className="font-semibold">{queue.name}</span>
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
        pageSize={PAGE_SIZE}
        onNext={() => setPage((prev) => prev + 1)}
        onPrev={() => setPage((prev) => Math.max(1, prev - 1))}
      />
    </div>
  );
}
