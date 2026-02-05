"use client";

import { useMemo, useState } from "react";
import type { DlqEntry } from "@/lib/types";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";

const PAGE_SIZE = 5;

export function DlqTable({ entries }: { entries: DlqEntry[] }) {
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);

  if (entries.length === 0) {
    return (
      <div className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-6 text-sm text-muted">
        No DLQ entries found for the current Redis prefix.
      </div>
    );
  }

  const filtered = useMemo(() => {
    return entries.filter((entry) => {
      const q = query.toLowerCase();
      return (
        entry.id.toLowerCase().includes(q) ||
        entry.queue.toLowerCase().includes(q) ||
        entry.handler.toLowerCase().includes(q)
      );
    });
  }, [entries, query]);

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
        placeholder="Search by id, queue, handler"
        onQuery={handleQuery}
        rightSlot={
          <button className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white">
            Replay selected
          </button>
        }
      />
      <Table columns={["ID", "Queue", "Handler", "Age", "Attempts"]}>
        {paged.map((entry) => (
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
