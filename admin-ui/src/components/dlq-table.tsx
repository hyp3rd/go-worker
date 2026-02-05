"use client";

import { useRouter, useSearchParams } from "next/navigation";
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
}: {
  entries: DlqEntry[];
  total: number;
  page: number;
  pageSize: number;
  query: string;
}) {
  const router = useRouter();
  const searchParams = useSearchParams();

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
