"use client";

import Link from "next/link";
import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { QueueSummary } from "@/lib/types";
import { formatNumber } from "@/lib/format";
import { FilterBar } from "@/components/filters";
import { Pagination } from "@/components/pagination";
import { Table, TableCell, TableRow } from "@/components/table";

const PAGE_SIZE = 5;

export function QueuesTable({ queues }: { queues: QueueSummary[] }) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createWeight, setCreateWeight] = useState("1");
  const [message, setMessage] = useState<{
    tone: "success" | "error";
    text: string;
  } | null>(null);
  const [pending, startTransition] = useTransition();

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
        const payload = (await res.json()) as { error?: string };
        throw new Error(payload?.error ?? "Queue create failed");
      }

      setMessage({ tone: "success", text: "Queue created." });
      setCreateOpen(false);
      setCreateName("");
      setCreateWeight("1");
      startTransition(() => router.refresh());
    } catch (error) {
      setMessage({
        tone: "error",
        text: error instanceof Error ? error.message : "Queue create failed",
      });
    }
  };

  return (
    <div className="mt-6 space-y-4">
      {message ? (
        <div
          className={`rounded-2xl border px-4 py-3 text-sm ${
            message.tone === "success"
              ? "border-emerald-200 bg-emerald-50 text-emerald-800"
              : "border-rose-200 bg-rose-50 text-rose-800"
          }`}
        >
          {message.text}
        </div>
      ) : null}
      <FilterBar
        placeholder="Search queue name"
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
