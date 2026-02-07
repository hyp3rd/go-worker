"use client";

import { useMemo, useState } from "react";

export function FilterBar({
  placeholder,
  statusOptions,
  initialQuery,
  initialStatus,
  onQuery,
  onStatus,
  rightSlot,
}: {
  placeholder: string;
  statusOptions?: string[];
  initialQuery?: string;
  initialStatus?: string;
  onQuery: (value: string) => void;
  onStatus?: (value: string) => void;
  rightSlot?: React.ReactNode;
}) {
  const [query, setQuery] = useState(initialQuery ?? "");
  const [status, setStatus] = useState(
    initialStatus ?? statusOptions?.[0] ?? "all"
  );

  useMemo(() => {
    onQuery(query);
  }, [query, onQuery]);

  useMemo(() => {
    if (onStatus) {
      onStatus(status);
    }
  }, [onStatus, status]);

  return (
    <div className="flex flex-col gap-3 rounded-2xl border border-soft bg-[var(--card)] p-4 md:flex-row md:items-end md:justify-between">
      <div className="flex flex-1 flex-col gap-3 md:flex-row md:items-end">
        <div className="flex-1">
          <label className="text-xs uppercase tracking-[0.2em] text-muted">
            Search
          </label>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-2 text-sm"
            placeholder={placeholder}
          />
        </div>
        {statusOptions ? (
          <div className="min-w-[160px]">
            <label className="text-xs uppercase tracking-[0.2em] text-muted">
              Status
            </label>
            <select
              value={status}
              onChange={(event) => setStatus(event.target.value)}
              className="mt-2 w-full rounded-2xl border border-soft bg-white px-3 py-2 text-sm"
            >
              {statusOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>
        ) : null}
      </div>
      {rightSlot ? (
        <div className="md:self-end md:pt-6">{rightSlot}</div>
      ) : null}
    </div>
  );
}
