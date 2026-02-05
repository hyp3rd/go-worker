"use client";

export function Pagination({
  page,
  total,
  pageSize,
  onNext,
  onPrev,
}: {
  page: number;
  total: number;
  pageSize: number;
  onNext: () => void;
  onPrev: () => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="flex items-center justify-between rounded-2xl border border-soft bg-white px-4 py-3 text-sm">
      <span className="text-muted">
        Page {page} of {totalPages}
      </span>
      <div className="flex gap-2">
        <button
          onClick={onPrev}
          disabled={page <= 1}
          className="rounded-full border border-soft px-3 py-1 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-50"
        >
          Previous
        </button>
        <button
          onClick={onNext}
          disabled={page >= totalPages}
          className="rounded-full border border-soft px-3 py-1 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-50"
        >
          Next
        </button>
      </div>
    </div>
  );
}
