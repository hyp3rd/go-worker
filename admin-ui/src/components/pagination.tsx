"use client";

export function Pagination({
  page,
  total,
  pageSize,
  pageSizeOptions,
  onNext,
  onPrev,
  onPageSizeChange,
}: {
  page: number;
  total: number;
  pageSize: number;
  pageSizeOptions?: number[];
  onNext: () => void;
  onPrev: () => void;
  onPageSizeChange?: (value: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const sizes = pageSizeOptions ?? [];

  return (
    <div className="flex items-center justify-between rounded-2xl border border-soft bg-white px-4 py-3 text-sm">
      <span className="text-muted">
        Page {page} of {totalPages}
      </span>
      <div className="flex flex-wrap items-center gap-3">
        {onPageSizeChange && sizes.length > 0 ? (
          <label className="flex items-center gap-2 text-xs text-muted">
            <span>Rows</span>
            <select
              value={pageSize}
              onChange={(event) =>
                onPageSizeChange(Number(event.target.value))
              }
              className="rounded-full border border-soft bg-white px-2 py-1 text-xs font-semibold"
            >
              {sizes.map((size) => (
                <option key={size} value={size}>
                  {size}
                </option>
              ))}
            </select>
          </label>
        ) : null}
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
    </div>
  );
}
