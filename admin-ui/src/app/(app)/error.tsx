"use client";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="rounded-3xl border border-rose-200 bg-rose-50 p-6 text-rose-800 shadow-soft">
      <p className="text-xs uppercase tracking-[0.2em]">Error</p>
      <h2 className="mt-3 font-display text-2xl font-semibold">
        Something went wrong
      </h2>
      <p className="mt-2 text-sm">{error.message}</p>
      <button
        onClick={reset}
        className="mt-4 rounded-full bg-black px-4 py-2 text-xs font-semibold text-white"
      >
        Retry
      </button>
    </div>
  );
}
