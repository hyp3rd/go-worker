export default function Loading() {
  return (
    <div className="space-y-6">
      <div className="grid gap-6 md:grid-cols-3">
        {[0, 1, 2].map((card) => (
          <div
            key={card}
            className="h-28 animate-pulse rounded-2xl border border-soft bg-white/60"
          />
        ))}
      </div>
      <div className="grid gap-6 lg:grid-cols-[2fr_1fr]">
        <div className="h-72 animate-pulse rounded-3xl border border-soft bg-white/60" />
        <div className="h-72 animate-pulse rounded-3xl border border-soft bg-white/60" />
      </div>
      <div className="h-64 animate-pulse rounded-3xl border border-soft bg-white/60" />
    </div>
  );
}
