export function StatCard({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <div className="rounded-2xl border border-soft bg-white/80 p-6 shadow-soft">
      <p className="text-xs uppercase tracking-[0.2em] text-muted">{label}</p>
      <div className="mt-4 flex items-end justify-between">
        <span className="font-display text-3xl font-semibold">{value}</span>
        <span className="text-xs text-muted">{note}</span>
      </div>
    </div>
  );
}
