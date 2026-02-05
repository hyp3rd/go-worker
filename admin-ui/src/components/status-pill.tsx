export function StatusPill({ status }: { status: "healthy" | "lagging" | "paused" }) {
  const styles: Record<typeof status, string> = {
    healthy: "bg-emerald-100 text-emerald-700",
    lagging: "bg-amber-100 text-amber-700",
    paused: "bg-zinc-200 text-zinc-700",
  };

  return (
    <span className={`rounded-full px-2 py-1 text-xs font-semibold ${styles[status]}`}>
      {status}
    </span>
  );
}
