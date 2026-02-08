export function HelpCallout({
  title = "Notes",
  items,
}: {
  title?: string;
  items: string[];
}) {
  if (items.length === 0) {
    return null;
  }

  return (
    <div className="mt-4 rounded-2xl border border-soft bg-[var(--card)] p-4">
      <p className="text-xs uppercase tracking-[0.2em] text-muted">{title}</p>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-700">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
}
