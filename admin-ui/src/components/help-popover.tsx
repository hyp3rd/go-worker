"use client";

export function HelpPopover({
  title = "Help",
  items,
  align = "right",
  href,
  linkLabel = "Docs",
}: {
  title?: string;
  items: string[];
  align?: "left" | "right";
  href?: string;
  linkLabel?: string;
}) {
  if (items.length === 0) {
    return null;
  }

  const alignClass = align === "left" ? "left-0" : "right-0";

  return (
    <details className="relative">
      <summary className="flex h-9 w-9 cursor-pointer items-center justify-center rounded-full border border-soft bg-white text-sm font-semibold text-slate-700 shadow-soft outline-none transition hover:bg-[var(--card)] focus-visible:ring-2 focus-visible:ring-[var(--accent)] [&::-webkit-details-marker]:hidden">
        <span className="sr-only">Help</span>
        ?
      </summary>
      <div
        className={`absolute ${alignClass} z-20 mt-2 w-72 rounded-2xl border border-soft bg-white/95 p-4 text-sm text-slate-700 shadow-soft`}
      >
        <p className="text-xs uppercase tracking-[0.2em] text-muted">
          {title}
        </p>
        <ul className="mt-3 list-disc space-y-2 pl-5">
          {items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
        {href ? (
          <a
            href={href}
            className="mt-3 inline-flex items-center text-xs font-semibold text-[var(--accent-ink)] hover:underline"
          >
            {linkLabel}
          </a>
        ) : null}
      </div>
    </details>
  );
}
