"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
  { href: "/", label: "Overview" },
  { href: "/queues", label: "Queues" },
  { href: "/schedules", label: "Schedules" },
  { href: "/dlq", label: "DLQ" },
  { href: "/settings", label: "Settings" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <nav className="mt-10 flex flex-col gap-2 text-sm">
      {navItems.map((item) => {
        const active = pathname === item.href;
        return (
          <Link
            key={item.href}
            href={item.href}
            className={`rounded-2xl px-3 py-2 font-semibold transition ${
              active
                ? "bg-black text-white"
                : "text-[var(--ink-subtle)] hover:bg-[var(--bg-muted)]"
            }`}
          >
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}
