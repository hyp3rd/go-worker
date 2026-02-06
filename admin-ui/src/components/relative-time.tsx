"use client";

import { useEffect, useState } from "react";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";

dayjs.extend(relativeTime);

type RelativeTimeProps = {
  valueMs?: number | null;
  fallback?: string;
  mode?: "future" | "past" | "auto";
  overdueLabel?: string;
  refreshMs?: number;
};

export function RelativeTime({
  valueMs,
  fallback = "n/a",
  mode = "auto",
  overdueLabel = "overdue",
  refreshMs = 15000,
}: RelativeTimeProps) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), refreshMs);
    return () => window.clearInterval(id);
  }, [refreshMs]);

  if (!valueMs || valueMs <= 0) {
    return <span>{fallback}</span>;
  }

  const diff = valueMs - now;
  if (mode === "future" && diff < 0) {
    return <span>{overdueLabel}</span>;
  }

  if (mode === "past" && diff > 0) {
    return <span>{fallback}</span>;
  }

  return <span>{dayjs(valueMs).from(now)}</span>;
}
