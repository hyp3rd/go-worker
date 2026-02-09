"use client";

import { useEffect, useMemo, useState } from "react";

type SessionExpiryProps = {
  initialExpiresAt?: number | null;
};

const refreshIntervalMs = 60_000;

const formatCountdown = (remainingMs: number) => {
  const totalSeconds = Math.max(0, Math.floor(remainingMs / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m`;
  }
  return `${Math.max(1, totalSeconds)}s`;
};

export function SessionExpiry({ initialExpiresAt }: SessionExpiryProps) {
  const [expiresAt, setExpiresAt] = useState<number | null>(
    initialExpiresAt ?? null
  );
  const [now, setNow] = useState(0);

  useEffect(() => {
    const bootstrapId = window.setTimeout(() => setNow(Date.now()), 0);
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => {
      clearTimeout(bootstrapId);
      clearInterval(tick);
    };
  }, []);

  useEffect(() => {
    const refresh = async () => {
      try {
        const res = await fetch("/api/auth/session", {
          method: "GET",
          credentials: "include",
          cache: "no-store",
        });
        if (!res.ok) {
          return;
        }
        const payload = (await res.json()) as {
          expiresAt?: number;
        };
        if (typeof payload.expiresAt === "number") {
          setExpiresAt(payload.expiresAt);
        }
      } catch {
        // ignore refresh errors
      }
    };

    refresh();
    const timer = setInterval(refresh, refreshIntervalMs);
    return () => clearInterval(timer);
  }, []);

  const label = useMemo(() => {
    if (!expiresAt) {
      return "";
    }
    const remaining = expiresAt - now;
    if (remaining <= 0) {
      return "expired";
    }
    return `expires in ${formatCountdown(remaining)}`;
  }, [expiresAt, now]);

  if (!label) {
    return null;
  }

  return (
    <span className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-medium">
      Session {label}
    </span>
  );
}
