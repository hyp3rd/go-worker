"use client";

import { useEffect, useState } from "react";
import type { CoordinationStatus } from "@/lib/types";

type BannerState = {
  state: "ok" | "paused" | "degraded";
  message: string;
  lastChecked: number;
};

const refreshIntervalMs = 20000;

const buildState = (coordination?: CoordinationStatus | null): BannerState => {
  if (!coordination) {
    return {
      state: "degraded",
      message: "Admin gateway unreachable",
      lastChecked: Date.now(),
    };
  }
  if (coordination.paused) {
    return {
      state: "paused",
      message: "Durable dequeue is paused",
      lastChecked: Date.now(),
    };
  }
  return {
    state: "ok",
    message: "Gateway connected · durable dequeue active",
    lastChecked: Date.now(),
  };
};

export function StatusBanner({
  initialCoordination,
}: {
  initialCoordination: CoordinationStatus | null;
}) {
  const [status, setStatus] = useState<BannerState>(
    buildState(initialCoordination)
  );

  useEffect(() => {
    const refresh = async () => {
      try {
        const res = await fetch("/api/overview", { cache: "no-store" });
        if (!res.ok) {
          setStatus(buildState(null));
          return;
        }
        const payload = (await res.json()) as {
          coordination?: CoordinationStatus;
        };
        setStatus(buildState(payload.coordination ?? null));
      } catch {
        setStatus(buildState(null));
      }
    };

    refresh();
    const timer = setInterval(refresh, refreshIntervalMs);
    return () => clearInterval(timer);
  }, []);

  const bannerStyles =
    status.state === "ok"
      ? "border-emerald-200 bg-emerald-50 text-emerald-900"
      : status.state === "paused"
        ? "border-amber-200 bg-amber-50 text-amber-900"
        : "border-rose-200 bg-rose-50 text-rose-800";

  return (
    <div className={`rounded-2xl border px-4 py-3 text-sm ${bannerStyles}`}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="font-semibold">{status.message}</span>
        <span className="text-xs text-muted">
          checked {new Date(status.lastChecked).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}
