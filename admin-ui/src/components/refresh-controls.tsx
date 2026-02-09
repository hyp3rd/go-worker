"use client";

import { useEffect, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

const autoRefreshKey = "workerctl_auto_refresh";
const refreshIntervalMs = 15000;

const formatRefreshAge = (lastRefresh: number | null) => {
  if (!lastRefresh) {
    return "never";
  }
  const seconds = Math.max(0, Math.floor((Date.now() - lastRefresh) / 1000));
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ago`;
};

export function RefreshControls() {
  const router = useRouter();
  const [autoRefresh, setAutoRefresh] = useState(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return window.localStorage.getItem(autoRefreshKey) === "true";
  });
  const [lastRefresh, setLastRefresh] = useState<number | null>(null);
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    if (!autoRefresh) {
      return;
    }
    const timer = setInterval(() => {
      startTransition(() => {
        router.refresh();
        setLastRefresh(Date.now());
      });
    }, refreshIntervalMs);
    return () => clearInterval(timer);
  }, [autoRefresh, router, startTransition]);

  const toggleAuto = () => {
    const next = !autoRefresh;
    setAutoRefresh(next);
    window.localStorage.setItem(autoRefreshKey, next ? "true" : "false");
  };

  const handleRefresh = () => {
    startTransition(() => {
      router.refresh();
      setLastRefresh(Date.now());
    });
  };

  return (
    <div className="flex flex-wrap items-center gap-3">
      <button
        type="button"
        onClick={handleRefresh}
        disabled={pending}
        className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-semibold disabled:cursor-not-allowed disabled:opacity-60"
      >
        Refresh
      </button>
      <label className="flex items-center gap-2 text-xs text-muted">
        <input
          type="checkbox"
          checked={autoRefresh}
          onChange={toggleAuto}
          className="h-4 w-4 rounded border-soft"
        />
        Auto {Math.floor(refreshIntervalMs / 1000)}s
      </label>
      <span className="text-xs text-muted">
        Last refresh {formatRefreshAge(lastRefresh)}
      </span>
    </div>
  );
}
