import { Nav } from "@/components/nav";
import { SessionActions } from "@/components/session-actions";
import { SessionExpiry } from "@/components/session-expiry";
import { StatusBanner } from "@/components/status-banner";
import type { CoordinationStatus } from "@/lib/types";

type AuditNotice = {
  message: string;
  timestamp: number;
};

export function AppShell({
  children,
  audit,
  sessionExpiry,
  coordination,
}: {
  children: React.ReactNode;
  audit?: AuditNotice | null;
  sessionExpiry?: number | null;
  coordination?: CoordinationStatus | null;
}) {
  const isPaused = coordination?.paused ?? false;
  const liveLabel = coordination
    ? isPaused
      ? "Paused"
      : "Active"
    : "Unknown";
  const liveDot = coordination
    ? isPaused
      ? "bg-amber-400"
      : "bg-emerald-400"
    : "bg-slate-400";

  return (
    <div className="min-h-screen bg-grid">
      <div className="mx-auto flex min-h-screen max-w-7xl gap-6 px-6 pb-20 pt-8">
        <aside className="hidden w-60 flex-shrink-0 lg:block">
          <div className="rounded-3xl border border-soft bg-white/80 p-5 shadow-soft">
            <p className="text-xs uppercase tracking-[0.35em] text-muted">
              go-worker
            </p>
            <h1 className="mt-3 font-display text-2xl font-semibold">
              Admin UI
            </h1>
            <p className="mt-2 text-sm text-muted">
              Secure operations for durable queues.
            </p>
            <Nav />
            <div className="mt-10 rounded-2xl border border-soft bg-gradient-to-br from-[#ffdccb] via-[#fff6ea] to-[#d7f2ec] p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-muted">
                coordination
              </p>
              {coordination ? (
                <>
                  <p className="mt-2 text-sm font-medium">
                    Leader lock {coordination.leaderLock} · rate limit{" "}
                    {coordination.globalRateLimit}
                  </p>
                  <p className="mt-1 text-xs text-muted">
                    Lease {coordination.lease} ·{" "}
                    {coordination.paused ? "dequeue paused" : "dequeue active"}
                  </p>
                </>
              ) : (
                <>
                  <p className="mt-2 text-sm font-medium">No data yet</p>
                  <p className="mt-1 text-xs text-muted">
                    Connect the admin gateway to populate status.
                  </p>
                </>
              )}
            </div>
          </div>
        </aside>

        <div className="flex-1">
          <header className="flex flex-col gap-4 rounded-3xl border border-soft bg-white/80 p-6 shadow-soft md:flex-row md:items-center md:justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.35em] text-muted">
                go-worker control
              </p>
              <h2 className="mt-2 font-display text-3xl font-semibold">
                Operations Console
              </h2>
              <p className="mt-2 max-w-2xl text-sm text-muted">
                Minimal surface area, maximal signal. Watch queues, scheduled
                jobs, and durable throughput.
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <span className="rounded-full border border-soft bg-white px-3 py-1 text-xs font-medium">
                Version v2.4.0
              </span>
              <SessionExpiry initialExpiresAt={sessionExpiry} />
              <span className="flex items-center gap-2 rounded-full bg-black px-3 py-1 text-xs font-semibold text-white">
                <span className={`h-2 w-2 rounded-full ${liveDot}`} />
                {liveLabel}
              </span>
              <SessionActions />
            </div>
          </header>

          <StatusBanner initialCoordination={coordination ?? null} />

          {audit ? (
            <div className="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900">
              <span className="font-semibold">{audit.message}</span>
              <span className="ml-2 text-xs text-emerald-700">
                {new Date(audit.timestamp).toLocaleTimeString()}
              </span>
            </div>
          ) : null}

          <main className="mt-6 space-y-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
