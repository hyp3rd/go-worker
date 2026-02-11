import { SectionHeader } from "@/components/section-header";
import { StatCard } from "@/components/stat-card";
import { StatusPill } from "@/components/status-pill";
import { RunbookActions } from "@/components/runbook-actions";
import { RefreshControls } from "@/components/refresh-controls";
import { JobOverview } from "@/components/job-overview";
import { OperationsTimeline } from "@/components/operations-timeline";
import Link from "next/link";
import {
  getAdminActionCounters,
  getAuditEvents,
  getCoordinationStatus,
  getJobEvents,
  getScheduleEvents,
  getJobSchedules,
  getOverviewStats,
  getQueueSummaries,
} from "@/lib/data";
import { formatLatency, formatNumber } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function Home() {
  const [stats, queues, jobs, coordination, actions, jobEvents, scheduleEvents, auditEvents] = await Promise.all([
    getOverviewStats(),
    getQueueSummaries(),
    getJobSchedules(),
    getCoordinationStatus(),
    getAdminActionCounters(),
    getJobEvents({ limit: 200 }),
    getScheduleEvents({ limit: 200 }),
    getAuditEvents({ limit: 50 }),
  ]);

  return (
    <>
      <section className="grid gap-6 md:grid-cols-3">
        <StatCard
          label="Active workers"
          value={formatNumber(stats.activeWorkers)}
          note="auto‑scaled"
        />
        <StatCard
          label="Queued tasks"
          value={formatNumber(stats.queuedTasks)}
          note={`${stats.queues} queues`}
        />
        <StatCard
          label="Avg latency"
          value={formatLatency(stats.avgLatencyMs)}
          note={`p95 ${formatLatency(stats.p95LatencyMs)}`}
        />
      </section>

      <section className="grid gap-6 lg:grid-cols-[2fr_1fr]">
        <div className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
          <SectionHeader
            title="Queue health"
            description="Weighted queues with deterministic scheduling."
            action={
              <div className="flex flex-wrap items-center gap-3">
                <RefreshControls />
                <Link
                  href="/queues?create=1"
                  className="rounded-full border border-soft bg-black px-4 py-2 text-xs font-semibold text-white"
                >
                  Add queue
                </Link>
              </div>
            }
          />
          <div className="mt-6 space-y-4">
            {queues.map((queue) => (
              <div
                key={queue.name}
                className="rounded-2xl border border-soft bg-[var(--card)] p-4"
              >
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-display text-lg font-semibold">
                      {queue.name}
                    </p>
                    <p className="text-xs text-muted">weight {queue.weight}</p>
                  </div>
                  <div className="flex items-center gap-3 text-xs">
                    <span className="rounded-full bg-amber-100 px-2 py-1 text-amber-700">
                      ready {formatNumber(queue.ready)}
                    </span>
                    <span className="rounded-full bg-emerald-100 px-2 py-1 text-emerald-700">
                      processing {formatNumber(queue.processing)}
                    </span>
                    <span className="rounded-full bg-rose-100 px-2 py-1 text-rose-700">
                      dead {formatNumber(queue.dead)}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-3xl border border-soft bg-white/90 p-6 shadow-soft">
          <SectionHeader
            title="Runbook"
            description="High‑confidence actions for production."
          />
          <RunbookActions paused={coordination.paused} initialAuditEvents={auditEvents} />
          <div className="mt-6 rounded-2xl border border-soft bg-[var(--card)] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">
              Action counters
            </p>
            <div className="mt-3 grid grid-cols-3 gap-3 text-xs">
              <div className="rounded-xl bg-white px-3 py-2 text-center shadow-soft">
                <p className="text-muted">pause</p>
                <p className="mt-1 text-sm font-semibold">{actions.pause}</p>
              </div>
              <div className="rounded-xl bg-white px-3 py-2 text-center shadow-soft">
                <p className="text-muted">resume</p>
                <p className="mt-1 text-sm font-semibold">{actions.resume}</p>
              </div>
              <div className="rounded-xl bg-white px-3 py-2 text-center shadow-soft">
                <p className="text-muted">replay</p>
                <p className="mt-1 text-sm font-semibold">{actions.replay}</p>
              </div>
            </div>
          </div>
          <div className="mt-6 rounded-2xl border border-soft bg-gradient-to-br from-[#ffdccb] via-[#fff6ea] to-[#d7f2ec] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">
              coordination
            </p>
            <p className="mt-2 text-sm font-medium">
              Global rate limit {coordination.globalRateLimit} · leader lock{" "}
              {coordination.leaderLock}
            </p>
            <p className="mt-1 text-xs text-muted">
              Lease {coordination.lease} ·{" "}
              {coordination.paused ? "dequeue paused" : "dequeue active"}
            </p>
          </div>
        </div>
      </section>

      <JobOverview events={jobEvents} />

      <OperationsTimeline jobs={jobEvents} schedules={scheduleEvents} audits={auditEvents} />

      <section className="rounded-3xl border border-soft bg-white/95 p-6 shadow-soft">
          <SectionHeader
            title="Scheduled jobs"
            description="Cron schedules across in‑memory and durable workers."
            action={
              <div className="flex gap-2">
                <Link
                  href="/schedules#schedule-events"
                  className="rounded-full border border-soft px-4 py-2 text-xs font-semibold"
                >
                  Event log
                </Link>
                <Link
                  href="/schedules"
                  className="rounded-full bg-[var(--accent)] px-4 py-2 text-xs font-semibold text-[var(--accent-ink)]"
                >
                Manage schedules
              </Link>
            </div>
          }
        />
        <div className="mt-6 grid gap-4 md:grid-cols-3">
          {jobs.map((job) => (
            <div
              key={job.name}
              className="rounded-2xl border border-soft bg-[var(--card)] p-4"
            >
              <div className="flex items-center justify-between">
                <p className="font-display text-base font-semibold">
                  {job.name}
                </p>
                <StatusPill status={job.status} />
              </div>
              <p className="mt-2 text-xs text-muted">{job.schedule}</p>
              <p className="mt-4 text-sm font-medium">
                Next run <span className="text-muted">{job.nextRun}</span>
              </p>
              <p className="mt-1 text-xs text-muted">
                Last run {job.lastRun}
              </p>
            </div>
          ))}
        </div>
      </section>
    </>
  );
}
