import type {
  AdminActionCounters,
  CoordinationStatus,
  DlqEntry,
  HealthInfo,
  JobSchedule,
  OverviewStats,
  QueueSummary,
} from "@/lib/types";

export const overviewStats: OverviewStats = {
  activeWorkers: 24,
  queuedTasks: 224,
  queues: 3,
  avgLatencyMs: 218,
  p95LatencyMs: 410,
};

export const adminActionCounters: AdminActionCounters = {
  pause: 2,
  resume: 2,
  replay: 5,
};

export const coordinationStatus: CoordinationStatus = {
  globalRateLimit: "240 tasks/min",
  leaderLock: "active",
  lease: "30s",
  paused: false,
};

export const queueSummaries: QueueSummary[] = [
  {
    name: "default",
    ready: 142,
    processing: 6,
    dead: 2,
    weight: 1,
  },
  {
    name: "critical",
    ready: 18,
    processing: 3,
    dead: 0,
    weight: 3,
  },
  {
    name: "emails",
    ready: 64,
    processing: 4,
    dead: 1,
    weight: 2,
  },
];

export const jobSchedules: JobSchedule[] = [
  {
    name: "hourly-report",
    schedule: "0 * * * *",
    nextRun: "in 12m",
    lastRun: "48m ago",
    nextRunMs: Date.now() + 12 * 60 * 1000,
    lastRunMs: Date.now() - 48 * 60 * 1000,
    status: "healthy",
    paused: false,
    durable: false,
  },
  {
    name: "daily-email",
    schedule: "0 0 * * *",
    nextRun: "in 6h",
    lastRun: "18h ago",
    nextRunMs: Date.now() + 6 * 60 * 60 * 1000,
    lastRunMs: Date.now() - 18 * 60 * 60 * 1000,
    status: "healthy",
    paused: false,
    durable: true,
  },
  {
    name: "ledger-sync",
    schedule: "*/5 * * * *",
    nextRun: "in 3m",
    lastRun: "7m ago",
    nextRunMs: Date.now() + 3 * 60 * 1000,
    lastRunMs: Date.now() - 7 * 60 * 1000,
    status: "lagging",
    paused: false,
    durable: true,
  },
];

export const dlqEntries: DlqEntry[] = [
  {
    id: "9c49a79e-f945-4372-b43e-363eadb95efa",
    queue: "emails",
    handler: "send_email",
    age: "3m",
    attempts: 4,
  },
  {
    id: "1f92b6e8-28a3-467f-946c-170b9f6bb553",
    queue: "default",
    handler: "ledger_sync",
    age: "12m",
    attempts: 5,
  },
];

export const healthInfo: HealthInfo = {
  status: "ok",
  version: "dev",
  commit: "",
  buildTime: "",
  goVersion: "go",
};
