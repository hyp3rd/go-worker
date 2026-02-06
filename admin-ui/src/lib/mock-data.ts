import type {
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
    status: "healthy",
  },
  {
    name: "daily-email",
    schedule: "0 0 * * *",
    nextRun: "in 6h",
    lastRun: "18h ago",
    status: "healthy",
  },
  {
    name: "ledger-sync",
    schedule: "*/5 * * * *",
    nextRun: "in 3m",
    lastRun: "7m ago",
    status: "lagging",
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
