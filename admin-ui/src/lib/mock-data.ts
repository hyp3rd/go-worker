import type {
  AdminActionCounters,
  CoordinationStatus,
  DlqEntry,
  HealthInfo,
  AdminJob,
  JobSchedule,
  OverviewStats,
  QueueSummary,
  ScheduleFactory,
  ScheduleEvent,
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
    paused: false,
  },
  {
    name: "critical",
    ready: 18,
    processing: 3,
    dead: 0,
    weight: 3,
    paused: false,
  },
  {
    name: "emails",
    ready: 64,
    processing: 4,
    dead: 1,
    weight: 2,
    paused: false,
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

export const scheduleFactories: ScheduleFactory[] = [
  { name: "hourly-report", durable: false },
  { name: "daily-email", durable: true },
  { name: "ledger-sync", durable: true },
];

export const scheduleEvents: ScheduleEvent[] = [
  {
    taskId: "b5a0c2b3-2ed5-47f8-8d1e-2e1e2c29f2b0",
    name: "ledger-sync",
    spec: "*/5 * * * *",
    durable: true,
    status: "completed",
    queue: "default",
    startedAtMs: Date.now() - 75 * 1000,
    finishedAtMs: Date.now() - 48 * 1000,
    durationMs: 27 * 1000,
    result: "synced 128 accounts",
    metadata: {
      handler: "ledger_sync",
      command: "sync --batch",
    },
  },
  {
    taskId: "c9cbb1a8-3c11-4a17-8a83-3a5a720c2b8a",
    name: "daily-email",
    spec: "0 0 * * *",
    durable: true,
    status: "failed",
    queue: "emails",
    startedAtMs: Date.now() - 12 * 60 * 1000,
    finishedAtMs: Date.now() - 11 * 60 * 1000,
    durationMs: 45 * 1000,
    error: "smtp timeout",
    metadata: {
      handler: "send_email",
      command: "send --daily",
    },
  },
  {
    taskId: "f15e59d7-1b76-4ed6-9a0c-f4572bfe55b0",
    name: "hourly-report",
    spec: "0 * * * *",
    durable: false,
    status: "completed",
    queue: "default",
    startedAtMs: Date.now() - 4 * 60 * 1000,
    finishedAtMs: Date.now() - 3 * 60 * 1000,
    durationMs: 18 * 1000,
    result: "report stored",
    metadata: {
      task_name: "report_generator",
      description: "Generate the hourly SLA report.",
    },
  },
];

export const adminJobs: AdminJob[] = [
  {
    name: "metrics_rollup",
    description: "Roll up durable metrics snapshots",
    repo: "git@github.com:hyp3rd/worker-jobs.git",
    tag: "v1.2.0",
    path: "jobs/metrics",
    dockerfile: "Dockerfile",
    command: ["./run.sh"],
    env: ["WORKER_ADMIN_API_URL"],
    queue: "default",
    retries: 2,
    timeoutSeconds: 600,
    createdAtMs: Date.now() - 24 * 60 * 60 * 1000,
    updatedAtMs: Date.now() - 60 * 60 * 1000,
  },
  {
    name: "dlq_sweep",
    description: "Sweep DLQ and emit alerts",
    repo: "git@github.com:hyp3rd/worker-jobs.git",
    tag: "v1.2.0",
    path: "jobs/dlq",
    dockerfile: "Dockerfile",
    command: ["./run.sh", "--notify"],
    env: ["WORKER_ADMIN_API_URL"],
    queue: "default",
    retries: 1,
    timeoutSeconds: 900,
    createdAtMs: Date.now() - 7 * 24 * 60 * 60 * 1000,
    updatedAtMs: Date.now() - 12 * 60 * 60 * 1000,
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
