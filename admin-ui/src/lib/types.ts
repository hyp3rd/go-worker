export type QueueSummary = {
  name: string;
  ready: number;
  processing: number;
  dead: number;
  weight: number;
  paused: boolean;
};

export type QueueDetail = QueueSummary;

export type JobSchedule = {
  name: string;
  schedule: string;
  nextRun: string;
  lastRun: string;
  nextRunMs?: number;
  lastRunMs?: number;
  status: "healthy" | "lagging" | "paused";
  paused: boolean;
  durable: boolean;
};

export type ScheduleFactory = {
  name: string;
  durable: boolean;
};

export type ScheduleEvent = {
  taskId: string;
  name: string;
  spec: string;
  durable: boolean;
  status: string;
  queue: string;
  startedAtMs?: number;
  finishedAtMs?: number;
  durationMs?: number;
  result?: string;
  error?: string;
  metadata?: Record<string, string>;
};

export type OverviewStats = {
  activeWorkers: number;
  queuedTasks: number;
  queues: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
};

export type AdminActionCounters = {
  pause: number;
  resume: number;
  replay: number;
};

export type DlqEntry = {
  id: string;
  queue: string;
  handler: string;
  age: string;
  attempts: number;
};

export type DlqEntryDetail = {
  id: string;
  queue: string;
  handler: string;
  attempts: number;
  ageMs: number;
  failedAtMs: number;
  updatedAtMs: number;
  lastError: string;
  payloadSize: number;
  metadata?: Record<string, string>;
};

export type CoordinationStatus = {
  globalRateLimit: string;
  leaderLock: string;
  lease: string;
  paused: boolean;
};

export type HealthInfo = {
  status: string;
  version: string;
  commit: string;
  buildTime: string;
  goVersion: string;
};
