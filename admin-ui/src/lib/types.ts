export type QueueSummary = {
  name: string;
  ready: number;
  processing: number;
  dead: number;
  weight: number;
};

export type QueueDetail = QueueSummary;

export type JobSchedule = {
  name: string;
  schedule: string;
  nextRun: string;
  status: "healthy" | "lagging" | "paused";
};

export type OverviewStats = {
  activeWorkers: number;
  queuedTasks: number;
  queues: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
};

export type DlqEntry = {
  id: string;
  queue: string;
  handler: string;
  age: string;
  attempts: number;
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
