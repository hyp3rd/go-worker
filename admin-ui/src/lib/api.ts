import { headers } from "next/headers";
import type {
  AdminActionCounters,
  AdminJob,
  CoordinationStatus,
  DlqEntry,
  HealthInfo,
  JobEvent,
  JobSchedule,
  OverviewStats,
  QueueDetail,
  QueueSummary,
  ScheduleFactory,
  ScheduleEvent,
} from "@/lib/types";

const getOrigin = async () => {
  const explicit =
    process.env.NEXT_PUBLIC_WORKER_ADMIN_ORIGIN ??
    process.env.NEXT_PUBLIC_ADMIN_UI_ORIGIN;
  if (explicit) {
    return explicit.replace(/\/$/, "");
  }

  const headerList = await headers();
  const host = headerList.get("host");
  if (!host) {
    return "http://localhost:3000";
  }

  const proto = headerList.get("x-forwarded-proto") ?? "http";
  return `${proto}://${host}`;
};

async function fetchJson<T>(path: string): Promise<T> {
  const origin = await getOrigin();
  const res = await fetch(`${origin}${path}`, {
    cache: "no-store",
  });

  if (!res.ok) {
    let detail = "";
    try {
      const body = (await res.json()) as { error?: string };
      if (body?.error) {
        detail = body.error;
      }
    } catch {
      // ignore response parse errors
    }

    const message = detail
      ? `Request failed: ${detail}`
      : `Request failed: ${res.status}`;

    throw new Error(message);
  }

  return res.json() as Promise<T>;
}

export async function fetchOverview(): Promise<{
  stats: OverviewStats;
  coordination: CoordinationStatus;
  actions: AdminActionCounters;
}> {
  return fetchJson("/api/overview");
}

export async function fetchHealth(): Promise<HealthInfo> {
  return fetchJson("/api/health");
}

export async function fetchQueues(): Promise<{ queues: QueueSummary[] }> {
  return fetchJson("/api/queues");
}

export async function fetchQueueDetail(
  name: string
): Promise<{ queue: QueueDetail }> {
  const encoded = encodeURIComponent(name);
  return fetchJson(`/api/queues/${encoded}`);
}

export async function fetchSchedules(): Promise<{ schedules: JobSchedule[] }> {
  return fetchJson("/api/schedules");
}

export async function fetchJobs(): Promise<{ jobs: AdminJob[] }> {
  return fetchJson("/api/jobs");
}

export async function fetchScheduleFactories(): Promise<{
  factories: ScheduleFactory[];
}> {
  return fetchJson("/api/schedules/factories");
}

export async function fetchScheduleEvents(params?: {
  name?: string;
  limit?: number;
}): Promise<{ events: ScheduleEvent[] }> {
  const search = new URLSearchParams();
  if (params?.name) {
    search.set("name", params.name);
  }
  if (params?.limit) {
    search.set("limit", String(params.limit));
  }
  const suffix = search.toString();
  const path = suffix ? `/api/schedules/events?${suffix}` : "/api/schedules/events";
  return fetchJson(path);
}

export async function fetchJobEvents(params?: {
  name?: string;
  limit?: number;
}): Promise<{ events: JobEvent[] }> {
  const search = new URLSearchParams();
  if (params?.name) {
    search.set("name", params.name);
  }
  if (params?.limit) {
    search.set("limit", String(params.limit));
  }
  const suffix = search.toString();
  const path = suffix ? `/api/jobs/events?${suffix}` : "/api/jobs/events";
  return fetchJson(path);
}

export async function fetchDlq(params?: {
  limit?: number;
  offset?: number;
  queue?: string;
  handler?: string;
  query?: string;
}): Promise<{ entries: DlqEntry[]; total: number }> {
  const search = new URLSearchParams();
  if (params?.limit) {
    search.set("limit", String(params.limit));
  }
  if (params?.offset) {
    search.set("offset", String(params.offset));
  }
  if (params?.queue) {
    search.set("queue", params.queue);
  }
  if (params?.handler) {
    search.set("handler", params.handler);
  }
  if (params?.query) {
    search.set("query", params.query);
  }
  const suffix = search.toString();
  const path = suffix ? `/api/dlq?${suffix}` : "/api/dlq";
  return fetchJson(path);
}
