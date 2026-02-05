import { headers } from "next/headers";
import type {
  CoordinationStatus,
  DlqEntry,
  JobSchedule,
  OverviewStats,
  QueueSummary,
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
}> {
  return fetchJson("/api/overview");
}

export async function fetchQueues(): Promise<{ queues: QueueSummary[] }> {
  return fetchJson("/api/queues");
}

export async function fetchSchedules(): Promise<{ schedules: JobSchedule[] }> {
  return fetchJson("/api/schedules");
}

export async function fetchDlq(): Promise<{ entries: DlqEntry[] }> {
  return fetchJson("/api/dlq");
}
