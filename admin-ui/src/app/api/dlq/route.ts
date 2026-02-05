import { NextResponse } from "next/server";
import type { DlqEntry } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const formatAge = (ageMs: number) => {
  if (!Number.isFinite(ageMs) || ageMs <= 0) {
    return "unknown";
  }

  if (ageMs < 60_000) {
    return `${Math.max(1, Math.floor(ageMs / 1000))}s`;
  }

  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h`;
  }

  const days = Math.floor(hours / 24);
  return `${days}d`;
};

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const limitParam = url.searchParams.get("limit");
    const limit = limitParam ? Number(limitParam) : undefined;

    const payload = await gatewayRequest<{
      entries: Array<{
        id: string;
        queue: string;
        handler: string;
        attempts: number;
        ageMs: number;
      }>;
    }>({
      method: "GET",
      path: `/admin/v1/dlq${limit ? `?limit=${limit}` : ""}`,
    });

    const entries: DlqEntry[] = payload.entries.map((entry) => ({
      id: entry.id,
      queue: entry.queue,
      handler: entry.handler,
      attempts: entry.attempts,
      age: formatAge(entry.ageMs),
    }));

    return NextResponse.json({ entries });
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
