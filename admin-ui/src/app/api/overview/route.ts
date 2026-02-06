import { NextResponse } from "next/server";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const payload = await gatewayRequest<{
      stats: {
        activeWorkers: number;
        queuedTasks: number;
        queues: number;
        avgLatencyMs: number;
        p95LatencyMs: number;
      };
      coordination: {
        globalRateLimit: string;
        leaderLock: string;
        lease: string;
        paused: boolean;
      };
      actions: {
        pause: number;
        resume: number;
        replay: number;
      };
    }>({ method: "GET", path: "/admin/v1/overview" });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
