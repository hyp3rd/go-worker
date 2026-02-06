import { NextResponse, type NextRequest } from "next/server";
import type { JobSchedule } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteParams = {
  params: Promise<{
    name: string;
  }>;
};

export async function POST(request: NextRequest, { params }: RouteParams) {
  try {
    const resolved = await params;
    const name = resolved?.name ?? "";
    const body = (await request.json()) as { paused?: boolean };
    const payload = await gatewayRequest<{ schedule: JobSchedule }>({
      method: "POST",
      path: `/admin/v1/schedules/${encodeURIComponent(name)}/pause`,
      body: { paused: body?.paused ?? true },
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
