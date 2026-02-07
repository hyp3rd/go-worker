import { NextResponse, type NextRequest } from "next/server";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as { paused?: boolean };
    const payload = await gatewayRequest<{ updated: number; paused: boolean }>({
      method: "POST",
      path: "/admin/v1/schedules/pause",
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
