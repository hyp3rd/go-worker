import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { requireSession } from "@/lib/api-auth";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const auth = await requireSession(request);
  if (auth) {
    return auth;
  }

  try {
    const payload = await gatewayRequest<{ paused: boolean }>({
      method: "POST",
      path: "/admin/v1/pause",
    });
    return NextResponse.json({
      paused: payload.paused,
      message: payload.paused ? "Durable dequeue paused" : "Pause requested",
    });
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
