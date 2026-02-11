import { NextResponse } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import type { ScheduleEvent } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = new URLSearchParams();
    const name = searchParams.get("name");
    const limit = searchParams.get("limit");
    if (name) {
      query.set("name", name);
    }
    if (limit) {
      query.set("limit", limit);
    }

    const path = query.toString()
      ? `/admin/v1/schedules/events?${query.toString()}`
      : "/admin/v1/schedules/events";

    const payload = await gatewayRequest<{ events: ScheduleEvent[] }>({
      method: "GET",
      path,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
