import { NextResponse } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type ScheduleFactory = {
  name: string;
  durable: boolean;
};

export async function GET() {
  try {
    const payload = await gatewayRequest<{ factories: ScheduleFactory[] }>({
      method: "GET",
      path: "/admin/v1/schedules/factories",
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
