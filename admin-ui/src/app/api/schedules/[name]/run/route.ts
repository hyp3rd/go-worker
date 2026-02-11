import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteParams = {
  params: Promise<{
    name: string;
  }>;
};

export async function POST(_request: NextRequest, { params }: RouteParams) {
  try {
    const resolved = await params;
    const name = resolved?.name ?? "";
    const payload = await gatewayRequest<{ taskId: string }>({
      method: "POST",
      path: `/admin/v1/schedules/${encodeURIComponent(name)}/run`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
