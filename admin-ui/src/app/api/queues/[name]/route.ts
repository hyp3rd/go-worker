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

export async function GET(_request: NextRequest, { params }: RouteParams) {
  try {
    const { name } = await params;
    const decoded = decodeURIComponent(name ?? "");
    const payload = await gatewayRequest<{
      queue: {
        name: string;
        ready: number;
        processing: number;
        dead: number;
        weight: number;
        paused: boolean;
      };
    }>({
      method: "GET",
      path: `/admin/v1/queues/${encodeURIComponent(decoded)}`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
