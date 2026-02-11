import { NextResponse } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const payload = await gatewayRequest<{
      queues: Array<{
        name: string;
        ready: number;
        processing: number;
        dead: number;
        weight: number;
        paused: boolean;
      }>;
    }>({ method: "GET", path: "/admin/v1/queues" });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
