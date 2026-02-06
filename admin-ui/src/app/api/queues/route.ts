import { NextResponse } from "next/server";
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
      }>;
    }>({ method: "GET", path: "/admin/v1/queues" });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
