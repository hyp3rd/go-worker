import { NextResponse, type NextRequest } from "next/server";
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
    const { name } = await params;
    const decoded = decodeURIComponent(name ?? "");
    const body = await request.json().catch(() => ({}));
    const paused = typeof body?.paused === "boolean" ? body.paused : true;

    const payload = await gatewayRequest<{ queue: unknown }>({
      method: "POST",
      path: `/admin/v1/queues/${encodeURIComponent(decoded)}/pause`,
      body: { paused },
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
