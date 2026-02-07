import { NextResponse, type NextRequest } from "next/server";
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
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
