import { NextResponse, type NextRequest } from "next/server";
import type { AdminJob } from "@/lib/types";
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
    const payload = await gatewayRequest<{ job: AdminJob }>({
      method: "GET",
      path: `/admin/v1/jobs/${encodeURIComponent(decoded)}`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}

export async function DELETE(_request: NextRequest, { params }: RouteParams) {
  try {
    const { name } = await params;
    const decoded = decodeURIComponent(name ?? "");
    const payload = await gatewayRequest<{ deleted: boolean }>({
      method: "DELETE",
      path: `/admin/v1/jobs/${encodeURIComponent(decoded)}`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
