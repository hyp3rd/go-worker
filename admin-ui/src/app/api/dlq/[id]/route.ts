import { NextResponse, type NextRequest } from "next/server";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteParams = {
  params: Promise<{
    id: string;
  }>;
};

export async function GET(_request: NextRequest, { params }: RouteParams) {
  try {
    const { id } = await params;
    const decoded = decodeURIComponent(id ?? "");
    const payload = await gatewayRequest<{ entry: unknown }>({
      method: "GET",
      path: `/admin/v1/dlq/${encodeURIComponent(decoded)}`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
