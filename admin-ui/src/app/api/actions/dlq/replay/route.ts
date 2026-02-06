import { NextResponse, type NextRequest } from "next/server";
import { requireSession } from "@/lib/api-auth";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  const auth = await requireSession(request);
  if (auth) {
    return auth;
  }

  try {
    const payload = (await request.json().catch(() => ({}))) as {
      limit?: number;
    };

    const limit = Number.isFinite(payload.limit)
      ? Math.max(1, Math.min(1000, payload.limit as number))
      : 100;

    const response = await gatewayRequest<{ moved: number }>({
      method: "POST",
      path: "/admin/v1/dlq/replay",
      body: { limit },
    });

    return NextResponse.json({
      moved: response.moved,
      message: `Replayed ${response.moved} DLQ item(s)`,
    });
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
