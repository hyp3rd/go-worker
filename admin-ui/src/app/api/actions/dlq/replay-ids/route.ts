import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { requireSession } from "@/lib/api-auth";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const maxReplayIDs = 1000;

export async function POST(request: NextRequest) {
  const auth = await requireSession(request);
  if (auth) {
    return auth;
  }

  try {
    const payload = (await request.json().catch(() => ({}))) as {
      ids?: string[];
    };

    const ids = Array.isArray(payload.ids) ? payload.ids : [];
    if (ids.length === 0) {
      return NextResponse.json(
        { error: "At least one DLQ id is required" },
        { status: 400 }
      );
    }

    const trimmed = Array.from(
      new Set(ids.map((id) => id.trim()).filter((id) => id.length > 0))
    );

    if (trimmed.length === 0) {
      return NextResponse.json(
        { error: "At least one DLQ id is required" },
        { status: 400 }
      );
    }

    if (trimmed.length > maxReplayIDs) {
      return NextResponse.json(
        { error: `Too many IDs (max ${maxReplayIDs})` },
        { status: 400 }
      );
    }

    const response = await gatewayRequest<{ moved: number }>({
      method: "POST",
      path: "/admin/v1/dlq/replay/ids",
      body: { ids: trimmed },
    });

    return NextResponse.json({
      moved: response.moved,
      message: `Replayed ${response.moved} DLQ item(s)`,
    });
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
