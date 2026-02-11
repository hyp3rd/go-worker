import { NextResponse } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRequest } from "@/lib/gateway";
import type { AdminAuditEvent } from "@/lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const parseLimit = (raw: string | null) => {
  if (!raw) {
    return 100;
  }

  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return 100;
  }

  return Math.min(Math.floor(value), 1000);
};

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const action = (url.searchParams.get("action") ?? "").trim();
    const target = (url.searchParams.get("target") ?? "").trim();
    const limit = parseLimit(url.searchParams.get("limit"));

    const query = new URLSearchParams();
    if (action) {
      query.set("action", action);
    }
    if (target) {
      query.set("target", target);
    }
    query.set("limit", String(limit));

    const path = `/admin/v1/audit?${query.toString()}`;
    const payload = await gatewayRequest<{ events: AdminAuditEvent[] }>({
      method: "GET",
      path,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
