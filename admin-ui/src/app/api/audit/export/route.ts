import { Readable } from "stream";
import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRawRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const defaultLimit = 500;
const maxLimit = 5000;

const parseLimit = (raw: string | null) => {
  if (!raw) {
    return defaultLimit;
  }

  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return defaultLimit;
  }

  return Math.min(Math.floor(value), maxLimit);
};

const parseFormat = (raw: string | null) => {
  const normalized = (raw ?? "").trim().toLowerCase();
  if (normalized === "json" || normalized === "csv") {
    return normalized;
  }

  return "jsonl";
};

export async function GET(request: NextRequest) {
  try {
    const url = new URL(request.url);
    const action = (url.searchParams.get("action") ?? "").trim();
    const target = (url.searchParams.get("target") ?? "").trim();
    const limit = parseLimit(url.searchParams.get("limit"));
    const format = parseFormat(url.searchParams.get("format"));

    const query = new URLSearchParams();
    query.set("format", format);
    query.set("limit", String(limit));
    if (action) {
      query.set("action", action);
    }
    if (target) {
      query.set("target", target);
    }

    const { stream, headers } = await gatewayRawRequest({
      method: "GET",
      path: `/admin/v1/audit/export?${query.toString()}`,
    });

    const webStream = Readable.toWeb(
      stream as unknown as Readable
    ) as ReadableStream<Uint8Array>;

    const contentType = getHeaderValue(headers["content-type"]) ?? "application/x-ndjson";
    const disposition =
      getHeaderValue(headers["content-disposition"]) ??
      `attachment; filename="admin-audit.${format}"`;

    return new NextResponse(webStream, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Content-Disposition": disposition,
        "Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return apiErrorResponse(error, "audit_export_failed", 502);
  }
}

const getHeaderValue = (value: string | string[] | undefined) => {
  if (Array.isArray(value)) {
    return value[0];
  }

  return value;
};
