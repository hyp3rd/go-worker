import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import type { QueueDetail } from "@/lib/types";
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
    const queueName = decodeURIComponent(name ?? "");
    const body = (await request.json()) as { weight?: number };

    const payload = await gatewayRequest<{ queue: QueueDetail }>({
      method: "POST",
      path: `/admin/v1/queues/${encodeURIComponent(queueName)}/weight`,
      body: { weight: Number(body?.weight ?? 0) },
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}

export async function DELETE(_request: NextRequest, { params }: RouteParams) {
  try {
    const { name } = await params;
    const queueName = decodeURIComponent(name ?? "");

    const payload = await gatewayRequest<{ queue: QueueDetail }>({
      method: "DELETE",
      path: `/admin/v1/queues/${encodeURIComponent(queueName)}/weight`,
    });

    return NextResponse.json(payload);
  } catch (error) {
    return apiErrorResponse(error, "admin_gateway_unavailable", 502);
  }
}
