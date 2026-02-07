import { NextResponse, type NextRequest } from "next/server";
import type { JobSchedule } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const payload = await gatewayRequest<{ schedules: JobSchedule[] }>({
      method: "GET",
      path: "/admin/v1/schedules",
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as {
      name?: string;
      spec?: string;
      durable?: boolean;
    };

    const payload = await gatewayRequest<{ schedule: JobSchedule }>({
      method: "POST",
      path: "/admin/v1/schedules",
      body: {
        name: body?.name ?? "",
        spec: body?.spec ?? "",
        durable: body?.durable ?? false,
      },
    });

    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
