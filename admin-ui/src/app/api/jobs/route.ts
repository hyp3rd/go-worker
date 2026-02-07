import { NextResponse, type NextRequest } from "next/server";
import type { AdminJob } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const payload = await gatewayRequest<{ jobs: AdminJob[] }>({
      method: "GET",
      path: "/admin/v1/jobs",
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
    const body = (await request.json()) as Partial<AdminJob>;

    const payload = await gatewayRequest<{ job: AdminJob }>({
      method: "POST",
      path: "/admin/v1/jobs",
      body: {
        name: body?.name ?? "",
        description: body?.description ?? "",
        repo: body?.repo ?? "",
        tag: body?.tag ?? "",
        path: body?.path ?? "",
        dockerfile: body?.dockerfile ?? "",
        command: body?.command ?? [],
        env: body?.env ?? [],
        queue: body?.queue ?? "",
        retries: body?.retries ?? 0,
        timeoutSeconds: body?.timeoutSeconds ?? 0,
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
