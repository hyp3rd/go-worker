import { gatewayStream } from "@/lib/gateway";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const { stream } = await gatewayStream("/admin/v1/events");

    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        stream.on("data", (chunk) => {
          controller.enqueue(new Uint8Array(chunk));
        });
        stream.on("end", () => controller.close());
        stream.on("error", (err) => controller.error(err));
      },
      cancel() {
        const nodeStream = stream as unknown as {
          destroy?: () => void;
        };
        if (typeof nodeStream.destroy === "function") {
          nodeStream.destroy();
        }
      },
    });

    return new Response(body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "admin_gateway_unavailable" },
      { status: 502 }
    );
  }
}
