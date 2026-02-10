import { gatewayStream } from "@/lib/gateway";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  try {
    const { stream } = await gatewayStream("/admin/v1/events", {
      lastEventId: request.headers.get("last-event-id") ?? "",
    });

    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        let closed = false;
        const closeOnce = () => {
          if (closed) {
            return;
          }
          closed = true;
          try {
            controller.close();
          } catch {
            // ignore double-close
          }
        };
        const isAbortError = (err: unknown) => {
          if (!err || typeof err !== "object") {
            return false;
          }
          const record = err as { code?: string; name?: string; message?: string };
          return (
            record.code === "ECONNRESET" ||
            record.name === "AbortError" ||
            (record.message?.includes("aborted") ?? false)
          );
        };

        stream.on("data", (chunk) => {
          if (closed) {
            return;
          }
          try {
            controller.enqueue(new Uint8Array(chunk));
          } catch {
            closeOnce();
          }
        });
        stream.on("end", closeOnce);
        stream.on("close", closeOnce);
        stream.on("error", (err) => {
          if (isAbortError(err)) {
            closeOnce();
            return;
          }
          if (closed) {
            return;
          }
          try {
            controller.error(err);
          } catch {
            closeOnce();
          }
        });
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
