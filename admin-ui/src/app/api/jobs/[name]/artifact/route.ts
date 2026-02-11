import { Readable } from "stream";
import { NextResponse, type NextRequest } from "next/server";
import { apiErrorResponse } from "@/lib/api-errors";
import { gatewayRawRequest, gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteParams = {
  params: Promise<{
    name: string;
  }>;
};

type ArtifactMeta = {
  artifact: {
    downloadable: boolean;
    redirectUrl?: string;
    filename?: string;
    sha256?: string;
    sizeBytes?: number;
  };
};

export async function GET(_request: NextRequest, { params }: RouteParams) {
  try {
    const { name } = await params;
    const decoded = decodeURIComponent(name ?? "");
    const encoded = encodeURIComponent(decoded);
    const metaPayload = await gatewayRequest<ArtifactMeta>({
      method: "GET",
      path: `/admin/v1/jobs/${encoded}/artifact/meta`,
    });

    const artifact = metaPayload.artifact;
    if (!artifact?.downloadable) {
      return NextResponse.json(
        { error: "This job source has no downloadable tarball" },
        { status: 400 }
      );
    }

    const redirectURL = artifact.redirectUrl?.trim();
    if (redirectURL) {
      return NextResponse.redirect(redirectURL, 307);
    }

    const { stream, headers } = await gatewayRawRequest({
      method: "GET",
      path: `/admin/v1/jobs/${encoded}/artifact`,
    });
    const webStream = Readable.toWeb(
      stream as unknown as Readable
    ) as ReadableStream<Uint8Array>;

    const contentType = getHeaderValue(headers["content-type"]) ?? "application/gzip";
    const contentLength = getHeaderValue(headers["content-length"]);
    const disposition =
      getHeaderValue(headers["content-disposition"]) ??
      `attachment; filename="${artifact.filename ?? `${decoded}.tar.gz`}"`;

    const response = new NextResponse(webStream, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Content-Disposition": disposition,
        "Cache-Control": "no-store",
      },
    });
    if (contentLength) {
      response.headers.set("Content-Length", contentLength);
    }
    const sha256 =
      getHeaderValue(headers["x-tarball-sha256"]) ??
      artifact.sha256?.trim();
    if (sha256) {
      response.headers.set("X-Tarball-SHA256", sha256);
    }

    return response;
  } catch (error) {
    return apiErrorResponse(error, "artifact_download_failed", 502);
  }
}

const getHeaderValue = (value: string | string[] | undefined) => {
  if (Array.isArray(value)) {
    return value[0];
  }

  return value;
};
