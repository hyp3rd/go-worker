import { createReadStream, promises as fsp } from "fs";
import path from "path";
import { Readable } from "stream";
import { NextResponse, type NextRequest } from "next/server";
import type { AdminJob } from "@/lib/types";
import { gatewayRequest } from "@/lib/gateway";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteParams = {
  params: Promise<{
    name: string;
  }>;
};

const normalizeSource = (value?: string) => {
  const key = value?.trim().toLowerCase();
  if (!key) {
    return "git_tag";
  }
  if (key === "git_tag" || key === "tarball_url" || key === "tarball_path") {
    return key;
  }
  return "git_tag";
};

const resolveTarballFile = (baseDir: string, relativePath: string) => {
  const sanitized = relativePath.trim();
  if (!sanitized) {
    throw new Error("Tarball path is required");
  }
  if (path.isAbsolute(sanitized)) {
    throw new Error("Absolute tarball paths are not allowed");
  }

  const resolvedBase = path.resolve(baseDir);
  const resolvedFile = path.resolve(resolvedBase, sanitized);
  const allowedPrefix = `${resolvedBase}${path.sep}`;
  if (resolvedFile !== resolvedBase && !resolvedFile.startsWith(allowedPrefix)) {
    throw new Error("Tarball path escapes configured base directory");
  }

  return resolvedFile;
};

export async function GET(_request: NextRequest, { params }: RouteParams) {
  try {
    const { name } = await params;
    const decoded = decodeURIComponent(name ?? "");
    const payload = await gatewayRequest<{ job: AdminJob }>({
      method: "GET",
      path: `/admin/v1/jobs/${encodeURIComponent(decoded)}`,
    });
    const job = payload.job;
    if (!job) {
      return NextResponse.json({ error: "Job not found" }, { status: 404 });
    }

    const source = normalizeSource(job.source);
    if (source === "tarball_url") {
      const target = job.tarballUrl?.trim();
      if (!target) {
        return NextResponse.json(
          { error: "Tarball URL missing for this job" },
          { status: 400 }
        );
      }
      return NextResponse.redirect(target, 307);
    }

    if (source !== "tarball_path") {
      return NextResponse.json(
        { error: "This job source has no downloadable tarball" },
        { status: 400 }
      );
    }

    const baseDir =
      process.env.WORKER_ADMIN_JOB_TARBALL_DIR ??
      process.env.WORKER_JOB_TARBALL_DIR ??
      "";
    if (!baseDir) {
      return NextResponse.json(
        {
          error:
            "Tarball downloads are disabled. Set WORKER_ADMIN_JOB_TARBALL_DIR.",
        },
        { status: 501 }
      );
    }

    const relativePath = job.tarballPath?.trim() ?? "";
    const tarballPath = resolveTarballFile(baseDir, relativePath);
    const stat = await fsp.stat(tarballPath);
    if (!stat.isFile()) {
      return NextResponse.json({ error: "Tarball not found" }, { status: 404 });
    }

    const stream = createReadStream(tarballPath);
    const webStream = Readable.toWeb(stream) as ReadableStream<Uint8Array>;
    const filename = path.basename(tarballPath);
    const response = new NextResponse(webStream, {
      status: 200,
      headers: {
        "Content-Type": "application/gzip",
        "Content-Length": stat.size.toString(),
        "Content-Disposition": `attachment; filename="${filename}"`,
        "Cache-Control": "no-store",
      },
    });
    const sha256 = job.tarballSha256?.trim();
    if (sha256) {
      response.headers.set("X-Tarball-SHA256", sha256);
    }

    return response;
  } catch (error) {
    return NextResponse.json(
      { error: (error as Error).message ?? "artifact_download_failed" },
      { status: 502 }
    );
  }
}
