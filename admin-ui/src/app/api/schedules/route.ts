import { NextResponse } from "next/server";
import type { JobSchedule } from "@/lib/types";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const readSchedules = (): JobSchedule[] => {
  const raw = process.env.ADMIN_UI_SCHEDULES_JSON;
  if (!raw) {
    return [];
  }

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .filter((entry) => entry && typeof entry === "object")
      .map((entry) => ({
        name: String(entry.name ?? "unnamed"),
        schedule: String(entry.schedule ?? "-"),
        nextRun: String(entry.nextRun ?? "n/a"),
        status: (entry.status as JobSchedule["status"]) ?? "healthy",
      }));
  } catch {
    return [];
  }
};

export async function GET() {
  return NextResponse.json({ schedules: readSchedules() });
}
