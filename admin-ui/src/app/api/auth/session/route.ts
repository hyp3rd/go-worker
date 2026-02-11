import { NextResponse, type NextRequest } from "next/server";
import { apiCodeErrorResponse } from "@/lib/api-errors";
import { requireSession } from "@/lib/api-auth";
import {
  getAdminPassword,
  cookieOptions,
  getSessionExpiry,
  refreshSessionTokenIfNeeded,
  sessionCookieName,
  verifySessionToken,
} from "@/lib/auth";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const auth = await requireSession(request);
  if (auth) {
    return auth;
  }

  const password = getAdminPassword();
  if (!password) {
    return apiCodeErrorResponse(
      "failedprecondition",
      "admin_password_missing",
      500
    );
  }

  const token = request.cookies.get(sessionCookieName)?.value ?? "";
  if (!token || !(await verifySessionToken(token, password))) {
    return apiCodeErrorResponse("unauthenticated", "unauthorized", 401);
  }

  const refreshed = await refreshSessionTokenIfNeeded(token, password);
  const activeToken = refreshed || token;
  const expiresAt = getSessionExpiry(activeToken);

  const response = NextResponse.json({
    expiresAt,
    now: Date.now(),
  });

  if (refreshed) {
    response.cookies.set(sessionCookieName, refreshed, cookieOptions);
  }

  return response;
}
