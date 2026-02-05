import { NextRequest, NextResponse } from "next/server";
import {
  auditCookieName,
  auditCookieOptions,
  createAuditCookieValue,
  cookieOptions,
  createSessionToken,
  getAdminPassword,
  sessionCookieName,
} from "@/lib/auth";

export async function POST(request: NextRequest) {
  const password = getAdminPassword();
  if (!password) {
    const url = new URL("/login", request.url);
    url.searchParams.set("error", "not_configured");
    return NextResponse.redirect(url);
  }

  const form = await request.formData();
  const provided = form.get("password");
  if (typeof provided !== "string" || provided !== password) {
    const url = new URL("/login", request.url);
    url.searchParams.set("error", "invalid");
    return NextResponse.redirect(url);
  }

  const token = await createSessionToken(password);
  const response = NextResponse.redirect(new URL("/", request.url));
  response.cookies.set(sessionCookieName, token, cookieOptions);
  response.cookies.set(
    auditCookieName,
    createAuditCookieValue("login"),
    auditCookieOptions,
  );
  return response;
}
