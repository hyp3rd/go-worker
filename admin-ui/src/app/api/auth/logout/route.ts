import { NextResponse } from "next/server";
import {
  auditCookieName,
  auditCookieOptions,
  createAuditCookieValue,
  cookieOptions,
  sessionCookieName,
} from "@/lib/auth";

export async function POST(request: Request) {
  const response = NextResponse.redirect(new URL("/login", request.url));
  response.cookies.set(sessionCookieName, "", {
    ...cookieOptions,
    maxAge: 0,
  });
  response.cookies.set(
    auditCookieName,
    createAuditCookieValue("logout"),
    auditCookieOptions,
  );
  return response;
}
