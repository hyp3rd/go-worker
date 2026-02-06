import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import {
  cookieOptions,
  getAdminPassword,
  refreshSessionTokenIfNeeded,
  sessionCookieName,
  verifySessionToken,
} from "@/lib/auth";

const isPublicPath = (pathname: string) => {
  if (pathname === "/login") {
    return true;
  }
  if (pathname.startsWith("/api/")) {
    return true;
  }
  if (pathname.startsWith("/api/auth")) {
    return true;
  }
  return false;
};

export async function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (isPublicPath(pathname)) {
    return NextResponse.next();
  }

  const password = getAdminPassword();
  if (!password) {
    const url = request.nextUrl.clone();
    url.pathname = "/login";
    url.searchParams.set("error", "not_configured");
    return NextResponse.redirect(url);
  }

  const cookie = request.cookies.get(sessionCookieName)?.value;
  if (!cookie || !(await verifySessionToken(cookie, password))) {
    const url = request.nextUrl.clone();
    url.pathname = "/login";
    return NextResponse.redirect(url);
  }

  const refreshed = await refreshSessionTokenIfNeeded(cookie, password);
  if (!refreshed) {
    return NextResponse.next();
  }

  const response = NextResponse.next();
  response.cookies.set(sessionCookieName, refreshed, cookieOptions);
  return response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico|api).*)"],
};
