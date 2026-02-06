import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import {
  getAdminPassword,
  sessionCookieName,
  verifySessionToken,
} from "@/lib/auth";

export const requireSession = async (request: NextRequest) => {
  const password = getAdminPassword();
  if (!password) {
    return NextResponse.json(
      { error: "admin_password_missing" },
      { status: 500 }
    );
  }

  const cookie = request.cookies.get(sessionCookieName)?.value;
  if (!cookie || !(await verifySessionToken(cookie, password))) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  return null;
};
