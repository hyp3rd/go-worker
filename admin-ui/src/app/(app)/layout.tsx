import { cookies } from "next/headers";
import { AppShell } from "@/components/app-shell";
import {
  auditCookieName,
  getSessionExpiry,
  parseAuditCookieValue,
  sessionCookieName,
} from "@/lib/auth";
import { getCoordinationStatus } from "@/lib/data";

export const dynamic = "force-dynamic";

export default async function AppLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const store = await cookies();
  const audit = parseAuditCookieValue(store.get(auditCookieName)?.value);
  const auditNotice =
    audit?.event === "login"
      ? {
          message: "Signed in successfully.",
          timestamp: audit.timestamp,
        }
      : null;

  const sessionToken = store.get(sessionCookieName)?.value ?? "";
  const expiry = sessionToken ? getSessionExpiry(sessionToken) : null;
  let coordination = null;
  try {
    coordination = await getCoordinationStatus();
  } catch {
    coordination = null;
  }

  return (
    <AppShell
      audit={auditNotice}
      sessionExpiry={expiry}
      coordination={coordination}
    >
      {children}
    </AppShell>
  );
}
