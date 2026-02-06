import { cookies } from "next/headers";
import { auditCookieName, parseAuditCookieValue } from "@/lib/auth";

type LoginPageProps = {
  searchParams?: Promise<{ error?: string }>;
};

const errorCopy: Record<string, string> = {
  invalid: "Invalid password. Try again.",
  not_configured: "WORKER_ADMIN_PASSWORD is not configured.",
};

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const resolved = searchParams ? await searchParams : undefined;
  const errorKey = resolved?.error ?? "";
  const errorMessage = errorCopy[errorKey] ?? "";
  const store = await cookies();
  const audit = parseAuditCookieValue(store.get(auditCookieName)?.value);
  const auditMessage =
    audit?.event === "logout"
      ? "Signed out. Your session has been cleared."
      : "";

  return (
    <div className="min-h-screen bg-grid">
      <div className="mx-auto flex min-h-screen max-w-xl items-center px-6">
        <div className="w-full rounded-3xl border border-soft bg-white/90 p-8 shadow-soft">
          <p className="text-xs uppercase tracking-[0.35em] text-muted">
            go-worker admin
          </p>
          <h1 className="mt-3 font-display text-3xl font-semibold">Sign in</h1>
          <p className="mt-2 text-sm text-muted">
            Enter the admin password to access operational dashboards.
          </p>

          <form className="mt-6 space-y-4" action="/api/auth/login" method="post">
            <div>
              <label className="text-xs uppercase tracking-[0.2em] text-muted">
                Password
              </label>
              <input
                name="password"
                type="password"
                className="mt-2 w-full rounded-2xl border border-soft bg-white px-4 py-3 text-sm"
                placeholder="••••••••"
                required
              />
            </div>
            {errorMessage ? (
              <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                {errorMessage}
              </div>
            ) : null}
            {auditMessage ? (
              <div className="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900">
                {auditMessage}
              </div>
            ) : null}
            <button
              type="submit"
              className="w-full rounded-2xl bg-black px-4 py-3 text-sm font-semibold text-white"
            >
              Continue
            </button>
          </form>

          <p className="mt-6 text-xs text-muted">
            Tip: set <span className="font-mono">WORKER_ADMIN_PASSWORD</span> in
            your environment.
          </p>
        </div>
      </div>
    </div>
  );
}
