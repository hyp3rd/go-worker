import type { ErrorDiagnostic } from "@/lib/error-diagnostics";

type NoticeBannerProps = {
  tone: "success" | "error" | "info";
  text: string;
  diagnostic?: ErrorDiagnostic;
};

export function NoticeBanner({ tone, text, diagnostic }: NoticeBannerProps) {
  const toneClasses =
    tone === "success"
      ? "border-emerald-200 bg-emerald-50 text-emerald-800"
      : tone === "info"
        ? "border-sky-200 bg-sky-50 text-sky-800"
        : "border-rose-200 bg-rose-50 text-rose-800";

  return (
    <div className={`rounded-2xl border px-4 py-3 text-sm ${toneClasses}`}>
      <p>{text}</p>
      {tone === "error" && diagnostic ? (
        <div className="mt-2 text-xs">
          {diagnostic.code ? (
            <p className="uppercase tracking-[0.16em] opacity-90">
              code: {diagnostic.code}
            </p>
          ) : null}
          {diagnostic.requestId ? (
            <p className="font-mono opacity-90">request-id: {diagnostic.requestId}</p>
          ) : null}
          {diagnostic.hint ? <p className="mt-1 opacity-90">{diagnostic.hint}</p> : null}
        </div>
      ) : null}
    </div>
  );
}
