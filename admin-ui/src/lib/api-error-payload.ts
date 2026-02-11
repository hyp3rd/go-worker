import type { ErrorDiagnostic } from "@/lib/error-diagnostics";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";
import { getErrorHint, toCanonicalErrorCode } from "@/lib/error-codes";

export type APIErrorDetails = ErrorDiagnostic & {
  code: string;
};

const normalizeDetails = (
  details: Partial<APIErrorDetails>,
  fallbackMessage: string,
  fallbackCode: string
): APIErrorDetails => {
  const message = (details.message ?? "").trim() || fallbackMessage;
  const code =
    toCanonicalErrorCode(details.code) ??
    toCanonicalErrorCode(fallbackCode) ??
    fallbackCode;

  return {
    message,
    code,
    requestId: details.requestId,
    hint: details.hint ?? getErrorHint(code),
  };
};

export const parseAPIErrorPayload = (
  payload: unknown,
  fallbackMessage: string,
  fallbackCode = "gateway_error"
): APIErrorDetails => {
  if (!payload || typeof payload !== "object") {
    return normalizeDetails({}, fallbackMessage, fallbackCode);
  }

  const body = payload as {
    error?: unknown;
    errorDetail?: unknown;
  };

  if (body.errorDetail && typeof body.errorDetail === "object") {
    return normalizeDetails(
      body.errorDetail as Partial<APIErrorDetails>,
      fallbackMessage,
      fallbackCode
    );
  }

  if (body.error && typeof body.error === "object") {
    return normalizeDetails(
      body.error as Partial<APIErrorDetails>,
      fallbackMessage,
      fallbackCode
    );
  }

  if (typeof body.error === "string" && body.error.trim() !== "") {
    const parsed = parseErrorDiagnostic(new Error(body.error), fallbackMessage);
    return normalizeDetails(parsed, fallbackMessage, fallbackCode);
  }

  return normalizeDetails({}, fallbackMessage, fallbackCode);
};
