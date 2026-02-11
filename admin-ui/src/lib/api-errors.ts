import { NextResponse } from "next/server";
import { parseErrorDiagnostic } from "@/lib/error-diagnostics";
import {
  errorCodeFromHTTPStatus,
  getErrorHint,
  toCanonicalErrorCode,
} from "@/lib/error-codes";
import type { APIErrorDetails } from "@/lib/api-error-payload";
import { parseAPIErrorPayload } from "@/lib/api-error-payload";

type APIErrorBody = {
  error: string;
  errorDetail: APIErrorDetails;
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

const toBody = (details: APIErrorDetails): APIErrorBody => ({
  error: details.message,
  errorDetail: details,
});

export const apiErrorResponse = (
  error: unknown,
  fallbackMessage: string,
  statusCode: number
) => {
  const parsed = parseErrorDiagnostic(error, fallbackMessage);
  const details = normalizeDetails(
    parsed,
    fallbackMessage,
    errorCodeFromHTTPStatus(statusCode)
  );

  return NextResponse.json(toBody(details), { status: statusCode });
};

export const apiCodeErrorResponse = (
  code: string,
  message: string,
  statusCode: number
) => {
  const details = normalizeDetails(
    {
      code,
      message,
    },
    message,
    errorCodeFromHTTPStatus(statusCode)
  );

  return NextResponse.json(toBody(details), { status: statusCode });
};

export { parseAPIErrorPayload };
