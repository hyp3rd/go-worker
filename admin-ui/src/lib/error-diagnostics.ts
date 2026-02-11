import { getErrorHint, toCanonicalErrorCode } from "@/lib/error-codes";

export type ErrorDiagnostic = {
  message: string;
  code?: string;
  requestId?: string;
  hint?: string;
};

const requestIDPattern = /\[([0-9a-fA-F-]{8,})\]\s*$/;
const codePattern = /\(([^()]+)\)\s*(?:\[[0-9a-fA-F-]{8,}\])?\s*$/;

export const parseErrorDiagnostic = (
  error: unknown,
  fallback: string
): ErrorDiagnostic => {
  const raw =
    error instanceof Error && error.message.trim()
      ? error.message.trim()
      : fallback;

  const requestIDMatch = raw.match(requestIDPattern);
  const requestId = requestIDMatch?.[1];
  const withoutRequestID = requestIDMatch
    ? raw.slice(0, requestIDMatch.index).trim()
    : raw;

  const codeMatch = withoutRequestID.match(codePattern);
  const code = toCanonicalErrorCode(codeMatch?.[1]);
  const message = codeMatch
    ? withoutRequestID.slice(0, codeMatch.index).trim()
    : withoutRequestID;

  return {
    message: message || fallback,
    code,
    requestId,
    hint: getErrorHint(code),
  };
};
