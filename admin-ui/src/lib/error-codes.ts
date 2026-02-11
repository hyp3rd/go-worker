const errorHintByCode: Record<string, string> = {
  unauthenticated:
    "Session credentials are invalid or expired. Sign in again and retry.",
  permissiondenied:
    "You are not allowed to perform this action with the current identity.",
  unavailable:
    "Admin gateway or worker service is unavailable. Verify container health and mTLS settings.",
  deadlineexceeded:
    "The request timed out. Retry, then check backend latency and limits.",
  failedprecondition:
    "A required dependency or state is missing. Verify schedule factory, job config, or queue state.",
  internal:
    "Server-side failure occurred. Use the request ID to inspect gateway and worker logs.",
  invalidargument:
    "Input validation failed. Check required fields and value formats.",
  notfound:
    "Requested resource was not found. Refresh data and retry.",
  resourceexhausted:
    "A configured limit was hit. Lower batch size or adjust admin limits.",
};

const aliasToCanonical: Record<string, string> = {
  permission_denied: "permissiondenied",
  deadline_exceeded: "deadlineexceeded",
  failed_precondition: "failedprecondition",
  invalid_argument: "invalidargument",
  not_found: "notfound",
  resource_exhausted: "resourceexhausted",
  resourceexhausted: "resourceexhausted",
  resourcelimit: "resourceexhausted",
};

const normalizeToken = (code: string) =>
  code
    .trim()
    .toLowerCase()
    .replace(/[^a-z_]/g, "");

export const toCanonicalErrorCode = (code?: string): string | undefined => {
  if (!code) {
    return undefined;
  }

  const normalized = normalizeToken(code);
  if (!normalized) {
    return undefined;
  }

  return aliasToCanonical[normalized] ?? normalized;
};

export const getErrorHint = (code?: string): string | undefined => {
  const canonical = toCanonicalErrorCode(code);
  if (!canonical) {
    return undefined;
  }
  return errorHintByCode[canonical];
};

export const errorCodeFromHTTPStatus = (statusCode: number): string => {
  if (statusCode === 400) {
    return "invalidargument";
  }
  if (statusCode === 401) {
    return "unauthenticated";
  }
  if (statusCode === 403) {
    return "permissiondenied";
  }
  if (statusCode === 404) {
    return "notfound";
  }
  if (statusCode === 408) {
    return "deadlineexceeded";
  }
  if (statusCode === 409) {
    return "alreadyexists";
  }
  if (statusCode === 412) {
    return "failedprecondition";
  }
  if (statusCode === 429) {
    return "resourceexhausted";
  }
  if (statusCode === 503) {
    return "unavailable";
  }
  if (statusCode >= 500) {
    return "internal";
  }
  return "gateway_error";
};
