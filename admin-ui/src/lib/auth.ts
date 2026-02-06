export const sessionCookieName = "workerctl_session";
export const sessionMaxAgeSeconds = 60 * 60 * 8;
export const sessionRefreshAfterSeconds = 60 * 15;
const sessionTokenVersion = "v2";
const sessionMaxSkewSeconds = 30;
const encoder = new TextEncoder();
export const auditCookieName = "workerctl_audit";
export const auditCookieMaxAgeSeconds = 30;

export const getAdminPassword = () =>
  (process.env.WORKER_ADMIN_PASSWORD ?? process.env.ADMIN_UI_PASSWORD ?? "")
    .trim();

const base64UrlEncode = (bytes: Uint8Array) => {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
};

const base64UrlDecode = (value: string) => {
  let base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const pad = base64.length % 4;
  if (pad) {
    base64 += "=".repeat(4 - pad);
  }
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
};

const timingSafeEqual = (a: Uint8Array, b: Uint8Array) => {
  if (a.length !== b.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) {
    diff |= a[i] ^ b[i];
  }
  return diff === 0;
};

const signSession = async (password: string, issuedAt: number) => {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(password),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const payload = encoder.encode(`${sessionTokenVersion}.${issuedAt}`);
  const signature = await crypto.subtle.sign("HMAC", key, payload);
  return base64UrlEncode(new Uint8Array(signature));
};

export const parseSessionToken = (token: string) => {
  const parts = token.split(".");
  if (parts.length !== 3) {
    return null;
  }
  const [version, issuedAtRaw, signature] = parts;
  if (!version || !issuedAtRaw || !signature) {
    return null;
  }
  const issuedAt = Number.parseInt(issuedAtRaw, 10);
  if (!Number.isFinite(issuedAt) || issuedAt <= 0) {
    return null;
  }
  return { version, issuedAt, signature };
};

export const createSessionToken = async (password: string, issuedAt?: number) => {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const issued = issuedAt && issuedAt > 0 ? issuedAt : nowSeconds;
  const signature = await signSession(password, issued);
  return `${sessionTokenVersion}.${issued}.${signature}`;
};

export const getSessionExpiry = (token: string) => {
  const parsed = parseSessionToken(token);
  if (!parsed || parsed.version !== sessionTokenVersion) {
    return null;
  }
  return (parsed.issuedAt + sessionMaxAgeSeconds) * 1000;
};

export const verifySessionToken = async (token: string, password: string) => {
  const parsed = parseSessionToken(token);
  if (!parsed || parsed.version !== sessionTokenVersion) {
    return false;
  }
  const nowSeconds = Math.floor(Date.now() / 1000);
  if (parsed.issuedAt - nowSeconds > sessionMaxSkewSeconds) {
    return false;
  }
  if (nowSeconds - parsed.issuedAt > sessionMaxAgeSeconds) {
    return false;
  }
  const expected = await signSession(password, parsed.issuedAt);
  return timingSafeEqual(
    base64UrlDecode(parsed.signature),
    base64UrlDecode(expected),
  );
};

export const refreshSessionTokenIfNeeded = async (
  token: string,
  password: string,
) => {
  const parsed = parseSessionToken(token);
  if (!parsed || parsed.version !== sessionTokenVersion) {
    return "";
  }
  const nowSeconds = Math.floor(Date.now() / 1000);
  if (nowSeconds - parsed.issuedAt > sessionMaxAgeSeconds) {
    return "";
  }
  if (nowSeconds - parsed.issuedAt < sessionRefreshAfterSeconds) {
    return "";
  }
  return createSessionToken(password, nowSeconds);
};

export type AuditEvent = "login" | "logout";

export const createAuditCookieValue = (event: AuditEvent) =>
  `${event}:${Date.now()}`;

export const parseAuditCookieValue = (value?: string) => {
  if (!value) {
    return null;
  }
  const [event, ts] = value.split(":");
  if (event !== "login" && event !== "logout") {
    return null;
  }
  const timestamp = Number.parseInt(ts ?? "", 10);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return null;
  }
  return { event, timestamp };
};

export const cookieOptions = {
  httpOnly: true,
  sameSite: "lax" as const,
  secure: process.env.NODE_ENV === "production",
  path: "/",
  maxAge: sessionMaxAgeSeconds,
};

export const auditCookieOptions = {
  httpOnly: true,
  sameSite: "lax" as const,
  secure: process.env.NODE_ENV === "production",
  path: "/",
  maxAge: auditCookieMaxAgeSeconds,
};
