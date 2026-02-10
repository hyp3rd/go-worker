import crypto from "crypto";
import fs from "fs";
import http from "http";
import https from "https";

type GatewayRequest = {
  method: "GET" | "POST" | "DELETE";
  path: string;
  body?: unknown;
};

type GatewayRawRequest = {
  method?: "GET" | "POST" | "DELETE";
  path: string;
  headers?: Record<string, string>;
};

type GatewayConfig = {
  baseUrl: string;
  certPath: string;
  keyPath: string;
  caPath: string;
};

let cachedConfig: GatewayConfig | null = null;
let cachedTLS:
  | { cert: Buffer; key: Buffer; ca: Buffer; rejectUnauthorized: boolean }
  | null = null;

const defaultGatewayUrl = "https://127.0.0.1:8081";
const gatewayTimeoutMs = 4000;
const gatewayRetryDelayMs = 200;

const loadConfig = (): GatewayConfig => {
  if (cachedConfig) {
    return cachedConfig;
  }

  cachedConfig = {
    baseUrl: process.env.WORKER_ADMIN_API_URL ?? defaultGatewayUrl,
    certPath: process.env.WORKER_ADMIN_MTLS_CERT ?? "",
    keyPath: process.env.WORKER_ADMIN_MTLS_KEY ?? "",
    caPath: process.env.WORKER_ADMIN_MTLS_CA ?? "",
  };

  return cachedConfig;
};

const loadTLS = (config: GatewayConfig) => {
  if (cachedTLS) {
    return cachedTLS;
  }

  if (!config.certPath || !config.keyPath || !config.caPath) {
    throw new Error("WORKER_ADMIN_MTLS_CERT/KEY/CA are required");
  }

  cachedTLS = {
    cert: fs.readFileSync(config.certPath),
    key: fs.readFileSync(config.keyPath),
    ca: fs.readFileSync(config.caPath),
    rejectUnauthorized: true,
  };

  return cachedTLS;
};

export const gatewayRequest = async <T>({
  method,
  path,
  body,
}: GatewayRequest): Promise<T> => {
  const config = loadConfig();
  const url = new URL(path, config.baseUrl);
  const isTLS = url.protocol === "https:";

  const payload = body ? Buffer.from(JSON.stringify(body)) : undefined;
  const headers: Record<string, string> = {
    Accept: "application/json",
    "X-Request-Id": crypto.randomUUID(),
  };
  if (payload) {
    headers["Content-Type"] = "application/json";
    headers["Content-Length"] = payload.length.toString();
  }

  const requestOptions: https.RequestOptions = {
    method,
    headers,
  };

  if (isTLS) {
    Object.assign(requestOptions, loadTLS(config));
  }

  const attempts = method === "GET" ? 2 : 1;
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await executeGatewayRequest<T>({
        url,
        requestOptions,
        payload,
        timeoutMs: gatewayTimeoutMs,
      });
    } catch (err) {
      lastError = err as Error;
      if (attempt < attempts) {
        await new Promise((resolve) => setTimeout(resolve, gatewayRetryDelayMs));
        continue;
      }
    }
  }

  throw lastError ?? new Error("gateway_request_failed");
};

const executeGatewayRequest = async <T>({
  url,
  requestOptions,
  payload,
  timeoutMs,
}: {
  url: URL;
  requestOptions: https.RequestOptions;
  payload?: Buffer;
  timeoutMs: number;
}): Promise<T> => {
  return new Promise<T>((resolve, reject) => {
    const client = url.protocol === "https:" ? https : http;
    const req = client.request(url, requestOptions, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        const headerValue = res.headers["x-request-id"];
        const requestId = Array.isArray(headerValue)
          ? headerValue[0]
          : headerValue;

        if (!res.statusCode || res.statusCode >= 400) {
          const parsed = parseGatewayError(raw);
          const message = parsed
            ? `${parsed.message} (${parsed.code})${requestId ? ` [${requestId}]` : ""}`
            : raw || `gateway error ${res.statusCode}`;
          reject(new Error(message));
          return;
        }

        try {
          resolve(JSON.parse(raw) as T);
        } catch {
          const message = `invalid json${requestId ? ` [${requestId}]` : ""}`;
          reject(new Error(message));
        }
      });
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error("gateway_timeout"));
    });
    req.on("error", (err) => reject(err));

    if (payload) {
      req.write(payload);
    }
    req.end();
  });
};

const parseGatewayError = (
  raw: string
):
  | {
      code: string;
      message: string;
    }
  | null => {
  try {
    const parsed = JSON.parse(raw) as {
      error?: { code?: string; message?: string };
    };
    if (parsed?.error?.message) {
      return {
        code: parsed.error.code ?? "gateway_error",
        message: parsed.error.message,
      };
    }
    return null;
  } catch {
    return null;
  }
};

export const gatewayStream = async (
  path: string,
  opts?: { lastEventId?: string }
) => {
  const config = loadConfig();
  const url = new URL(path, config.baseUrl);
  const isTLS = url.protocol === "https:";

  const headers: Record<string, string> = {
    Accept: "text/event-stream",
    "X-Request-Id": crypto.randomUUID(),
  };
  const lastEventID = opts?.lastEventId?.trim();
  if (lastEventID) {
    headers["Last-Event-ID"] = lastEventID;
  }

  const requestOptions: https.RequestOptions = {
    method: "GET",
    headers,
  };

  if (isTLS) {
    Object.assign(requestOptions, loadTLS(config));
  }

  const client = isTLS ? https : http;

  return new Promise<{
    statusCode: number;
    headers: http.IncomingHttpHeaders;
    stream: NodeJS.ReadableStream;
  }>((resolve, reject) => {
    const req = client.request(url, requestOptions, (res) => {
      const statusCode = res.statusCode ?? 0;
      if (statusCode >= 400) {
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          const parsed = parseGatewayError(raw);
          const message = parsed
            ? `${parsed.message} (${parsed.code})`
            : raw || `gateway error ${statusCode}`;
          reject(new Error(message));
        });
        return;
      }

      resolve({
        statusCode,
        headers: res.headers,
        stream: res,
      });
    });

    req.on("error", (err) => reject(err));
    req.end();
  });
};

export const gatewayRawRequest = async ({
  method = "GET",
  path,
  headers: extraHeaders,
}: GatewayRawRequest) => {
  const config = loadConfig();
  const url = new URL(path, config.baseUrl);
  const isTLS = url.protocol === "https:";

  const headers: Record<string, string> = {
    Accept: "*/*",
    "X-Request-Id": crypto.randomUUID(),
    ...extraHeaders,
  };

  const requestOptions: https.RequestOptions = {
    method,
    headers,
  };

  if (isTLS) {
    Object.assign(requestOptions, loadTLS(config));
  }

  const client = isTLS ? https : http;

  return new Promise<{
    statusCode: number;
    headers: http.IncomingHttpHeaders;
    stream: NodeJS.ReadableStream;
  }>((resolve, reject) => {
    const req = client.request(url, requestOptions, (res) => {
      const statusCode = res.statusCode ?? 0;
      if (statusCode >= 400) {
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          const parsed = parseGatewayError(raw);
          const message = parsed
            ? `${parsed.message} (${parsed.code})`
            : raw || `gateway error ${statusCode}`;
          reject(new Error(message));
        });
        return;
      }

      resolve({
        statusCode,
        headers: res.headers,
        stream: res,
      });
    });

    req.on("error", (err) => reject(err));
    req.end();
  });
};
