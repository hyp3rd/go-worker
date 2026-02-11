import { parseAPIErrorPayload } from "@/lib/api-error-payload";

export const formatAPIErrorMessage = (
  message: string,
  code: string,
  requestId?: string
) => `${message} (${code})${requestId ? ` [${requestId}]` : ""}`;

export const readAPIErrorMessage = async (
  response: Response,
  fallbackMessage: string
) => {
  const payload = (await response.json().catch(() => ({}))) as unknown;
  const parsed = parseAPIErrorPayload(payload, fallbackMessage);
  return formatAPIErrorMessage(parsed.message, parsed.code, parsed.requestId);
};

export const throwAPIResponseError = async (
  response: Response,
  fallbackMessage: string
): Promise<never> => {
  throw new Error(await readAPIErrorMessage(response, fallbackMessage));
};
