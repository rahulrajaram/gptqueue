import { GptQueueError } from "./errors.js";

export type ToolPayload = Record<string, unknown>;

export function toolResult(
  payload: ToolPayload,
  isError = false,
  legacyPayload: unknown = payload
) {
  const legacyText = typeof legacyPayload === "string"
    ? legacyPayload
    : JSON.stringify(legacyPayload, null, 2);
  return {
    content: [{ type: "text" as const, text: legacyText }],
    structuredContent: payload,
    ...(isError ? { isError: true } : {}),
  };
}

export function stableToolError(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  // gptqueue's own errors carry their code; the text patterns below are only
  // a fallback for errors raised by dependencies (e.g. ioredis).
  const code = error instanceof GptQueueError
    ? error.code
    : /Session .* not found|session.*expired/i.test(message)
    ? "SESSION_UNAVAILABLE"
    : /not registered/i.test(message)
      ? "AGENT_NOT_REGISTERED"
      : /ECONNREFUSED|Redis is already connecting|Connection is closed|connect ETIMEDOUT|max retries/i.test(message)
        ? "REDIS_UNAVAILABLE"
        : "GPTQUEUE_ERROR";
  return toolResult(
    {
      status: "error",
      error: { code, message, retryable: code === "REDIS_UNAVAILABLE" },
    },
    true
  );
}
