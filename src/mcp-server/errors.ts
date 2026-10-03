/** Client-facing error codes gptqueue raises itself. */
export type GptQueueErrorCode = "SESSION_UNAVAILABLE" | "AGENT_NOT_REGISTERED";

/**
 * An error whose classification travels in `code`, not in its wording, so
 * stableToolError never has to pattern-match gptqueue's own messages.
 */
export class GptQueueError extends Error {
  constructor(readonly code: GptQueueErrorCode, message: string) {
    super(message);
    this.name = "GptQueueError";
  }
}
