/** Client-facing error codes gptqueue raises itself. */
export type GptQueueErrorCode = "SESSION_UNAVAILABLE" | "AGENT_NOT_REGISTERED" | "AGENT_NAME_HELD" | "RUNTIME_BINDING_HELD" | "AGENT_NAME_RESERVED" | "AGENT_REGISTRATION_INVALID"
  | "RECIPIENT_RETIRED"
  /** Startup registration has not settled yet but is still being attempted: retry later. */
  | "REGISTRATION_PENDING";

/** Structured, JSON-safe details an error carries to the client. */
export type GptQueueErrorDetails = Readonly<Record<string, string | number | boolean>>;

/**
 * An error whose classification travels in `code`, not in its wording, so
 * stableToolError never has to pattern-match gptqueue's own messages.
 */
export class GptQueueError extends Error {
  constructor(
    readonly code: GptQueueErrorCode,
    message: string,
    readonly details?: GptQueueErrorDetails
  ) {
    super(message);
    this.name = "GptQueueError";
  }
}
