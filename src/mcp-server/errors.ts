/** Client-facing error codes gptqueue raises itself. */
export type GptQueueErrorCode = "SESSION_UNAVAILABLE" | "AGENT_NOT_REGISTERED" | "AGENT_NAME_HELD" | "RUNTIME_BINDING_HELD" | "AGENT_NAME_RESERVED" | "AGENT_REGISTRATION_INVALID"
  | "RECIPIENT_RETIRED"
  /** The agent name is not a valid mailbox key component (SEC-1: 1..128 chars of [A-Za-z0-9._-]). */
  | "AGENT_NAME_INVALID"
  /** The name is still held by another live session of a shared registration, so renaming it would strand them. */
  | "AGENT_NAME_SHARED"
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
