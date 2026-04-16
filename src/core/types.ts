/**
 * Session and Mailbox types for the transport-agnostic core.
 *
 * All interfaces are Readonly to encourage immutable usage.
 * State transitions produce new objects via spread/copy, not mutation.
 */

/** A live lease representing one connected transport/client. */
export interface SessionRecord {
  readonly session_id: string;
  readonly agent_name: string;
  readonly role: "publisher" | "consumer" | "both";
  readonly description?: string;
  readonly created_at: string;
  readonly last_seen: string;
  readonly transport?: string;
}

/** Canonical metadata for a named agent mailbox. */
export interface MailboxMeta {
  readonly agent_name: string;
  readonly max_size: number;
  readonly created_at: string;
}

/** Lease state derived from TTL presence in Redis. */
export interface LeaseState {
  readonly session_id: string;
  readonly alive: boolean;
  readonly ttl_seconds: number;
}

/** Agent presence computed from session lease aggregation. */
export interface AgentPresence {
  readonly agent_name: string;
  readonly online: boolean;
  readonly active_sessions: readonly string[];
}
