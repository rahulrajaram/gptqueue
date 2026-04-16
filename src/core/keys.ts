/**
 * Redis key schema for sessions, mailboxes, and leases.
 *
 * New session-aware keys coexist with legacy keys during migration.
 */

/** Legacy keys (kept for backward compatibility during transition). */
export const LEGACY_KEYS = {
  registry: "gptq:registry",
  queue: (agent: string) => `gptq:q:${agent}`,
  meta: (agent: string) => `gptq:meta:${agent}`,
  heartbeat: (agent: string) => `gptq:heartbeat:${agent}`,
} as const;

/** Session-aware key schema. */
export const SESSION_KEYS = {
  /** Canonical agent metadata (hash). */
  agent: (name: string) => `gptq:agent:${name}`,

  /** Durable mailbox queue (list). */
  queue: (name: string) => `gptq:q:${name}`,

  /** Mailbox metadata (hash): max_size, created_at, current_size. */
  mailboxMeta: (name: string) => `gptq:meta:${name}`,

  /** Session record (hash): agent_name, role, description, created_at, last_seen. */
  session: (sessionId: string) => `gptq:session:${sessionId}`,

  /** TTL-backed liveness indicator for a session (string with EX). */
  lease: (sessionId: string) => `gptq:lease:${sessionId}`,

  /** Set of active session IDs for an agent (set). */
  agentSessions: (name: string) => `gptq:agent-sessions:${name}`,

  /** Global agent discovery index (hash, same as legacy registry). */
  registry: "gptq:registry",
} as const;

/** Default constants. */
export const SESSION_DEFAULTS = {
  LEASE_TTL_SECONDS: 30,
  LEASE_REFRESH_INTERVAL_SECONDS: 10,
  DEFAULT_QUEUE_BOUND: 10,
} as const;
