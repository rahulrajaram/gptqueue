/**
 * Redis key schema for sessions, mailboxes, and leases. This is the single
 * source of truth for every `gptq:*` key string (and the mailbox size bound).
 * Lua scripts that build keys from a prefix inside the script (e.g.
 * `'gptq:lease:' .. sid`) must stay in sync with the builders here.
 */

import { createHash } from "node:crypto";

const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");

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

  /** Global agent discovery index (hash). */
  registry: "gptq:registry",

  /** TTL-backed legacy liveness heartbeat per agent (string with EX). */
  heartbeat: (name: string) => `gptq:heartbeat:${name}`,

  /** Live runtime binding record for an agent (string with EX). */
  runtimeBinding: (agent: string) => `gptq:runtime-binding:${agent}`,

  /** Retry-deduplication namespace; individual keys expire after 24 hours. */
  idempotency: (sender: string) => `gptq:idempotency:${sender}`,

  /** Header-only wake events for an agent's inbox (stream). */
  inboxEvents: (agent: string) => `gptq:inbox-events:${agent}`,

  /** Observability trace of an agent's inbox activations (stream). */
  inboxTrace: (agent: string) => `gptq:inbox-trace:${agent}`,

  /** Owner of an outstanding request awaiting a reply (string with EX). */
  outstanding: (agent: string, requestId: string) => `gptq:outstanding:${agent}:${requestId}`,

  /** Registered-shell activation record for an agent (string). */
  activation: (agent: string) => `gptq:activation:${agent}`,

  /** Registered-shell diagnostic profile for an agent (string). */
  agentProfile: (agent: string) => `gptq:agent-profile:${agent}`,

  /** Marker that an agent has recent outbound traffic (string with EX). */
  outboundActivity: (agent: string) => `gptq:outbound-activity:${agent}`,

  /** Experimental-wrapper exclusive ownership claim for an agent name (string). */
  wrapperClaim: (agent: string) => `gptq:experimental-wrapper-claim:${sha256(agent)}`,

  /** Stable mailbox mapping for a (client, runtime_id) binding (string). */
  runtimeMailbox: (client: string, runtimeId: string) =>
    `gptq:runtime-mailbox:${sha256(JSON.stringify([client, runtimeId]))}`,

  /** Append-only audit of identity continuity adoptions (stream). */
  continuityAudit: "gptq:continuity-audit",
} as const;

/** Default constants. */
export const SESSION_DEFAULTS = {
  LEASE_TTL_SECONDS: 30,
  LEASE_REFRESH_INTERVAL_SECONDS: 10,
  DEFAULT_QUEUE_BOUND: 10,
} as const;

/** Worktree custody record key schema. */
export const CUSTODY_KEYS = {
  /** Hash: field = worktree_path, value = JSON-serialized stored custody record. */
  records: "gptq:custody",
} as const;

/** Durable actor directory key schema. */
export const ACTOR_KEYS = {
  /** Hash: field = actor_id, value = JSON-serialized actor directory record. */
  profiles: "gptq:actor-profiles",
} as const;

/** Per-actor coalescing wake lease key schema. */
export const WAKE_LEASE_KEYS = {
  /** String key per actor with TTL: JSON wake lease. */
  lease: (actorId: string) => `gptq:wake-lease:${actorId}`,
} as const;

/** Durable task-claim key schema (at-least-once batch delivery). */
export const CLAIM_KEYS = {
  /** Hash: field = claim_id, value = JSON-serialized claim record. */
  claims: "gptq:claims",
  /** Per-actor zset of outstanding claims: member = claim_id, score = expires_at epoch ms. */
  index: (actorId: string) => `gptq:claims-index:${actorId}`,
  /**
   * Sidecar recovery counter per message (string, INCR'd in claims-recover.lua):
   * counts how many times a message has been recovered onto its inbox so lazy
   * recovery can quarantine a repeatedly-failed message to the DLQ. Gets a long
   * EX on first INCR to bound orphans.
   */
  recoverCount: (actorId: string, messageId: string) =>
    `gptq:rc:${actorId}:${messageId}`,
} as const;

/** Dead-letter queue (DLQ) key schema. Lists store RAW task payloads (newest at head). */
export const DLQ_KEYS = {
  /** Per-actor list of dead-lettered raw task payloads; length bounded by DLQ_PROVISIONAL.DLQ_MAX_LENGTH. */
  list: (actorId: string) => `gptq:dlq:${actorId}`,
} as const;

/**
 * PROVISIONAL dead-letter / recovery-quarantine / claim-lifecycle policy
 * constants. These are named, documented placeholders pending principal
 * calibration: the recovery cap, the DLQ length bound, the per-message
 * recovery-counter TTL, and the claim lifetime budget. Tuning these is policy,
 * not code — freezes are imports, so tests that need a smaller value invoke the
 * relevant Lua directly with a small ARGV.
 */
export const DLQ_PROVISIONAL = {
  /** Max recoveries of one message before lazy recovery quarantines it to the DLQ. */
  RECOVER_CAP: 5,
  /** Max DLQ entries kept per actor (newest retained; trimmed entries are dropped). */
  DLQ_MAX_LENGTH: 1000,
  /** TTL (seconds) on each per-message recovery counter once created, bounding orphans. */
  RECOVER_COUNTER_TTL_SECONDS: 604800, // 7 days
  /**
   * PROVISIONAL claim lifetime budget (seconds): the hard cap on how far past
   * its claimed_at instant a claim may be renewed (`claimed_at_ms + budget`),
   * so an endlessly-renewing runtime cannot hold a batch forever. 86400 = 1 day.
   */
  CLAIM_LIFETIME_BUDGET_SECONDS: 86400,
} as const;
