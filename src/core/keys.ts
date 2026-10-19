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

  /**
   * Rename-rollback bookkeeping beside the registry, never read by any public
   * path (hash): field = agent name, value = the raw provisional registry
   * value the latest in-flight registration wrote.
   */
  registryPending: "gptq:registry:pending",

  /**
   * Rename-rollback bookkeeping (hash): field = agent name, value = the raw
   * last completed registry value a failed registration restores.
   */
  registryRestore: "gptq:registry:restore",

  /** TTL-backed legacy liveness heartbeat per agent (string with EX). */
  heartbeat: (name: string) => `gptq:heartbeat:${name}`,

  /** Live runtime binding record for an agent (string with EX). */
  runtimeBinding: (agent: string) => `gptq:runtime-binding:${agent}`,

  /** Retry-deduplication namespace; individual keys expire after 24 hours. */
  idempotency: (sender: string) => `gptq:idempotency:${sender}`,

  /**
   * One retry-deduplication key, scoped to (sender, recipient, caller key).
   * Both variable parts are URI-encoded, so neither contains ':' and the
   * `:to=` segment can never collide with a legacy sender-only key
   * (`gptq:idempotency:<sender>:<encoded key>`), which is no longer read.
   */
  idempotencyFor: (sender: string, recipient: string, key: string) =>
    `gptq:idempotency:${sender}:to=${encodeURIComponent(recipient)}:${encodeURIComponent(key)}`,

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

  /** Advisory subject record of a sent task (string JSON with EX, see core/task-ledger.ts). */
  taskLedger: (taskId: string) => `gptq:task-ledger:${taskId}`,

  /** Marker that the request's owner replied to a task (string ISO time with EX, see core/task-ledger.ts). */
  taskAnswered: (taskId: string) => `gptq:task-answered:${taskId}`,

  /** Watchdog reminders sent to one recipient (zset: fire key -> ms), for the hourly cap (W3). */
  watchdogSent: (recipient: string) => `gptq:watchdog:sent:${recipient}`,

  /** Summary of the last real watchdog pass (string JSON with EX), read by --status (W6). */
  watchdogLastPass: "gptq:watchdog:last-pass",

  /** Summary of the last non-skipped real watchdog pass (string JSON with EX), judged by --status (W3). */
  watchdogLastSubstantivePass: "gptq:watchdog:last-substantive-pass",

  /** Watchdog pass lock: one pass at a time (string token with PX). */
  watchdogPassLock: "gptq:watchdog:pass-lock",

  /** Watchdog fire record: when an action keyed by `key` last took effect (string JSON with EX). */
  watchdogFired: (key: string) => `gptq:watchdog:fired:${key}`,

  /** Latest turn phase (running/idle) reported by an agent's runtime hooks (string JSON with EX). */
  turnState: (agent: string) => `gptq:turn-state:${agent}`,

  /** Mail event id the last Stop-gate block for a runtime session cited (string with EX). */
  stopGate: (client: string, runtimeId: string) => `gptq:stop-gate:${sha256(JSON.stringify([client, runtimeId]))}`,

  /** Experimental-wrapper exclusive ownership claim for an agent name (string). */
  wrapperClaim: (agent: string) => `gptq:experimental-wrapper-claim:${sha256(agent)}`,

  /** Stable mailbox mapping for a (client, runtime_id) binding (string). */
  runtimeMailbox: (client: string, runtimeId: string) =>
    `gptq:runtime-mailbox:${sha256(JSON.stringify([client, runtimeId]))}`,

  /** Append-only audit of identity continuity adoptions (stream). */
  continuityAudit: "gptq:continuity-audit",

  /** Append-only audit of agent-name takeovers from dead holders (stream). */
  identityAudit: "gptq:identity-audit",
  /**
   * Append-only audit of committed registrations (stream, MAXLEN ~ 10000):
   * one event per registry publication of a new session, written in the same
   * script as it. Best-effort: carries session tags, never raw session ids.
   */
  registrationAudit: "gptq:registration-audit",
  /**
   * Audit gap marker (string, epoch ms of the latest registration that could
   * not be audited because the audit key above was unusable). Registration
   * never fails over the audit; it records this instead.
   */
  registrationAuditGap: "gptq:registration-audit:gap",
  /**
   * DL-2: names registered as a sidecar's per-startup provisional identity
   * and not yet adopted or bound. The provisional sweeper retires a member
   * whose sessions all expired (core/provisional-sweeper.ts).
   */
  provisional: "gptq:provisional-identities",
  /**
   * COR-F2: forwarding tombstone a retired or adopted provisional name leaves
   * behind (value "to:<bound mailbox>" or "retired"). The push
   * scripts honour it, so a send that resolved the name just before its
   * retirement is forwarded or refused instead of stranded.
   */
  forward: (name: string) => `gptq:forward:${name}`,
} as const;

/** Default constants. */
export const SESSION_DEFAULTS = {
  /** Lifetime of a COR-F2 forwarding tombstone; covers the send path's retry window many times over. */
  FORWARD_TOMBSTONE_TTL_SECONDS: 86400,
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
  /**
   * Per-actor count (string, INCRBY'd in claims-recover.lua) of DLQ entries
   * the DLQ_MAX_LENGTH trim has discarded, reported by dlq_status so drops are
   * never silent. claims-purge.lua deletes it by the same literal prefix.
   */
  dropped: (actorId: string) => `gptq:dlq-dropped:${actorId}`,
  /** Per-actor hash: DLQ'd message id -> reason, when one is recorded (e.g. recipient_retired). */
  reasons: (actorId: string) => `gptq:dlq-reason:${actorId}`,
} as const;

/** Reserved sender of gptqueue's own envelopes (e.g. RECIPIENT_RETIRED); no agent may register it. */
export const SYSTEM_SENDER = "gptqueue-system";

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
