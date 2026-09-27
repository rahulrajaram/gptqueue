/**
 * TaskClaimStore: Redis-backed durable task-claim persistence for at-least-once
 * batch delivery to durable actor runtimes.
 *
 * A claim atomically pops a bounded batch of raw inbox messages off an actor's
 * inbox list, persists the claim (with the popped tasks) in a claims hash, and
 * indexes it per-actor in a zset scored by the claim's expiry. Acknowledging
 * removes the claim (confirming same-runtime delivery); an unacknowledged
 * claim whose TTL passes is lazily recovered back onto the inbox on the next
 * claim or recovery, so a crashed runtime can never lose delivered-but-unacked
 * messages.
 *
 * Mirrors SessionStore/CustodyStore/WakeLeaseStore's structure: an injected
 * ioredis client and typed ok/error results. Domain failures never throw.
 * The thin conditional Lua scripts perform the atomic boundaries; the ordering
 * and validation semantics live here.
 */

import { Redis } from "ioredis";
import { randomUUID } from "crypto";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  CLAIM_KEYS,
  DLQ_KEYS,
  DLQ_PROVISIONAL,
  SESSION_KEYS,
} from "./keys.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const LUA_DIR = join(__dirname, "..", "mcp-server", "lua");

/** One outstanding durable task claim. Tasks are raw inbox payloads, pop order. */
export interface TaskClaim {
  readonly claim_id: string; // crypto.randomUUID()
  readonly actor_id: string;
  readonly session_id: string; // owning runtime session
  readonly claimed_at: string; // caller-supplied ISO
  readonly expires_at: string; // claimed_at + ttl_seconds
  readonly tasks: readonly string[]; // raw inbox message payloads, in pop order
}

export type TaskClaimStoreErrorCode =
  | "invalid_claim_request"
  | "invalid_renew_request"
  | "concurrency_limit_reached"
  | "unknown_claim"
  | "not_claim_owner"
  | "claim_expired"
  | "budget_exceeded"
  | "store_corrupt"
  | "dlq_entry_not_found";

export type TaskClaimStoreError = Readonly<{
  code: TaskClaimStoreErrorCode;
  message: string;
}>;

export type ClaimResult =
  | Readonly<{ ok: true; claim: TaskClaim | null }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export type AcknowledgeResult =
  | Readonly<{ ok: true; acknowledged: number }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export type RenewResult =
  | Readonly<{ ok: true; claim_id: string; expires_at: string }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export type RecoverResult = Readonly<{
  ok: true;
  recovered: number;
  deadlettered: number;
}>;

export type DeadLetterEntriesInput = Readonly<{
  actor_id: string;
  /** Maximum DLQ entries to return (defaults to the DLQ length bound). */
  limit?: number;
}>;

export type DeadLetterEntry = Readonly<{
  message_id: string | null; // null when the envelope carries no stable id (legacy)
  payload: string; // RAW task payload exactly as stored / requeued
  deadlettered_at: string | null; // message timestamp (DLQ stores raw payloads only)
}>;

export type DeadLetterEntriesResult =
  | Readonly<{ ok: true; entries: readonly DeadLetterEntry[] }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export type RequeueInput = Readonly<{
  actor_id: string;
  message_id: string;
}>;

export type RequeueResult =
  | Readonly<{ ok: true; requeued: number }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export type TaskClaimGetResult =
  | Readonly<{ ok: true; claim: TaskClaim | null }>
  | Readonly<{ ok: false; error: TaskClaimStoreError }>;

export interface ClaimInput {
  readonly actor_id: string;
  readonly session_id: string; // owning runtime session
  readonly max_batch: number; // int 1..16
  readonly ttl_seconds: number; // int 1..3600
  /**
   * Outstanding-claims ceiling for this actor, enforced atomically by the
   * claim script (ZCARD on the actor's index, which is accurate because lazy
   * recovery has already removed expired claims). `undefined` = unlimited
   * (preserves the historical behavior for plain agents). When provided it
   * must be an integer of at least 1.
   */
  readonly max_concurrent_claims?: number;
  readonly now: string; // ISO timestamp; the adapter layer reads the clock, not the core
}

export interface RecoverInput {
  readonly actor_id: string;
  readonly now: string; // ISO timestamp; the adapter layer reads the clock, not the core
}

export interface AcknowledgeInput {
  readonly claim_id: string;
  readonly actor_id: string;
  readonly session_id: string;
}

export interface RenewInput {
  readonly claim_id: string;
  readonly actor_id: string;
  readonly session_id: string;
  /** Lease extension in whole seconds (int 1..3600), applied from the renew instant. */
  readonly ttl_seconds: number;
  /** Lifetime budget in whole seconds; renewal expiry is capped at claimed_at + budget. */
  readonly budget_seconds: number;
  readonly now: string; // ISO timestamp; the adapter layer reads the clock, not the core
}

export interface ActiveClaimInput {
  readonly actor_id: string;
  readonly session_id: string;
}

const EXCERPT = 80;
const excerptOf = (raw: string): string =>
  raw.length <= EXCERPT ? raw : `${raw.slice(0, EXCERPT)}...`;

export class TaskClaimStore {
  private readonly redis: Redis;
  private readonly recoverScript: string;
  private readonly batchClaimScript: string;
  private readonly ackScript: string;
  private readonly renewScript: string;
  private readonly requeueScript: string;

  constructor(redis: Redis) {
    this.redis = redis;
    this.recoverScript = readFileSync(
      join(LUA_DIR, "claims-recover.lua"),
      "utf-8"
    );
    this.batchClaimScript = readFileSync(
      join(LUA_DIR, "claims-batch-claim.lua"),
      "utf-8"
    );
    this.ackScript = readFileSync(join(LUA_DIR, "claims-ack.lua"), "utf-8");
    this.renewScript = readFileSync(join(LUA_DIR, "claims-renew.lua"), "utf-8");
    this.requeueScript = readFileSync(join(LUA_DIR, "claims-requeue.lua"), "utf-8");
  }

  /**
   * Lazily recover every expired outstanding claim for an actor back onto its
   * inbox. Run on every claim before issuing a new one (and available to
   * observers). Returns how many tasks were re-queued and how many were
   * dead-lettered. Recovered messages re-enter at the inbox's tail; a message
   * whose recovery counter exceeds DLQ_PROVISIONAL.RECOVER_CAP is quarantined
   * to the actor's DLQ instead (see the module doc).
   */
  async recoverExpired(input: RecoverInput): Promise<RecoverResult> {
    const nowMs = Date.parse(input.now);
    const [recovered, deadlettered] = (await this.redis.eval(
      this.recoverScript,
      4,
      CLAIM_KEYS.index(input.actor_id),
      CLAIM_KEYS.claims,
      SESSION_KEYS.queue(input.actor_id),
      DLQ_KEYS.list(input.actor_id),
      nowMs,
      DLQ_PROVISIONAL.RECOVER_CAP,
      DLQ_PROVISIONAL.DLQ_MAX_LENGTH,
      DLQ_PROVISIONAL.RECOVER_COUNTER_TTL_SECONDS
    )) as [number, number];
    return { ok: true, recovered, deadlettered };
  }

  /**
   * Read the calling actor's dead-letter queue, newest first. DLQ entries are
   * raw task payloads (the strings LPUSHed by lazy recovery), so each entry's
   * message_id and deadlettered_at are decoded from the stored envelope;
   * legacy payloads yield null for both. Returns at most `limit` entries
   * (defaults to the DLQ length bound).
   */
  async deadLetterEntries(
    input: DeadLetterEntriesInput
  ): Promise<DeadLetterEntriesResult> {
    const limit = input.limit ?? DLQ_PROVISIONAL.DLQ_MAX_LENGTH;
    const clamped = Math.max(1, Math.floor(limit));
    const list = DLQ_KEYS.list(input.actor_id);
    const raw = await this.redis.lrange(list, 0, clamped - 1);
    const entries = raw.map((payload) => {
      let message_id: string | null = null;
      let deadlettered_at: string | null = null;
      try {
        const msg = JSON.parse(payload) as {
          id?: unknown;
          timestamp?: unknown;
        };
        if (typeof msg.id === "string") message_id = msg.id;
        if (typeof msg.timestamp === "string") deadlettered_at = msg.timestamp;
      } catch {
        // Legacy / undecodable envelope: leave message_id and deadlettered_at null.
      }
      return Object.freeze({ message_id, payload, deadlettered_at });
    });
    return { ok: true, entries };
  }

  /**
   * Move one dead-lettered message from the actor's DLQ back to the inbox tail,
   * restoring a fresh recovery budget (the message's counter key is DELeted).
   * One Lua script finds the matching envelope, LREMs it, RPUSHes the raw
   * payload to the inbox and clears the counter atomically, so a failure
   * cannot lose the message between DLQ and inbox. Not found is a typed
   * dlq_entry_not_found error.
   */
  async requeue(input: RequeueInput): Promise<RequeueResult> {
    const moved = await this.redis.eval(
      this.requeueScript,
      3,
      DLQ_KEYS.list(input.actor_id),
      SESSION_KEYS.queue(input.actor_id),
      CLAIM_KEYS.recoverCount(input.actor_id, input.message_id),
      input.message_id
    );
    if (moved !== 1) {
      return fail(
        "dlq_entry_not_found",
        `no DLQ entry for message '${input.message_id}' on actor '${input.actor_id}'`
      );
    }
    return { ok: true, requeued: 1 };
  }

  /**
   * Atomically claim up to max_batch messages from an actor's inbox for the
   * owning runtime session. Returns `claim:null` (an empty batch indication,
   * not an error) when nothing is pending. Lazy recovery runs first so an
   * expired unacked claim's tasks are re-candidate before any new claim.
   *
   * When `max_concurrent_claims` is provided, enforcement runs atomically in
   * the claim script: if the actor already has that many outstanding unacked
   * claims (ZCARD on its index, accurate after lazy recovery), the claim is
   * refused with a `concurrency_limit_reached` error naming the limit.
   */
  async claim(input: ClaimInput): Promise<ClaimResult> {
    if (
      input.actor_id.trim().length === 0 ||
      input.session_id.trim().length === 0 ||
      !Number.isInteger(input.max_batch) ||
      input.max_batch < 1 ||
      input.max_batch > 16 ||
      !Number.isInteger(input.ttl_seconds) ||
      input.ttl_seconds < 1 ||
      input.ttl_seconds > 3600 ||
      (input.max_concurrent_claims !== undefined &&
        (!Number.isInteger(input.max_concurrent_claims) ||
          input.max_concurrent_claims < 1))
    ) {
      return fail(
        "invalid_claim_request",
        "claim requires a non-empty actor_id and session_id, max_batch an integer in 1..16, ttl_seconds an integer in 1..3600, and max_concurrent_claims (when provided) an integer of at least 1"
      );
    }

    // Lazy recovery before any new claim: an expired unacked claim's messages
    // are re-queued (atomically, per claim) before we pop a fresh batch.
    await this.recoverExpired({
      actor_id: input.actor_id,
      now: input.now,
    });

    const claimId = randomUUID();
    const expiresAt = new Date(
      Date.parse(input.now) + input.ttl_seconds * 1000
    ).toISOString();
    const expiresAtMs = Date.parse(expiresAt);

    const raw = (await this.redis.eval(
      this.batchClaimScript,
      3,
      SESSION_KEYS.queue(input.actor_id),
      CLAIM_KEYS.claims,
      CLAIM_KEYS.index(input.actor_id),
      claimId,
      input.actor_id,
      input.session_id,
      input.now,
      expiresAt,
      expiresAtMs,
      input.max_batch,
      // 0 is the "unlimited" sentinel consumed by the script (undefined => the
      // historical plain-agent behavior).
      input.max_concurrent_claims ?? 0
    )) as [number, string];
    const [flag, stored] = raw;

    // Sentinel from the script: the inbox was empty -> an empty batch.
    if (flag === 0) return { ok: true, claim: null };

    // The outstanding-claims ceiling was reached (or exceeded); no messages
    // were popped and the claim was not materialized.
    if (flag === 2) {
      const limit = input.max_concurrent_claims ?? 0;
      return fail(
        "concurrency_limit_reached",
        `actor '${input.actor_id}' already has ${limit} outstanding claim(s); refusing to exceed max_concurrent_claims of ${limit}`
      );
    }

    const claim = this.parseTaskClaim(stored);
    if (claim === null) {
      return fail(
        "store_corrupt",
        `fresh claim ${claimId} failed to round-trip (${excerptOf(stored)}); refusing to operate on it`
      );
    }
    return { ok: true, claim };
  }

  /**
   * Acknowledge an outstanding claim, confirming same-runtime batch delivery.
   * Removes the claim from the hash and index only for its owning session.
   * Returns how many tasks were acknowledged. A second ack is `unknown_claim`;
   * a foreign session is `not_claim_owner` naming the owning session.
   */
  async acknowledge(input: AcknowledgeInput): Promise<AcknowledgeResult> {
    const result = (await this.redis.eval(
      this.ackScript,
      2,
      CLAIM_KEYS.claims,
      CLAIM_KEYS.index(input.actor_id),
      input.claim_id,
      input.actor_id,
      input.session_id
    )) as [number, string];
    const [code, detail] = result;

    if (code === 1) {
      return { ok: true, acknowledged: detail as unknown as number };
    }
    if (code === 2) {
      return fail(
        "unknown_claim",
        `no outstanding claim '${input.claim_id}' for actor '${input.actor_id}'`
      );
    }
    return fail(
      "not_claim_owner",
      `claim '${input.claim_id}' is owned by session '${detail}'`
    );
  }

  /**
   * Renew an outstanding claim, extending its expiry by `ttl_seconds` (from
   * the renew instant) but never past the claim's lifetime budget rendered from
   * its `claimed_at` (claimed_at + budget_seconds). The caller must be the
   * owning session; the claim must not already be expired (renewal cannot
   * resurrect a claim). The new expiry is persisted atomically in both the
   * hash record and the index zset score. A second or foreign renewal, a
   * post-expiry renewal, or a budget-exhausted renewal are each a typed error.
   */
  async renew(input: RenewInput): Promise<RenewResult> {
    if (
      input.claim_id.trim().length === 0 ||
      input.actor_id.trim().length === 0 ||
      input.session_id.trim().length === 0 ||
      !Number.isInteger(input.ttl_seconds) ||
      input.ttl_seconds < 1 ||
      input.ttl_seconds > 3600
    ) {
      return fail(
        "invalid_renew_request",
        "renew requires a non-empty claim_id, actor_id and session_id, and ttl_seconds an integer in 1..3600"
      );
    }

    const result = (await this.redis.eval(
      this.renewScript,
      2,
      CLAIM_KEYS.claims,
      CLAIM_KEYS.index(input.actor_id),
      input.claim_id,
      input.actor_id,
      input.session_id,
      Date.parse(input.now),
      input.ttl_seconds,
      input.budget_seconds
    )) as [number, string];
    const [code, detail] = result;

    if (code === 1) {
      return { ok: true, claim_id: input.claim_id, expires_at: detail };
    }
    if (code === 2) {
      return fail(
        "unknown_claim",
        `no outstanding claim '${input.claim_id}' for actor '${input.actor_id}'`
      );
    }
    if (code === 4) {
      return fail(
        "claim_expired",
        `claim '${input.claim_id}' for actor '${input.actor_id}' has expired and cannot be renewed`
      );
    }
    if (code === 5) {
      return fail(
        "budget_exceeded",
        `claim '${input.claim_id}' for actor '${input.actor_id}' has exhausted its lifetime budget and cannot be renewed`
      );
    }
    return fail(
      "not_claim_owner",
      `claim '${input.claim_id}' is owned by session '${detail}'`
    );
  }

  /**
   * Return the first non-expired outstanding claim for an actor owned by the
   * given session, or null. HGETALL-free: reads the actor's zset members and
   * fetches them with one HMGET. Corrupt stored JSON is skipped (returns
   * nothing) so workload derivation on a hot path can never fail; this is
   * intentionally separate from `get`'s typed store_corrupt path.
   */
  async activeClaimFor(input: ActiveClaimInput): Promise<TaskClaim | null> {
    const members = await this.redis.zrange(CLAIM_KEYS.index(input.actor_id), 0, -1);
    if (members.length === 0) return null;
    const raws = await this.redis.hmget(CLAIM_KEYS.claims, ...members);
    const nowMs = Date.now();
    for (const raw of raws) {
      if (raw === null) continue;
      const claim = this.parseTaskClaim(raw);
      if (claim === null) continue; // skip corrupt silently on the hot path
      if (claim.actor_id !== input.actor_id) continue;
      if (claim.session_id !== input.session_id) continue;
      const expiresMs = Date.parse(claim.expires_at);
      if (Number.isNaN(expiresMs) || nowMs >= expiresMs) continue;
      return claim;
    }
    return null;
  }

  /** Read one claim by id (null when absent), with a typed store_corrupt path. */
  async get(claimId: string): Promise<TaskClaimGetResult> {
    const raw = await this.redis.hget(CLAIM_KEYS.claims, claimId);
    if (raw === null) return { ok: true, claim: null };
    const claim = this.parseTaskClaim(raw);
    if (claim === null) {
      return fail(
        "store_corrupt",
        `stored claim '${claimId}' is corrupt (${excerptOf(raw)}); refusing to operate on it`
      );
    }
    return { ok: true, claim };
  }

  /** Parse and deeply freeze a stored claim; null when malformed. */
  private parseTaskClaim(raw: string): TaskClaim | null {
    try {
      const parsed: unknown = JSON.parse(raw);
      const record = parsed as TaskClaim;
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        typeof record.claim_id === "string" &&
        typeof record.actor_id === "string" &&
        typeof record.session_id === "string" &&
        typeof record.claimed_at === "string" &&
        typeof record.expires_at === "string" &&
        Array.isArray(record.tasks) &&
        record.tasks.every((task) => typeof task === "string")
      ) {
        return Object.freeze({
          claim_id: record.claim_id,
          actor_id: record.actor_id,
          session_id: record.session_id,
          claimed_at: record.claimed_at,
          expires_at: record.expires_at,
          tasks: Object.freeze([...record.tasks]),
        });
      }
    } catch {
      // fall through to null below
    }
    return null;
  }
}

const fail = (
  code: TaskClaimStoreErrorCode,
  message: string
): Readonly<{ ok: false; error: TaskClaimStoreError }> =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });