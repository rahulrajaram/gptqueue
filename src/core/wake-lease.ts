/**
 * WakeLeaseStore: per-actor coalescing wake leases backed by Redis TTL.
 *
 * A wake lease is a controller-issued marker that exactly one activation is in
 * flight for an actor. Acquisition is atomic and coalescing: if a lease already
 * exists, the existing lease is returned (coalesced) and NO new lease is
 * issued — concurrent wake requests collapse to one outstanding activation.
 * Redis key TTL is the expiry mechanism, so no compare-on-read is needed.
 * Clearing is a conditional delete matched on `.lease_id`.
 *
 * Mirrors SessionStore/CustodyStore's structure: an injected ioredis client
 * and typed ok/error results. Domain failures never throw.
 */

import { Redis } from "ioredis";
import { randomUUID } from "crypto";
import { readFileSync } from "fs";
import { join } from "path";
import { WAKE_LEASE_KEYS } from "./keys.js";
import { LUA_DIR, describeStored, firstNonStringField } from "./stored-read.js";


export interface WakeLease {
  readonly lease_id: string;
  readonly actor_id: string;
  readonly issued_by_session: string;
  readonly issued_at: string;
  readonly expires_at: string;
  /** Additive spawn evidence; set best-effort by the dispatcher after launch. */
  readonly spawned_pid?: number;
  readonly spawned_at?: string;
}

export type WakeLeaseError = Readonly<{
  code: "invalid_lease_duration" | "store_corrupt";
  message: string;
}>;

export type WakeLeaseAcquireResult =
  | Readonly<{ ok: true; lease: WakeLease; coalesced: boolean }>
  | Readonly<{ ok: false; error: WakeLeaseError }>;

export type WakeLeaseClearResult = Readonly<{
  ok: true;
  cleared: boolean;
}>;

export type WakeLeaseAttachSpawnResult = Readonly<{
  ok: true;
  attached: boolean;
}>;

export interface WakeLeaseAcquireInput {
  readonly actor_id: string;
  readonly issued_by_session: string;
  /** Whole-second TTL, bounded to 1..3600. */
  readonly lease_seconds: number;
  readonly now: string; // ISO timestamp; the adapter layer reads the clock, not the core
}

export interface WakeLeaseClearInput {
  readonly actor_id: string;
  readonly lease_id: string;
}

export interface WakeLeaseAttachSpawnInput {
  readonly actor_id: string;
  readonly lease_id: string;
  readonly pid: number;
  readonly spawned_at: string; // ISO timestamp; the adapter layer reads the clock, not the core
}


export class WakeLeaseStore {
  private readonly redis: Redis;
  private readonly acquireScript: string;
  private readonly clearScript: string;
  private readonly attachSpawnScript: string;

  constructor(redis: Redis) {
    this.redis = redis;
    this.acquireScript = readFileSync(
      join(LUA_DIR, "wake-lease-acquire.lua"),
      "utf-8"
    );
    this.clearScript = readFileSync(
      join(LUA_DIR, "wake-lease-clear.lua"),
      "utf-8"
    );
    this.attachSpawnScript = readFileSync(
      join(LUA_DIR, "wake-lease-attach-spawn.lua"),
      "utf-8"
    );
  }

  /**
   * Acquire a coalescing wake lease for an actor. Returns the (new or already
   * outstanding) lease and whether it was deduplicated.
   */
  async acquire(
    input: WakeLeaseAcquireInput
  ): Promise<WakeLeaseAcquireResult> {
    if (
      !Number.isInteger(input.lease_seconds) ||
      input.lease_seconds < 1 ||
      input.lease_seconds > 3600
    ) {
      return fail(
        "invalid_lease_duration",
        "lease_seconds must be an integer between 1 and 3600"
      );
    }

    const lease: WakeLease = Object.freeze({
      lease_id: randomUUID(),
      actor_id: input.actor_id,
      issued_by_session: input.issued_by_session,
      issued_at: input.now,
      expires_at: new Date(
        Date.parse(input.now) + input.lease_seconds * 1000
      ).toISOString(),
    });

    const result = (await this.redis.eval(
      this.acquireScript,
      1,
      WAKE_LEASE_KEYS.lease(input.actor_id),
      input.actor_id,
      input.issued_by_session,
      lease.lease_id,
      lease.issued_at,
      lease.expires_at,
      input.lease_seconds
    )) as [string, number];
    const [raw, coalesced] = result;

    // The Lua script stores exactly the JSON we encode, so only external
    // corruption would make this unparseable.
    const stored = this.parseLease(raw);
    if (stored === null) {
      return fail(
        "store_corrupt",
        `stored wake lease is corrupt (${describeStored(raw, firstNonStringField(raw, LEASE_STRING_FIELDS))}); refusing to operate on it`
      );
    }
    return { ok: true, lease: stored, coalesced: coalesced === 1 };
  }

  /** Read the current wake lease for an actor, if any. */
  async get(actorId: string): Promise<WakeLease | null> {
    const raw = await this.redis.get(WAKE_LEASE_KEYS.lease(actorId));
    if (raw === null) return null;
    // A corrupt stored lease is treated as absent on read; clear() handles it
    // idempotently via the conditional Lua delete.
    return this.parseLease(raw);
  }

  /**
   * Conditionally clear a wake lease. Deletes only when the stored lease's
   * `.lease_id` matches the caller's. A mismatch (or expired/absent lease) is
   * `cleared:false` — NOT an error — so the runtime_ready race is idempotent.
   */
  async clear(input: WakeLeaseClearInput): Promise<WakeLeaseClearResult> {
    const result = (await this.redis.eval(
      this.clearScript,
      1,
      WAKE_LEASE_KEYS.lease(input.actor_id),
      input.lease_id
    )) as number;
    return { ok: true, cleared: result === 1 };
  }

  /**
   * Best-effort, conditional attach of spawn evidence to a wake lease. Updates
   * only when the stored lease's `.lease_id` matches the caller's, preserving
   * the lease TTL. A mismatch (or absent/expired lease) is `attached:false` —
   * NOT an error — so a best-effort spawn report stays idempotent. Observers
   * (e.g. actor_status) surface the pid when present.
   */
  async attachSpawn(
    input: WakeLeaseAttachSpawnInput
  ): Promise<WakeLeaseAttachSpawnResult> {
    const result = (await this.redis.eval(
      this.attachSpawnScript,
      1,
      WAKE_LEASE_KEYS.lease(input.actor_id),
      input.lease_id,
      input.pid,
      input.spawned_at
    )) as number;
    return { ok: true, attached: result === 1 };
  }

  private parseLease(raw: string): WakeLease | null {
    try {
      const parsed: unknown = JSON.parse(raw);
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        LEASE_STRING_FIELDS.every(
          (field) => typeof (parsed as Record<string, unknown>)[field] === "string"
        )
      ) {
        return Object.freeze(parsed as WakeLease);
      }
    } catch {
      // fall through to null below
    }
    return null;
  }
}

/** Every field a stored wake lease must carry as a string. */
const LEASE_STRING_FIELDS = ["lease_id", "actor_id", "issued_by_session", "issued_at", "expires_at"] as const;

const fail = (
  code: WakeLeaseError["code"],
  message: string
): WakeLeaseAcquireResult =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });