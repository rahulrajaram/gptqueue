/**
 * CustodyStore: Redis-backed persistence for worktree custody records.
 *
 * Wraps the pure custody model (src/core/custody-model.ts) with a durable
 * store. This is the ONLY component that serializes/deserializes stored
 * custody records. All mutations run through a thin conditional Lua script so
 * a transition only persists when the stored `.state` (and, for release, the
 * stored custodian `.session_id`) still matches the precondition read earlier;
 * full custody semantics live in the model and here.
 *
 * Mirrors SessionStore's structure: an injected ioredis client and typed
 * ok/error results. Domain failures never throw.
 */

import { Redis } from "ioredis";
import { z } from "zod";
import { readFileSync } from "fs";
import { join } from "path";
import { CUSTODY_KEYS } from "./keys.js";
import {
  admitHandoffRecord,
  createCustody,
  transitionCustody,
  type CustodianIdentity,
  type CustodyErrorCode,
  type CustodyEvent,
  type CustodyRecord,
  type WorktreeIdentity,
} from "./custody-model.js";
import { LUA_DIR, describeStored, type StoredRead as StoredReadOf } from "./stored-read.js";


/** Sentinel passed to the Lua script for a field that is expected to be absent. */
const ABSENT = "ABSENT";

export type CustodyStoreErrorCode =
  | CustodyErrorCode
  | "not_custodian"
  | "store_corrupt";

export type CustodyStoreError = Readonly<{
  code: CustodyStoreErrorCode;
  message: string;
}>;

export type CustodyOpResult =
  | Readonly<{ ok: true; record: CustodyRecord }>
  | Readonly<{ ok: false; error: CustodyStoreError }>;

export type CustodyClaimResult = CustodyOpResult;

export type CustodyReleaseResult = CustodyOpResult;

export type CustodyStatusResult =
  | Readonly<{ ok: true; record: CustodyRecord | null }>
  | Readonly<{ ok: false; error: CustodyStoreError }>;

export type CustodyListResult =
  | Readonly<{ ok: true; records: CustodyRecord[] }>
  | Readonly<{ ok: false; error: CustodyStoreError }>;

export interface CustodyClaimInput {
  readonly worktree_path: string;
  readonly repo_head: string;
  readonly tree_fingerprint: string;
  readonly lease_seconds: number;
  readonly inventory?: readonly string[];
  readonly actor_name: string;
  readonly session_id: string;
  readonly now: string; // ISO timestamp; the adapter layer reads the clock, not the core
}

export interface CustodyReleaseInput {
  readonly worktree_path: string;
  readonly actor_name: string;
  readonly session_id: string;
  readonly handoff: unknown;
  // ISO timestamp; the adapter layer reads the clock, not the core. Used to
  // self-heal an expired hold before applying release preconditions.
  readonly now: string;
}

export interface CustodyStatusInput {
  readonly worktree_path?: string;
  readonly now: string;
}

/** An inventory is a non-empty array of non-empty strings. */
const isValidInventory = (inventory: unknown): boolean =>
  Array.isArray(inventory) &&
  inventory.length > 0 &&
  inventory.every(
    (entry) => typeof entry === "string" && entry.length > 0
  );

type StoredRead = StoredReadOf<CustodyRecord>;

/**
 * A stored handoff must pass the same admission as on release. Transitions
 * re-freeze it by spreading both arrays, and admission alone tolerates an
 * absent hazards list, so that list is required here.
 */
const isStoredHandoff = (handoff: unknown): boolean =>
  typeof handoff === "object" &&
  handoff !== null &&
  Array.isArray((handoff as { hazards?: unknown }).hazards) &&
  admitHandoffRecord(handoff).ok;

/**
 * Read admission for a stored custody record: every field the domain code
 * dereferences must be present, so a schema-incomplete value is reported as
 * store_corrupt instead of throwing later. Extra fields are tolerated. A held
 * record names its custodian and lease; a released record carries the
 * handoff it was released with, and any record carrying a handoff (held and
 * forfeited records keep their predecessor's) carries a well-formed one.
 */
const storedCustodySchema = z
  .object({
    state: z.enum(["unowned", "held", "released", "forfeited"]),
    worktree: z.object({
      worktree_path: z.string().min(1),
      repo_head: z.string(),
      tree_fingerprint: z.string(),
    }).passthrough(),
    custodian: z.object({ actor_name: z.string(), session_id: z.string() }).passthrough().optional(),
    lease_expires_at: z.string().optional(),
    handoff: z.unknown().optional(),
  })
  .passthrough()
  .refine((record) => record.state !== "held" || record.custodian !== undefined, { path: ["custodian"] })
  .refine((record) => record.state !== "held" || record.lease_expires_at !== undefined, { path: ["lease_expires_at"] })
  .refine((record) => record.state !== "released" || record.handoff !== undefined, { path: ["handoff"] })
  .refine((record) => record.handoff === undefined || isStoredHandoff(record.handoff), { path: ["handoff"] });

export class CustodyStore {
  private readonly redis: Redis;
  private readonly conditionalSetScript: string;

  constructor(redis: Redis) {
    this.redis = redis;
    this.conditionalSetScript = readFileSync(
      join(LUA_DIR, "custody-conditional-set.lua"),
      "utf-8"
    );
  }

  /**
   * Claim a worktree for a custodian session. Handles the initial claim
   * (unowned), graceful re-claim (released) via a claim transition, and a
   * successor takeover (forfeited) via an assume transition requiring a
   * non-empty inventory.
   */
  async claim(input: CustodyClaimInput): Promise<CustodyClaimResult> {
    const leaseExpiresAt = new Date(
      Date.parse(input.now) + input.lease_seconds * 1000
    ).toISOString();
    const worktree: WorktreeIdentity = {
      worktree_path: input.worktree_path,
      repo_head: input.repo_head,
      tree_fingerprint: input.tree_fingerprint,
    };
    const custodian: CustodianIdentity = {
      actor_name: input.actor_name,
      session_id: input.session_id,
    };
    const hasTakeoverInventory = isValidInventory(input.inventory);

    // Bounded retry: on a precondition race the loop re-reads and retries once
    // before falling through to a derived domain error.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const stored = await this.readRecord(input.worktree_path);
      if (stored.kind === "corrupt") return this.corrupt(stored.diagnostic);

      let record = stored.kind === "record" ? stored.record : null;
      if (record === null) {
        // Fresh worktree: seed the custody record then claim it as "initial".
        const seeded = createCustody(worktree);
        const transition = transitionCustody(seeded, {
          type: "claim",
          custodian,
          lease_expires_at: leaseExpiresAt,
          acquired_at: input.now,
        });
        if (!transition.ok) return transition;
        const persisted = await this.conditionalSet(
          input.worktree_path,
          JSON.stringify(transition.state),
          ABSENT
        );
        if (persisted) return { ok: true, record: transition.state };
        // Race: another writer created a record; loop re-reads and retries.
        continue;
      }

      // Self-heal (M4): an expired-but-held lease is lazily forfeited BEFORE
      // any claim precondition runs, so a successor claiming an expired hold
      // reaches the takeover path instead of a dead-end `already_held`. The
      // forfeited state is persisted conditionally (precondition "held") and
      // the returned (forfeited) record drives the transition below.
      record = await this.lazyExpire(record, input.now);

      if (record.state === "held") {
        return fail(
          "already_held",
          `worktree is already held by ${record.custodian?.actor_name ?? "an unknown custodian"}`
        );
      }

      let event: CustodyEvent;
      if (record.state === "forfeited") {
        if (!hasTakeoverInventory) {
          return fail(
            "takeover_requires_inventory",
            "a forfeited worktree may only be reclaimed via successor takeover, which requires a non-empty inventory of non-empty strings"
          );
        }
        event = {
          type: "assume",
          custodian,
          lease_expires_at: leaseExpiresAt,
          inventory: input.inventory as readonly string[],
          acquired_at: input.now,
        };
      } else {
        // unowned or released
        event = {
          type: "claim",
          custodian,
          lease_expires_at: leaseExpiresAt,
          acquired_at: input.now,
        };
      }

      const transition = transitionCustody(record, event);
      if (!transition.ok) return transition;

      const persisted = await this.conditionalSet(
        input.worktree_path,
        JSON.stringify(transition.state),
        record.state
      );
      if (persisted) return { ok: true, record: transition.state };
      // Precondition failed (a concurrent writer won) -> loop re-reads and retries.
    }

    // Retries exhausted: derive the domain error from the current stored state.
    const stored = await this.readRecord(input.worktree_path);
    if (stored.kind === "corrupt") return this.corrupt(stored.diagnostic);
    const current = stored.kind === "record" ? stored.record : null;
    if (current && current.state === "held") {
      return fail(
        "already_held",
        `worktree is already held by ${current.custodian?.actor_name ?? "an unknown custodian"}`
      );
    }
    if (current && current.state === "forfeited") {
      return fail(
        "forfeited_requires_takeover",
        "a forfeited worktree may only be reclaimed via successor takeover"
      );
    }
    return fail(
      "already_held",
      "worktree custody changed concurrently; re-read the current state and retry"
    );
  }

  /**
   * Release a held worktree, storing the predecessor's structured handoff and
   * clearing the custodian. Only the current custodian session may release.
   */
  async release(input: CustodyReleaseInput): Promise<CustodyReleaseResult> {
    const admission = admitHandoffRecord(input.handoff);
    if (!admission.ok) {
      return fail(admission.error.code, admission.error.message);
    }

    for (let attempt = 0; attempt < 2; attempt += 1) {
      const stored = await this.readRecord(input.worktree_path);
      if (stored.kind === "corrupt") return this.corrupt(stored.diagnostic);
      let record = stored.kind === "record" ? stored.record : null;
      if (record !== null) {
        // Self-heal (M4): a release of an EXPIRED hold is not a success on a
        // stale hold. The lapsed lease is forfeited first, so the release
        // resolves to a typed `not_held` outcome (the worktree is no longer
        // held by anyone) rather than silently releasing a dead custodian.
        record = await this.lazyExpire(record, input.now);
      }

      if (!record || record.state !== "held") {
        return fail("not_held", "cannot release a worktree that is not held");
      }
      if (!record.custodian || record.custodian.session_id !== input.session_id) {
        return fail(
          "not_custodian",
          "only the current custodian session may release this worktree"
        );
      }

      const transition = transitionCustody(record, {
        type: "release",
        handoff: admission.handoff,
      });
      if (!transition.ok) return transition;

      const persisted = await this.conditionalSet(
        input.worktree_path,
        JSON.stringify(transition.state),
        "held",
        input.session_id
      );
      if (persisted) return { ok: true, record: transition.state };
    }

    // Retries exhausted: derive the domain error from the current stored state.
    const stored = await this.readRecord(input.worktree_path);
    if (stored.kind === "corrupt") return this.corrupt(stored.diagnostic);
    const current = stored.kind === "record" ? stored.record : null;
    if (!current || current.state !== "held") {
      return fail("not_held", "cannot release a worktree that is not held");
    }
    if (!current.custodian || current.custodian.session_id !== input.session_id) {
      return fail(
        "not_custodian",
        "only the current custodian session may release this worktree"
      );
    }
    return fail(
      "not_held",
      "worktree custody changed concurrently; re-read the current state and retry"
    );
  }

  /**
   * Read one record, or list every record, lazily forfeiting any held lease
   * whose expiry has passed (the forfeited result is persisted idempotently).
   */
  async status(
    input: CustodyStatusInput
  ): Promise<CustodyStatusResult | CustodyListResult> {
    if (input.worktree_path !== undefined) {
      const stored = await this.readRecord(input.worktree_path);
      if (stored.kind === "corrupt") return this.corrupt(stored.diagnostic);
      const record = stored.kind === "record" ? stored.record : null;
      if (record === null) return { ok: true, record: null };
      return { ok: true, record: await this.lazyExpire(record, input.now) };
    }

    const all = await this.redis.hgetall(CUSTODY_KEYS.records);
    const records: CustodyRecord[] = [];
    for (const [path, raw] of Object.entries(all)) {
      const parsed = this.parseStored(raw, path);
      if (parsed.kind === "corrupt") return this.corrupt(parsed.diagnostic);
      // hgetall only yields present fields, so a stored record is never null here.
      if (parsed.record === null) continue;
      records.push(await this.lazyExpire(parsed.record, input.now));
    }
    return { ok: true, records };
  }

  /** Lazily forfeit a held record whose lease has passed, persisting the result. */
  private async lazyExpire(
    record: CustodyRecord,
    now: string
  ): Promise<CustodyRecord> {
    if (
      record.state !== "held" ||
      record.lease_expires_at === undefined ||
      Number.isNaN(Date.parse(record.lease_expires_at)) ||
      Number.isNaN(Date.parse(now)) ||
      Date.parse(now) <= Date.parse(record.lease_expires_at)
    ) {
      return record;
    }
    const result = transitionCustody(record, { type: "expire", now });
    if (result.ok && result.state.state === "forfeited") {
      // Idempotent if a concurrent writer already forfeited: precondition "held".
      await this.conditionalSet(
        record.worktree.worktree_path,
        JSON.stringify(result.state),
        "held"
      );
      return result.state;
    }
    return record;
  }

  private async readRecord(path: string): Promise<StoredRead> {
    const raw = await this.redis.hget(CUSTODY_KEYS.records, path);
    if (raw === null) return { kind: "record", record: null };
    return this.parseStored(raw, path);
  }

  private parseStored(raw: string, path: string): StoredRead {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { kind: "corrupt", diagnostic: `${path}: ${describeStored(raw, "JSON")}` };
    }
    const admission = storedCustodySchema.safeParse(parsed);
    if (admission.success) return { kind: "record", record: parsed as CustodyRecord };
    // The first failing field's path names where, never what, it stored.
    const failing = admission.error.issues[0]?.path.map(String).join(".") || "(root)";
    return { kind: "corrupt", diagnostic: `${path}: ${describeStored(raw, failing)}` };
  }

  private corrupt(diagnostic: string): CustodyOpResult {
    return fail(
      "store_corrupt",
      `stored custody record is corrupt (${diagnostic}); refusing to operate on it`
    );
  }

  /**
   * Run the conditional-set Lua script. Returns true only when the write was
   * successfully persisted under the expected precondition.
   */
  private async conditionalSet(
    path: string,
    newValue: string,
    expectedState: string,
    expectedCustodianSession?: string
  ): Promise<boolean> {
    const result = (await this.redis.eval(
      this.conditionalSetScript,
      1,
      CUSTODY_KEYS.records,
      path,
      newValue,
      expectedState,
      expectedCustodianSession ?? ""
    )) as number;
    return result === 1;
  }
}

const fail = (
  code: CustodyStoreErrorCode,
  message: string
): CustodyOpResult =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });