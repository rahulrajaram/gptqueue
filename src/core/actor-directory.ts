/**
 * ActorDirectory: Redis-backed durable actor directory.
 *
 * Wraps the pure actor profile admission (src/core/actor-presence.ts) with a
 * durable directory. This is the ONLY component that serializes/deserializes
 * stored actor directory records. Registration runs through a thin conditional
 * Lua script so a record only persists when the field is absent (first
 * registration) or its stored `.registered_by` session still matches the
 * caller — a durable actor's profile is owned by the session that registered
 * it, and the same session may update it.
 *
 * Mirrors SessionStore/CustodyStore's structure: an injected ioredis client
 * and typed ok/error results. Domain failures never throw.
 */

import { Redis } from "ioredis";
import { readFileSync } from "fs";
import { join } from "path";
import { ACTOR_KEYS } from "./keys.js";
import {
  admitActorProfile,
  type DurableActorProfile,
  type LaunchContractReadiness,
} from "./actor-presence.js";
import { evaluateLaunchPolicy } from "./launch-policy.js";
import { LUA_DIR, excerptOf, type StoredRead as StoredReadOf } from "./stored-read.js";


/**
 * How a runtime is launched for this actor. The command is spawned WITHOUT a
 * shell; args are passed verbatim.
 */
export interface RuntimeLaunchContract {
  readonly command: string;
  readonly args: readonly string[];
  readonly cwd?: string;
}

/** One persisted entry in the durable actor directory. */
export interface ActorDirectoryRecord {
  readonly profile: DurableActorProfile;
  /** Required (non-null) exactly when the policy is `wake_if_offline`. */
  readonly launch: RuntimeLaunchContract | null;
  /** Session id that registered (and therefore owns) this actor profile. */
  readonly registered_by: string;
  /** Caller-supplied ISO timestamp of registration. */
  readonly registered_at: string;
}

export type ActorDirectoryErrorCode =
  | "invalid_identity"
  | "invalid_identity_charset"
  | "invalid_policy"
  | "invalid_concurrency"
  | "invalid_launch_contract"
  | "launch_command_rejected"
  | "launch_not_allowlisted"
  | "launch_cwd_confined"
  | "actor_owned_elsewhere"
  | "store_corrupt";

export type ActorDirectoryError = Readonly<{
  code: ActorDirectoryErrorCode;
  message: string;
}>;

export type ActorDirectoryResult =
  | Readonly<{ ok: true; record: ActorDirectoryRecord }>
  | Readonly<{ ok: false; error: ActorDirectoryError }>;

export type ActorGetResult =
  | Readonly<{ ok: true; record: ActorDirectoryRecord | null }>
  | Readonly<{ ok: false; error: ActorDirectoryError }>;

export type ActorListResult =
  | Readonly<{ ok: true; records: ActorDirectoryRecord[] }>
  | Readonly<{ ok: false; error: ActorDirectoryError }>;

export interface ActorRegisterInput {
  readonly profile_input: unknown;
  readonly launch: RuntimeLaunchContract | null;
  readonly registered_by: string;
  readonly registered_at: string; // ISO timestamp, caller-supplied
}

/** A launch contract is well-formed: non-empty command and string args. */
const isWellFormedLaunch = (launch: RuntimeLaunchContract): boolean =>
  typeof launch.command === "string" &&
  launch.command.length > 0 &&
  Array.isArray(launch.args) &&
  launch.args.every((arg) => typeof arg === "string");

/**
 * A `wake_if_offline` actor must carry a runnable launch contract so it can be
 * activated. `store_only` actors may omit a launch.
 */
const isValidLaunchContract = (
  profile: DurableActorProfile,
  launch: RuntimeLaunchContract | null
): boolean => {
  if (profile.activation_policy.mode !== "wake_if_offline") return true;
  return launch !== null && isWellFormedLaunch(launch);
};

/** Freeze a launch contract (and its args) for publication in a record. */
const freezeLaunch = (
  launch: RuntimeLaunchContract | null
): RuntimeLaunchContract | null =>
  launch === null
    ? null
    : Object.freeze({
        command: launch.command,
        args: Object.freeze([...launch.args]),
        ...(launch.cwd === undefined ? {} : { cwd: launch.cwd }),
      });

type StoredRead = StoredReadOf<ActorDirectoryRecord>;

export class ActorDirectory {
  private readonly redis: Redis;
  private readonly conditionalSetScript: string;

  constructor(redis: Redis) {
    this.redis = redis;
    this.conditionalSetScript = readFileSync(
      join(LUA_DIR, "actor-conditional-set.lua"),
      "utf-8"
    );
  }

  /**
   * Register (or re-register/update) a durable actor profile. Admission runs
   * first and its error codes propagate unchanged. A `wake_if_offline` actor
   * must carry a runnable launch contract, and its launch contract must pass
   * the operator launch policy (allowlist + cwd confinement) before it is
   * persisted — enforcement happens again at dispatch so a stale record
   * cannot spawn what the operator has since disallowed. Ownership is
   * session-scoped: a foreign session that did not register the actor is
   * rejected as `actor_owned_elsewhere`.
   */
  async register(input: ActorRegisterInput): Promise<ActorDirectoryResult> {
    const admission = admitActorProfile(
      input.profile_input as DurableActorProfile
    );
    if (!admission.ok) {
      return fail(admission.error.code, admission.error.message);
    }
    const profile = admission.profile;

    if (!isValidLaunchContract(profile, input.launch)) {
      return fail(
        "invalid_launch_contract",
        "a wake_if_offline actor requires a launch contract with a non-empty command and string[] args"
      );
    }

    // Operator launch policy gates NEW wake_if_offline registrations
    // (fail-closed when the allowlist is absent/unparseable). store_only
    // actors are never dispatched, so their (inert, metadata-only) launch
    // contracts are not policy-evaluated here.
    if (
      profile.activation_policy.mode === "wake_if_offline" &&
      input.launch !== null
    ) {
      const policy = await evaluateLaunchPolicy(input.launch);
      if (!policy.ok) {
        return fail(policy.error.code, policy.error.message);
      }
    }

    const record: ActorDirectoryRecord = Object.freeze({
      profile,
      launch: freezeLaunch(input.launch),
      registered_by: input.registered_by,
      registered_at: input.registered_at,
    });

    // Bounded retry: on a precondition race the loop re-reads and retries once
    // before falling through to a derived domain error.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const stored = await this.readRecord(profile.actor_id);
      if (stored.kind === "corrupt") return this.corrupt(stored.excerpt);
      const existing = stored.kind === "record" ? stored.record : null;

      if (existing !== null && existing.registered_by !== input.registered_by) {
        return fail(
          "actor_owned_elsewhere",
          `actor '${profile.actor_id}' is owned by another session`
        );
      }

      const persisted = await this.conditionalSet(
        profile.actor_id,
        JSON.stringify(record),
        input.registered_by
      );
      if (persisted) return { ok: true, record };
      // Precondition failed (a concurrent foreign writer won) -> re-read and retry.
    }

    // Retries exhausted: derive the domain error from the current stored state.
    const stored = await this.readRecord(profile.actor_id);
    if (stored.kind === "corrupt") return this.corrupt(stored.excerpt);
    const current = stored.kind === "record" ? stored.record : null;
    if (current !== null && current.registered_by !== input.registered_by) {
      return fail(
        "actor_owned_elsewhere",
        `actor '${profile.actor_id}' is owned by another session`
      );
    }
    return fail(
      "actor_owned_elsewhere",
      `actor '${profile.actor_id}' was registered concurrently by another session; re-read the current state and retry`
    );
  }

  /** Read one actor's directory record (null when not registered). */
  async get(actorId: string): Promise<ActorGetResult> {
    const stored = await this.readRecord(actorId);
    if (stored.kind === "corrupt") return this.corrupt(stored.excerpt);
    return {
      ok: true,
      record: stored.kind === "record" ? stored.record : null,
    };
  }

  /** List every actor directory record. */
  async list(): Promise<ActorListResult> {
    const all = await this.redis.hgetall(ACTOR_KEYS.profiles);
    const records: ActorDirectoryRecord[] = [];
    for (const [actorId, raw] of Object.entries(all)) {
      const parsed = this.parseStored(raw, actorId);
      if (parsed.kind === "corrupt") return this.corrupt(parsed.excerpt);
      // hgetall only yields present fields, so a stored record is never null here.
      if (parsed.record === null) continue;
      records.push(parsed.record);
    }
    return { ok: true, records };
  }

  /** Whether a registered actor's launch contract can currently run. */
  contractReadiness(record: ActorDirectoryRecord): LaunchContractReadiness {
    return record.launch !== null &&
      typeof record.launch.command === "string" &&
      record.launch.command.length > 0
      ? "runnable"
      : "not_runnable";
  }

  private async readRecord(actorId: string): Promise<StoredRead> {
    const raw = await this.redis.hget(ACTOR_KEYS.profiles, actorId);
    if (raw === null) return { kind: "record", record: null };
    return this.parseStored(raw, actorId);
  }

  private parseStored(raw: string, actorId: string): StoredRead {
    try {
      const parsed: unknown = JSON.parse(raw);
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        typeof (parsed as { profile?: unknown }).profile === "object" &&
        (parsed as { profile: unknown }).profile !== null &&
        typeof (parsed as { registered_by?: unknown }).registered_by === "string" &&
        // The stored profile must pass the same admission as a new one;
        // a malformed shape may throw inside it, which is caught below.
        admitActorProfile((parsed as { profile: DurableActorProfile }).profile).ok
      ) {
        return { kind: "record", record: parsed as ActorDirectoryRecord };
      }
    } catch {
      // fall through to corrupt below
    }
    return { kind: "corrupt", excerpt: `${actorId}=${excerptOf(raw)}` };
  }

  private corrupt(excerpt: string): Readonly<{
    ok: false;
    error: ActorDirectoryError;
  }> {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "store_corrupt" as const,
        message: `stored actor directory record is corrupt (${excerpt}); refusing to operate on it`,
      }),
    });
  }

  /**
   * Run the actor conditional-set Lua script. Returns true only when the write
   * was successfully persisted (field absent, or the stored `.registered_by`
   * matches the expected owner).
   */
  private async conditionalSet(
    actorId: string,
    newValue: string,
    expectedOwner: string
  ): Promise<boolean> {
    const result = (await this.redis.eval(
      this.conditionalSetScript,
      1,
      ACTOR_KEYS.profiles,
      actorId,
      newValue,
      expectedOwner
    )) as number;
    return result === 1;
  }
}

const fail = (
  code: ActorDirectoryErrorCode,
  message: string
): ActorDirectoryResult =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });