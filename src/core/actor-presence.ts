/**
 * Pure reference model for durable actor profiles and runtime presence
 * classification. It returns classification results; it performs no Redis,
 * MCP, process, or network work.
 *
 * Two identities are deliberately separate:
 * - A durable actor profile survives process exit and owns activation policy.
 * - A runtime incarnation is one temporary, leased process of that actor.
 *
 * Delivery mode (`store_only` on a queued task) is a per-task property and is
 * intentionally absent here; activation policy is an actor-owned property.
 * Presence classification never inspects queued work.
 */

export type ActivationPolicyMode = "wake_if_offline" | "store_only";

/** Actor-owned policy: who may wake this actor and within which bounds. */
export interface ActivationPolicy {
  readonly mode: ActivationPolicyMode;
}

/**
 * The minimum durable actor profile from the approved operating model.
 * All fields are immutable once admitted.
 */
export interface DurableActorProfile {
  readonly actor_id: string;
  /** Human-facing address such as `metabuilder`. */
  readonly alias: string;
  /** Structured routing claims; opaque descriptive strings in this slice. */
  readonly capabilities: readonly string[];
  readonly workspace_root: string;
  readonly working_directory: string;
  /** Isolated state for one incarnation; created per activation. */
  readonly run_directory?: string;
  /** Approved adapter and launch profile name. */
  readonly runtime: string;
  readonly activation_policy: ActivationPolicy;
  readonly max_concurrency: number;
}

/** One observed runtime incarnation of a durable actor. */
export interface RuntimeIncarnation {
  readonly incarnation_id: string;
  readonly session_id?: string;
  readonly started_at?: string;
  /**
   * The wake lease held by this incarnation. An observation without a lease
   * is not usable presence and is ignored by classification.
   */
  readonly lease_id?: string;
  readonly workload: "processing" | "idle";
}

/** Whether the actor's registered launch contract can currently run. */
export type LaunchContractReadiness = "runnable" | "not_runnable";

export interface PresenceInput {
  readonly actor: DurableActorProfile | undefined;
  readonly launch_contract: LaunchContractReadiness;
  readonly runtime: RuntimeIncarnation | undefined;
  /** A controller-issued wake lease currently outstanding, if any. */
  readonly wake_lease_id?: string;
}

/**
 * Discovery presence states. `active`, `idle`, and `starting` describe a
 * runtime-attached actor; `offline_launchable`, `offline_store_only`, and
 * `unavailable` describe an offline actor.
 */
export type RuntimePresenceState =
  | "active"
  | "idle"
  | "starting"
  | "offline_launchable"
  | "offline_store_only"
  | "unavailable";

export type PresenceClassification =
  | Readonly<{
      ok: true;
      actor_id: string;
      presence: RuntimePresenceState;
    }>
  | Readonly<{
      ok: false;
      error: Readonly<{
        code: "unknown_recipient";
        message: string;
      }>;
    }>;

export type ProfileAdmission =
  | Readonly<{ ok: true; profile: DurableActorProfile }>
  | Readonly<{
      ok: false;
      error: Readonly<{
        code:
          | "invalid_identity"
          | "invalid_identity_charset"
          | "invalid_policy"
          | "invalid_concurrency";
        message: string;
      }>;
    }>;

/**
 * Internal, fully discriminated situation resolved from raw presence inputs.
 * Classification switches over this union exhaustively.
 */
type Situation =
  | Readonly<{ kind: "runtime_leased"; workload: "processing" | "idle" }>
  | Readonly<{ kind: "wake_lease_outstanding" }>
  | Readonly<{
      kind: "offline";
      contract: LaunchContractReadiness;
      policy: ActivationPolicyMode;
    }>;

const assertNever = (value: never): never => {
  throw new Error(`Unhandled situation: ${JSON.stringify(value)}`);
};

/**
 * Exhaustive decision table over situations. Precedence (checked in order by
 * `resolveSituation`):
 * 1. A leased runtime wins: an actor with a live incarnation is not offline.
 * 2. An outstanding wake lease wins: an activation is already in flight.
 * 3. Otherwise the actor is offline; a non-runnable launch contract makes it
 *    `unavailable` regardless of policy, a `store_only` policy makes it
 *    `offline_store_only`, and anything else is `offline_launchable`.
 */
const classifySituation = (situation: Situation): RuntimePresenceState => {
  switch (situation.kind) {
    case "runtime_leased":
      return situation.workload === "processing" ? "active" : "idle";
    case "wake_lease_outstanding":
      return "starting";
    case "offline":
      switch (situation.contract) {
        case "not_runnable":
          return "unavailable";
        case "runnable":
          switch (situation.policy) {
            case "store_only":
              return "offline_store_only";
            case "wake_if_offline":
              return "offline_launchable";
            default:
              return assertNever(situation.policy);
          }
        default:
          return assertNever(situation.contract);
      }
    default:
      return assertNever(situation);
  }
};

const resolveSituation = (
  input: PresenceInput,
  actor: DurableActorProfile
): Situation => {
  if (
    input.runtime !== undefined &&
    input.runtime.lease_id !== undefined
  ) {
    return { kind: "runtime_leased", workload: input.runtime.workload };
  }
  if (input.wake_lease_id !== undefined) {
    return { kind: "wake_lease_outstanding" };
  }
  return {
    kind: "offline",
    contract: input.launch_contract,
    policy: actor.activation_policy.mode,
  };
};

/**
 * Classify one actor's presence from pure inputs. Unknown recipients are
 * rejected without creating any queue, mailbox, or directory data.
 */
export const classifyPresence = (
  input: PresenceInput
): PresenceClassification => {
  const actor = input.actor;
  if (actor === undefined) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "unknown_recipient",
        message:
          "recipient is not a registered durable actor; no mailbox or queue data was created",
      }),
    });
  }
  return Object.freeze({
    ok: true,
    actor_id: actor.actor_id,
    presence: classifySituation(resolveSituation(input, actor)),
  });
};

const isActivationPolicyMode = (value: unknown): value is ActivationPolicyMode =>
  value === "wake_if_offline" || value === "store_only";

/**
 * Identity charset for actor_id/alias (review M2): letters, digits, dot,
 * underscore, hyphen, 1..64 chars. Prevents a caller from embedding path or
 * injection-special characters into a durable identity.
 */
const IDENTITY_CHARSET = /^[A-Za-z0-9._-]{1,64}$/;

/**
 * Admit and deeply freeze a durable actor profile. Pure validation only;
 * it performs no registration, Redis, or filesystem work.
 */
export const admitActorProfile = (
  candidate: DurableActorProfile
): ProfileAdmission => {
  if (candidate.actor_id.length === 0) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_identity",
        message: "actor_id must be a non-empty stable identity",
      }),
    });
  }
  if (candidate.alias.length === 0) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_identity",
        message: "alias must be a non-empty address",
      }),
    });
  }
  if (!IDENTITY_CHARSET.test(candidate.actor_id)) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_identity_charset",
        message:
          "actor_id must match /^[A-Za-z0-9._-]{1,64}$/ (letters, digits, '.', '_', '-'; 1..64 chars)",
      }),
    });
  }
  if (!IDENTITY_CHARSET.test(candidate.alias)) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_identity_charset",
        message:
          "alias must match /^[A-Za-z0-9._-]{1,64}$/ (letters, digits, '.', '_', '-'; 1..64 chars)",
      }),
    });
  }
  if (!isActivationPolicyMode(candidate.activation_policy?.mode)) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_policy",
        message:
          "activation_policy.mode must be wake_if_offline or store_only",
      }),
    });
  }
  if (
    !Number.isInteger(candidate.max_concurrency) ||
    candidate.max_concurrency < 1
  ) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "invalid_concurrency",
        message: "max_concurrency must be an integer of at least 1",
      }),
    });
  }
  return Object.freeze({
    ok: true,
    profile: Object.freeze({
      ...candidate,
      capabilities: Object.freeze([...candidate.capabilities]),
      activation_policy: Object.freeze({ ...candidate.activation_policy }),
    }),
  });
};
