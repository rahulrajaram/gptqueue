import type { OccupancySignalName } from "./occupancy-guard.js";

/**
 * Classified refusals of a mailbox adoption (runtime adoptIdentity) or an
 * operator continuity apply (applyContinuity). A refusal is a guard result:
 * nothing was written. Redis, transport or protocol failures are NEVER a
 * refusal; `parseGuardReply` throws them as plain errors instead.
 */
export type AdoptionRefusalReason =
  | "mapping_changed" | "registration_missing" | "source_session_invalid"
  | "source_ownership_conflict" | "target_occupied" | "source_busy"
  /** Operator path only: a registry entry no longer matches the plan's fingerprint. */
  | "registration_changed";

/** Runtime -6: why the provisional source is not quiet. */
export type SourceBusySignal = "claims_index" | "outbound_activity" | "wrapper_claim";

/**
 * The first blocking check (other checks may also block):
 * - target_occupied: an occupancy signal of the target;
 * - source_busy: a source signal (operator path also reports an occupancy
 *   signal of the source, or `queued_messages`);
 * - registration_changed: which side's registry entry changed.
 */
export type AdoptionRefusalSignal =
  | OccupancySignalName | SourceBusySignal | "queued_messages"
  | "target_registration" | "source_registration";

export type AdoptionRefusal = Readonly<{
  code: number;
  reason: AdoptionRefusalReason;
  signal: AdoptionRefusalSignal | null;
  /** Redis ms when a recheck may be worthwhile: a hint, never a promise. */
  recheckAfterMs: number | null;
}>;

/** Runtime path (adoptIdentity) Lua return codes. */
export const RUNTIME_ADOPTION_REFUSALS: ReadonlyMap<number, AdoptionRefusalReason> = new Map([
  [-1, "mapping_changed"], [-2, "registration_missing"], [-3, "source_session_invalid"],
  [-4, "source_ownership_conflict"], [-5, "target_occupied"], [-6, "source_busy"],
]);

/** Operator path (applyContinuity) Lua return codes; unchanged numbering. */
export const OPERATOR_CONTINUITY_REFUSALS: ReadonlyMap<number, AdoptionRefusalReason> = new Map([
  [-1, "mapping_changed"], [-2, "registration_changed"], [-3, "registration_changed"],
  [-4, "target_occupied"], [-5, "source_busy"],
]);

const SIGNALS: ReadonlySet<string> = new Set<AdoptionRefusalSignal>([
  "runtime_binding", "heartbeat", "wrapper_claim", "live_session_lease", "claims_index",
  "outbound_activity", "queued_messages", "target_registration", "source_registration",
]);

const isSignal = (value: unknown): value is AdoptionRefusalSignal => typeof value === "string" && SIGNALS.has(value);

/**
 * Parse a guard script reply: `1` is success (null); a known code, bare or
 * as `[code, signal, hintMs]`, is a refusal. Any other reply is an
 * infrastructure or protocol fault and throws a plain Error (never a refusal).
 */
export const parseGuardReply = (
  reply: unknown, codes: ReadonlyMap<number, AdoptionRefusalReason>,
): AdoptionRefusal | null => {
  if (reply === 1) return null;
  const [code, signal, hint] = Array.isArray(reply) ? reply : [reply, null, null];
  const reason = typeof code === "number" ? codes.get(code) : undefined;
  if (reason === undefined || (signal !== null && signal !== undefined && !isSignal(signal))) {
    throw new Error(`Unexpected adoption guard reply: ${JSON.stringify(reply)}`);
  }
  return Object.freeze({ code: code as number, reason, signal: isSignal(signal) ? signal : null,
    recheckAfterMs: typeof hint === "number" && Number.isFinite(hint) && hint > 0 ? hint : null });
};

/** A refused guard, carrying its classification. */
export class AdoptionRefusedError extends Error {
  constructor(prefix: string, readonly refusal: AdoptionRefusal) {
    super(`${prefix} (${refusal.code}): ${refusal.reason}${refusal.signal ? `, first blocker ${refusal.signal}` : ""}`);
    this.name = "AdoptionRefusedError";
  }
}

/**
 * Whether a refusal needs an operator. Occupancy and source activity can
 * clear on their own (claims expire, a dead owner's signals lapse); the rest
 * mean the plan, the registry or this session's ownership no longer hold.
 */
export const operatorActionRequired = (reason: AdoptionRefusalReason): boolean => {
  switch (reason) {
    case "target_occupied":
    case "source_busy":
      return false;
    case "mapping_changed":
    case "registration_missing":
    case "source_session_invalid":
    case "source_ownership_conflict":
    case "registration_changed":
      return true;
    default: {
      const unreachable: never = reason;
      throw new Error(`Unhandled adoption refusal reason: ${String(unreachable)}`);
    }
  }
};
