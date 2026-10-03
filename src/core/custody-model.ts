/**
 * Pure reference model for worktree custody: how an agent shell acquires a
 * worktree, records a structured handoff, loses it on lease expiry, and how a
 * successor takes over a forfeited worktree. It returns immutable records and
 * decision results; it performs no I/O, no Redis, no process, no git, and no
 * network work.
 *
 * Every timestamp and every fingerprint arrives as an argument; the model
 * never reads the clock, the filesystem, git, or an external store. Records
 * are copied on transition, never mutated in place.
 */

export type CustodyState = "unowned" | "held" | "released" | "forfeited";

export type CustodyAcquisitionMode =
  | "initial" // claimed from unowned, no predecessor handoff
  | "graceful_handoff" // claimed from released, predecessor left a handoff record
  | "successor_takeover"; // claimed from forfeited, successor reconstructed state

export interface CustodianIdentity {
  readonly actor_name: string; // non-empty, e.g. "GPTQueue agent"
  readonly session_id: string; // non-empty
}

export interface WorktreeIdentity {
  readonly worktree_path: string; // non-empty absolute-looking path string
  readonly repo_head: string; // non-empty commit sha string
  readonly tree_fingerprint: string; // non-empty opaque digest of git status, SUPPLIED BY CALLER
}

export interface HandoffRecordV1 {
  readonly schema_version: 1;
  readonly authored_by: "origin" | "successor_reconstructed";
  readonly repo_head: string; // non-empty
  readonly tracked_tree_state: "clean" | "dirty";
  readonly untracked_inventory: readonly string[]; // every entry non-empty
  readonly unfinished_work: string; // non-empty human note
  readonly hazards: readonly string[];
  readonly next_step: string; // non-empty
}

export interface CustodyRecord {
  readonly state: CustodyState;
  readonly worktree: WorktreeIdentity;
  readonly custodian?: CustodianIdentity; // set iff held
  readonly mode?: CustodyAcquisitionMode; // set iff held
  readonly acquired_at?: string; // ISO timestamp, set iff held
  readonly lease_expires_at?: string; // ISO timestamp, set iff held
  readonly handoff?: HandoffRecordV1; // set iff released (the predecessor's record)
}

export type CustodyEvent =
  | Readonly<{
      type: "claim";
      custodian: CustodianIdentity;
      lease_expires_at: string;
      acquired_at?: string;
    }>
  | Readonly<{
      type: "assume";
      custodian: CustodianIdentity;
      lease_expires_at: string;
      inventory: readonly string[];
      acquired_at?: string;
    }>
  | Readonly<{ type: "release"; handoff: unknown }>
  | Readonly<{ type: "expire"; now: string }>;

export type HandoffAdmission =
  | Readonly<{ ok: true; handoff: HandoffRecordV1 }>
  | Readonly<{
      ok: false;
      error: Readonly<{
        code:
          | "invalid_version"
          | "missing_head"
          | "invalid_tree_state"
          | "dirty_tree_without_inventory"
          | "invalid_inventory"
          | "missing_unfinished_work"
          | "invalid_authored_by"
          | "missing_next_step";
        message: string;
      }>;
    }>;

export type CustodyErrorCode =
  | "invalid_version"
  | "missing_head"
  | "invalid_tree_state"
  | "dirty_tree_without_inventory"
  | "invalid_inventory"
  | "missing_unfinished_work"
  | "invalid_authored_by"
  | "missing_next_step"
  | "already_held"
  | "forfeited_requires_takeover"
  | "invalid_custodian"
  | "takeover_requires_inventory"
  | "takeover_requires_forfeited"
  | "not_held"
  | "invalid_timestamp";

export type CustodyTransitionResult =
  | Readonly<{ ok: true; state: CustodyRecord }>
  | Readonly<{
      ok: false;
      error: Readonly<{ code: CustodyErrorCode; message: string }>;
    }>;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

/** An inventory is empty when absent, null, or an empty array. */
const isEmptyInventory = (inventory: unknown): boolean =>
  inventory === undefined ||
  inventory === null ||
  (Array.isArray(inventory) && inventory.length === 0);

/** Malformed unless it is an array whose every entry is a non-empty string. */
const isInvalidInventory = (inventory: unknown): boolean =>
  !Array.isArray(inventory) ||
  inventory.some(
    (entry) => typeof entry !== "string" || entry.length === 0
  );

/** hazards are optional; when present they must be an array of strings. */
const isStringArray = (value: unknown): boolean =>
  value === undefined ||
  (Array.isArray(value) && value.every((entry) => typeof entry === "string"));

const isValidCustodian = (value: unknown): value is CustodianIdentity =>
  isRecord(value) &&
  typeof value.actor_name === "string" &&
  value.actor_name.length > 0 &&
  typeof value.session_id === "string" &&
  value.session_id.length > 0;

const assertNever = (value: never): never => {
  throw new Error(`Unhandled custody value: ${JSON.stringify(value)}`);
};

const freezeHandoff = (handoff: HandoffRecordV1): HandoffRecordV1 =>
  Object.freeze({
    ...handoff,
    untracked_inventory: Object.freeze([...handoff.untracked_inventory]),
    hazards: Object.freeze([...handoff.hazards]),
  });

const freezeRecord = (record: CustodyRecord): CustodyRecord =>
  Object.freeze({
    ...record,
    worktree: Object.freeze({ ...record.worktree }),
    custodian:
      record.custodian === undefined
        ? undefined
        : Object.freeze({ ...record.custodian }),
    handoff:
      record.handoff === undefined ? undefined : freezeHandoff(record.handoff),
  });

const handoffInvalid = (
  code:
    | "invalid_version"
    | "missing_head"
    | "invalid_tree_state"
    | "dirty_tree_without_inventory"
    | "invalid_inventory"
    | "missing_unfinished_work"
    | "invalid_authored_by"
    | "missing_next_step",
  message: string
): HandoffAdmission =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });

/**
 * Admit and deeply freeze a HandoffRecordV1 from an unknown value. Pure
 * validation only; it performs no filesystem, git, or Redis work.
 */
export const admitHandoffRecord = (input: unknown): HandoffAdmission => {
  if (!isRecord(input) || input.schema_version !== 1) {
    return handoffInvalid(
      "invalid_version",
      "schema_version must be exactly 1"
    );
  }
  if (typeof input.repo_head !== "string" || input.repo_head.length === 0) {
    return handoffInvalid(
      "missing_head",
      "repo_head must be a non-empty commit sha"
    );
  }
  if (input.tracked_tree_state !== "clean" && input.tracked_tree_state !== "dirty") {
    return handoffInvalid(
      "invalid_tree_state",
      "tracked_tree_state must be clean or dirty"
    );
  }
  if (
    input.tracked_tree_state === "dirty" &&
    isEmptyInventory(input.untracked_inventory)
  ) {
    return handoffInvalid(
      "dirty_tree_without_inventory",
      "a dirty tree must include an untracked inventory"
    );
  }
  if (
    isInvalidInventory(input.untracked_inventory) ||
    !isStringArray(input.hazards)
  ) {
    return handoffInvalid(
      "invalid_inventory",
      "untracked_inventory and hazards must be arrays of non-empty strings"
    );
  }
  if (
    typeof input.unfinished_work !== "string" ||
    input.unfinished_work.length === 0
  ) {
    return handoffInvalid(
      "missing_unfinished_work",
      "unfinished_work must be a non-empty note"
    );
  }
  if (
    input.authored_by !== "origin" &&
    input.authored_by !== "successor_reconstructed"
  ) {
    return handoffInvalid(
      "invalid_authored_by",
      "authored_by must be origin or successor_reconstructed"
    );
  }
  if (typeof input.next_step !== "string" || input.next_step.length === 0) {
    return handoffInvalid(
      "missing_next_step",
      "next_step must be a non-empty instruction"
    );
  }
  return Object.freeze({
    ok: true,
    handoff: freezeHandoff({
      schema_version: 1,
      authored_by: input.authored_by as HandoffRecordV1["authored_by"],
      repo_head: input.repo_head,
      tracked_tree_state: input.tracked_tree_state as HandoffRecordV1["tracked_tree_state"],
      untracked_inventory: input.untracked_inventory as readonly string[],
      unfinished_work: input.unfinished_work,
      hazards: input.hazards as readonly string[],
      next_step: input.next_step,
    }),
  });
};

/** Create a fresh unowned custody record for a worktree, deeply frozen. */
export const createCustody = (worktree: WorktreeIdentity): CustodyRecord =>
  freezeRecord({ state: "unowned", worktree });

const invalid = (
  code: CustodyErrorCode,
  message: string
): CustodyTransitionResult =>
  Object.freeze({
    ok: false,
    error: Object.freeze({ code, message }),
  });

const ok = (state: CustodyRecord): CustodyTransitionResult =>
  Object.freeze({ ok: true, state });

/**
 * Reduce one custody event into a new immutable record, or a frozen typed
 * error. The input record is never mutated.
 */
export const transitionCustody = (
  record: CustodyRecord,
  event: CustodyEvent
): CustodyTransitionResult => {
  switch (event.type) {
    case "claim": {
      switch (record.state) {
        case "held":
          return invalid(
            "already_held",
            `worktree is already held by ${record.custodian?.actor_name ?? "an unknown custodian"}`
          );
        case "forfeited":
          return invalid(
            "forfeited_requires_takeover",
            "a forfeited worktree may only be reclaimed via successor takeover"
          );
        case "unowned":
        case "released": {
          const lease = event.lease_expires_at;
          const acquiredAt = event.acquired_at;
          if (
            !isValidCustodian(event.custodian) ||
            typeof lease !== "string" ||
            lease.length === 0 ||
            (acquiredAt !== undefined &&
              (typeof acquiredAt !== "string" || acquiredAt.length === 0))
          ) {
            return invalid(
              "invalid_custodian",
              "claim requires a non-empty custodian identity and lease timestamp"
            );
          }
          return ok(
            freezeRecord({
              ...record,
              state: "held",
              custodian: event.custodian,
              mode:
                record.state === "released"
                  ? "graceful_handoff"
                  : "initial",
              lease_expires_at: lease,
              acquired_at: acquiredAt,
            })
          );
        }
        default:
          return assertNever(record.state);
      }
    }

    case "assume": {
      if (record.state !== "forfeited") {
        return invalid(
          "takeover_requires_forfeited",
          "successor takeover requires a forfeited worktree"
        );
      }
      const lease = event.lease_expires_at;
      const acquiredAt = event.acquired_at;
      if (
        !isValidCustodian(event.custodian) ||
        typeof lease !== "string" ||
        lease.length === 0 ||
        (acquiredAt !== undefined &&
          (typeof acquiredAt !== "string" || acquiredAt.length === 0))
      ) {
        return invalid(
          "invalid_custodian",
          "takeover requires a non-empty custodian identity and lease timestamp"
        );
      }
      if (
        !Array.isArray(event.inventory) ||
        event.inventory.length === 0 ||
        event.inventory.some(
          (entry) => typeof entry !== "string" || entry.length === 0
        )
      ) {
        return invalid(
          "takeover_requires_inventory",
          "successor takeover requires a non-empty inventory of non-empty strings"
        );
      }
      return ok(
        freezeRecord({
          ...record,
          state: "held",
          custodian: event.custodian, // successor only; dead session attribution is never carried forward
          mode: "successor_takeover",
          lease_expires_at: lease,
          acquired_at: acquiredAt,
        })
      );
    }

    case "release": {
      if (record.state !== "held") {
        return invalid("not_held", "cannot release a worktree that is not held");
      }
      const admission = admitHandoffRecord(event.handoff);
      if (!admission.ok) {
        return invalid(admission.error.code, admission.error.message);
      }
      return ok(
        freezeRecord({
          ...record,
          state: "released",
          custodian: undefined,
          mode: undefined,
          acquired_at: undefined,
          lease_expires_at: undefined,
          handoff: admission.handoff,
        })
      );
    }

    case "expire": {
      if (record.state !== "held") {
        return ok(record);
      }
      const lease = record.lease_expires_at;
      if (lease === undefined) {
        return invalid(
          "invalid_timestamp",
          "a held worktree must carry a lease expiry timestamp"
        );
      }
      const nowMs = Date.parse(event.now);
      const leaseMs = Date.parse(lease);
      if (Number.isNaN(nowMs) || Number.isNaN(leaseMs)) {
        return invalid(
          "invalid_timestamp",
          "lease comparison requires parseable ISO timestamps"
        );
      }
      if (nowMs <= leaseMs) {
        return ok(record);
      }
      return ok(
        freezeRecord({
          ...record,
          state: "forfeited",
          custodian: undefined, // a forfeited record never names a dead session as holder
          mode: undefined,
          acquired_at: undefined,
          lease_expires_at: undefined,
        })
      );
    }

    default:
      return assertNever(event);
  }
};
