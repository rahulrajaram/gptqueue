/**
 * Pure tenure state machine: bind, renew, release, reap, and epoch fencing.
 *
 * The holding process (host, PID namespace, pid, start time) is the tenure
 * unit; a new native session id in the same process keeps the tenure. A
 * different process displaces the holder only when the caller supplies
 * same-host proof that the holder exited, or when the holder is unobservable
 * and its lease has run out. A provably live holder is never displaced.
 *
 * No I/O: timestamps and liveness arrive in the event. Records are frozen and
 * never mutated; a no-op returns the input tenure object itself.
 */
import type {
  HolderIdentity,
  Liveness,
  Result,
  SeatErrorCode,
  SeatTenure,
  TenureHistoryEntry,
  VacateReason,
} from "./types.js";

export type TenureEvent =
  | Readonly<{ type: "bind"; holder: HolderIdentity; now: string; lease_expires_at: string; holder_liveness: Liveness }>
  | Readonly<{ type: "renew"; holder: HolderIdentity; epoch: number; now: string; lease_expires_at: string }>
  | Readonly<{ type: "release"; holder: HolderIdentity; epoch: number; now: string }>
  | Readonly<{ type: "reap"; now: string; holder_liveness: Liveness }>;

export type TenureStep = Readonly<{ tenure: SeatTenure; entries: readonly TenureHistoryEntry[] }>;

type HeldTenure = SeatTenure & Readonly<{ state: "held"; holder: HolderIdentity; bound_at: string; lease_expires_at: string }>;

const assertNever = (value: never): never => {
  throw new Error(`Unhandled tenure value: ${JSON.stringify(value)}`);
};

const refuse = (code: SeatErrorCode, message: string): Result<never> =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });

const ok = <T>(value: T): Result<T> => Object.freeze({ ok: true, value });

const step = (tenure: SeatTenure, entries: readonly TenureHistoryEntry[] = []): Result<TenureStep> =>
  ok(Object.freeze({ tenure, entries: Object.freeze(entries.map((entry) => Object.freeze({ ...entry }))) }));

const isTimestamp = (value: unknown): value is string => typeof value === "string" && !Number.isNaN(Date.parse(value));

const isEpoch = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) >= 0;

const isHolder = (value: HolderIdentity): boolean =>
  typeof value === "object" &&
  value !== null &&
  [value.runtime, value.session_id, value.host, value.pid_start].every((s) => typeof s === "string" && s.length > 0) &&
  [value.machine_id, value.boot_id, value.pidns].every((s) => s === null || (typeof s === "string" && s.length > 0)) &&
  Number.isSafeInteger(value.pid) &&
  value.pid > 0;

const isHeld = (tenure: SeatTenure): tenure is HeldTenure =>
  tenure.state === "held" && tenure.epoch >= 1 && tenure.holder !== undefined &&
  isTimestamp(tenure.bound_at) && isTimestamp(tenure.lease_expires_at);

export const vacantTenure = (seat_id: string): SeatTenure => Object.freeze({ seat_id, state: "vacant", epoch: 0 });

export const sameProcess = (a: HolderIdentity, b: HolderIdentity): boolean =>
  a.host === b.host && a.machine_id === b.machine_id && a.boot_id === b.boot_id &&
  a.pidns === b.pidns && a.pid === b.pid && a.pid_start === b.pid_start;

const held = (seat_id: string, epoch: number, holder: HolderIdentity, bound_at: string, lease_expires_at: string): SeatTenure =>
  Object.freeze({ seat_id, state: "held", epoch, holder: Object.freeze({ ...holder }), bound_at, lease_expires_at });

const vacated = (tenure: SeatTenure): SeatTenure => Object.freeze({ seat_id: tenure.seat_id, state: "vacant", epoch: tenure.epoch });

const bindEntry = (seat_id: string, epoch: number, reason: "vacant" | "takeover", holder: HolderIdentity, at: string): TenureHistoryEntry =>
  ({ kind: "bind", seat_id, epoch, reason, runtime: holder.runtime, session_id: holder.session_id, at });

const vacateEntry = (tenure: HeldTenure, reason: VacateReason, at: string): TenureHistoryEntry =>
  ({ kind: "vacate", seat_id: tenure.seat_id, epoch: tenure.epoch, reason, runtime: tenure.holder.runtime, session_id: tenure.holder.session_id, at });

/** Why the current holder may be displaced now, or null when it may not. */
const displacement = (tenure: HeldTenure, liveness: Liveness, now: string): VacateReason | null => {
  switch (liveness) {
    case "exited":
      return "holder_exit_proven";
    case "alive":
      return null; // proven alive beats any timer
    case "unknown":
      return Date.parse(now) > Date.parse(tenure.lease_expires_at) ? "lease_expired" : null;
    default:
      return assertNever(liveness);
  }
};

const heldMessage = (tenure: HeldTenure, liveness: Liveness): string => {
  const who = `seat '${tenure.seat_id}' is held at epoch ${tenure.epoch} by a ${tenure.holder.runtime} process on ${tenure.holder.host}`;
  switch (liveness) {
    case "alive":
      return `${who}, which is running`;
    case "unknown":
      return `${who}, whose liveness is not observable from here; it stays held until its lease expires at ${tenure.lease_expires_at}`;
    case "exited":
      return who; // unreachable in practice: an exited holder is displaced
    default:
      return assertNever(liveness);
  }
};

/** AT4: only the current tenure's epoch may write. */
export const checkFence = (tenure: SeatTenure, epoch: number): Result<SeatTenure> => {
  if (!isEpoch(epoch)) return refuse("invalid_request", "epoch must be a non-negative integer");
  return tenure.state === "held" && tenure.epoch === epoch
    ? ok(tenure)
    : refuse(
        "stale_epoch",
        `epoch ${epoch} is not the current tenure of seat '${tenure.seat_id}' (it is ${tenure.state} at epoch ${tenure.epoch})`
      );
};

/** The current holder's tenure, fenced by epoch and by process identity. */
const fencedHolder = (tenure: SeatTenure, holder: HolderIdentity, epoch: number): Result<HeldTenure> => {
  const fence = checkFence(tenure, epoch);
  if (!fence.ok) return fence;
  if (!isHeld(tenure)) return refuse("store_corrupt", `held seat '${tenure.seat_id}' is missing its holder or timestamps`);
  return sameProcess(tenure.holder, holder)
    ? ok(tenure)
    : refuse("not_holder", `the caller is not the process holding seat '${tenure.seat_id}' at epoch ${epoch}`);
};

const bind = (tenure: SeatTenure, event: Extract<TenureEvent, { type: "bind" }>): Result<TenureStep> => {
  const { holder, now, lease_expires_at } = event;
  switch (tenure.state) {
    case "vacant": {
      const epoch = tenure.epoch + 1;
      return step(held(tenure.seat_id, epoch, holder, now, lease_expires_at), [bindEntry(tenure.seat_id, epoch, "vacant", holder, now)]);
    }
    case "held": {
      if (!isHeld(tenure)) return refuse("store_corrupt", `held seat '${tenure.seat_id}' is missing its holder or timestamps`);
      if (sameProcess(tenure.holder, holder)) {
        const next = held(tenure.seat_id, tenure.epoch, holder, tenure.bound_at, lease_expires_at);
        return tenure.holder.session_id === holder.session_id
          ? step(next)
          : step(next, [{
              kind: "rebind", seat_id: tenure.seat_id, epoch: tenure.epoch, runtime: holder.runtime,
              session_id: holder.session_id, previous_session_id: tenure.holder.session_id, at: now,
            }]);
      }
      const reason = displacement(tenure, event.holder_liveness, now);
      if (reason === null) return refuse("seat_held", heldMessage(tenure, event.holder_liveness));
      const epoch = tenure.epoch + 1;
      return step(held(tenure.seat_id, epoch, holder, now, lease_expires_at), [
        vacateEntry(tenure, reason, now),
        bindEntry(tenure.seat_id, epoch, "takeover", holder, now),
      ]);
    }
    default:
      return assertNever(tenure.state);
  }
};

const validEvent = (event: TenureEvent): boolean => {
  switch (event.type) {
    case "bind":
      return isHolder(event.holder) && isTimestamp(event.now) && isTimestamp(event.lease_expires_at);
    case "renew":
      return isHolder(event.holder) && isEpoch(event.epoch) && isTimestamp(event.now) && isTimestamp(event.lease_expires_at);
    case "release":
      return isHolder(event.holder) && isEpoch(event.epoch) && isTimestamp(event.now);
    case "reap":
      return isTimestamp(event.now);
    default:
      return assertNever(event);
  }
};

/** Reduce one event into the next tenure plus the history entries it appends. */
export const transitionTenure = (tenure: SeatTenure, event: TenureEvent): Result<TenureStep> => {
  if (!validEvent(event)) return refuse("invalid_request", `malformed ${event.type} request`);
  switch (event.type) {
    case "bind":
      return bind(tenure, event);
    case "renew": {
      const current = fencedHolder(tenure, event.holder, event.epoch);
      if (!current.ok) return current;
      const t = current.value;
      return step(held(t.seat_id, t.epoch, t.holder, t.bound_at, event.lease_expires_at));
    }
    case "release": {
      const current = fencedHolder(tenure, event.holder, event.epoch);
      if (!current.ok) return current;
      return step(vacated(current.value), [vacateEntry(current.value, "released", event.now)]);
    }
    case "reap": {
      if (tenure.state === "vacant") return step(tenure);
      if (!isHeld(tenure)) return refuse("store_corrupt", `held seat '${tenure.seat_id}' is missing its holder or timestamps`);
      const reason = displacement(tenure, event.holder_liveness, event.now);
      return reason === null ? step(tenure) : step(vacated(tenure), [vacateEntry(tenure, reason, event.now)]);
    }
    default:
      return assertNever(event);
  }
};
