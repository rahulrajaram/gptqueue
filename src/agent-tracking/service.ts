/**
 * Seat service: composes the pure tenure model with the store, clock and
 * process-probe ports. Every mutation is read -> transition -> compare-and-set,
 * retried on a lost race, so concurrent binders resolve to exactly one holder.
 */
import { judgeLiveness } from "./liveness.js";
import type { Clock, ProcessProbe, SeatStore, StoredTenure } from "./ports.js";
import { checkFence, transitionTenure, vacantTenure, type TenureEvent, type TenureStep } from "./tenure.js";
import type { HolderIdentity, Liveness, Result, SeatErrorCode, SeatTenure, TenureHistoryEntry } from "./types.js";

/** Same-host liveness is the primary signal, so the lease only bounds unobservable holders. */
export const DEFAULT_LEASE_SECONDS = 6 * 60 * 60;
export const DEFAULT_MAX_ATTEMPTS = 4;
const SEAT_ID = /^[A-Za-z0-9._-]{1,64}$/;

export interface SeatServiceOptions {
  readonly store: SeatStore;
  readonly clock: Clock;
  readonly probe: ProcessProbe;
  readonly lease_seconds?: number;
  readonly max_attempts?: number;
}

export interface SeatService {
  /** Hold the seat: first bind, idempotent same-process rebind, or takeover from a proven-gone holder. */
  bind(seat_id: string, holder: HolderIdentity): Promise<Result<TenureStep>>;
  renew(seat_id: string, holder: HolderIdentity, epoch: number): Promise<Result<SeatTenure>>;
  release(seat_id: string, holder: HolderIdentity, epoch: number): Promise<Result<SeatTenure>>;
  /** Vacate a holder proven gone (or unobservable past its lease); a live holder is left alone. */
  reap(seat_id: string): Promise<Result<SeatTenure>>;
  status(seat_id: string): Promise<Result<SeatTenure>>;
  history(seat_id: string): Promise<Result<readonly TenureHistoryEntry[]>>;
  /** AT4: a consumer checks its epoch before any seat-scoped write. */
  fence(seat_id: string, epoch: number): Promise<Result<SeatTenure>>;
}

const refuse = (code: SeatErrorCode, message: string): Result<never> =>
  Object.freeze({ ok: false, error: Object.freeze({ code, message }) });

const ok = <T>(value: T): Result<T> => Object.freeze({ ok: true, value });

/** Run a store call, turning a thrown transport error into store_unavailable. */
const guarded = async <T>(call: () => Promise<Result<T>>): Promise<Result<T>> => {
  try {
    return await call();
  } catch (error) {
    return refuse("store_unavailable", `seat store unavailable (${(error as Error).message})`);
  }
};

export const createSeatService = (options: SeatServiceOptions): SeatService => {
  const leaseMs = (options.lease_seconds ?? DEFAULT_LEASE_SECONDS) * 1000;
  const attempts = options.max_attempts ?? DEFAULT_MAX_ATTEMPTS;
  const now = (): string => options.clock.now().toISOString();
  const leaseFrom = (at: string): string => new Date(Date.parse(at) + leaseMs).toISOString();
  const livenessOf = (tenure: SeatTenure): Liveness =>
    tenure.holder === undefined ? "unknown" : judgeLiveness(tenure.holder, options.probe);

  const load = (seat_id: string): Promise<Result<StoredTenure>> =>
    guarded(async () => {
      if (!SEAT_ID.test(seat_id)) return refuse("invalid_request", `seat id must match ${SEAT_ID.source}`);
      const read = await options.store.read(seat_id);
      if (!read.ok) return read;
      return ok(read.stored ?? { tenure: vacantTenure(seat_id), rev: 0 });
    });

  /** read -> transition -> compare-and-set; a lost race re-reads and re-decides from fresh state. */
  const mutate = async (seat_id: string, eventFor: (tenure: SeatTenure) => TenureEvent): Promise<Result<TenureStep>> => {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const loaded = await load(seat_id);
      if (!loaded.ok) return loaded;
      const { tenure, rev } = loaded.value;
      const next = transitionTenure(tenure, eventFor(tenure));
      if (!next.ok || next.value.tenure === tenure) return next; // refused, or a no-op with nothing to write
      const committed = await guarded(async () => {
        const result = await options.store.commit(seat_id, rev, next.value.tenure, next.value.entries);
        return result.ok ? ok(result.committed) : result;
      });
      if (!committed.ok) return committed;
      if (committed.value) return next;
    }
    return refuse("contention", `seat '${seat_id}' kept changing during ${attempts} attempts; retry`);
  };

  const tenureOnly = async (pending: Promise<Result<TenureStep>>): Promise<Result<SeatTenure>> => {
    const result = await pending;
    return result.ok ? ok(result.value.tenure) : result;
  };

  return Object.freeze({
    bind: (seat_id: string, holder: HolderIdentity) =>
      mutate(seat_id, (tenure) => {
        const at = now();
        return { type: "bind", holder, now: at, lease_expires_at: leaseFrom(at), holder_liveness: livenessOf(tenure) };
      }),
    renew: (seat_id: string, holder: HolderIdentity, epoch: number) =>
      tenureOnly(mutate(seat_id, () => {
        const at = now();
        return { type: "renew", holder, epoch, now: at, lease_expires_at: leaseFrom(at) };
      })),
    release: (seat_id: string, holder: HolderIdentity, epoch: number) =>
      tenureOnly(mutate(seat_id, () => ({ type: "release", holder, epoch, now: now() }))),
    reap: (seat_id: string) =>
      tenureOnly(mutate(seat_id, (tenure) => ({ type: "reap", now: now(), holder_liveness: livenessOf(tenure) }))),
    status: async (seat_id: string) => {
      const loaded = await load(seat_id);
      return loaded.ok ? ok(loaded.value.tenure) : loaded;
    },
    history: (seat_id: string) =>
      guarded(async () => {
        if (!SEAT_ID.test(seat_id)) return refuse("invalid_request", `seat id must match ${SEAT_ID.source}`);
        const read = await options.store.history(seat_id);
        return read.ok ? ok(read.entries) : read;
      }),
    fence: async (seat_id: string, epoch: number) => {
      const loaded = await load(seat_id);
      return loaded.ok ? checkFence(loaded.value.tenure, epoch) : loaded;
    },
  });
};
