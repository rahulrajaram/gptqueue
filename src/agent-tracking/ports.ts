/**
 * Ports: the only ways the seat service reaches the outside world. A host
 * (GPTQueue, or a future standalone package) supplies the adapters.
 */
import type { SeatError, SeatTenure, TenureHistoryEntry } from "./types.js";

/** A stored tenure plus the store revision used for compare-and-set. */
export interface StoredTenure {
  readonly tenure: SeatTenure;
  /** 0 means "no record yet"; every commit increments it by one. */
  readonly rev: number;
}

export type StoreRead =
  | Readonly<{ ok: true; stored: StoredTenure | null }>
  | Readonly<{ ok: false; error: SeatError }>;

export type StoreCommit =
  | Readonly<{ ok: true; committed: boolean }> // false: the revision moved; re-read and retry
  | Readonly<{ ok: false; error: SeatError }>;

export type StoreHistory =
  | Readonly<{ ok: true; entries: readonly TenureHistoryEntry[] }>
  | Readonly<{ ok: false; error: SeatError }>;

/**
 * Durable seat state. `commit` is atomic: it writes the tenure at
 * `expected_rev + 1` and appends the history entries only when the stored
 * revision still equals `expected_rev` (0 = absent). History is append-only;
 * the port offers no way to edit or remove an entry.
 */
export interface SeatStore {
  read(seat_id: string): Promise<StoreRead>;
  commit(
    seat_id: string,
    expected_rev: number,
    tenure: SeatTenure,
    entries: readonly TenureHistoryEntry[]
  ): Promise<StoreCommit>;
  history(seat_id: string): Promise<StoreHistory>;
}

export interface Clock {
  now(): Date;
}

/** What a host can observe about one local pid. */
export type ProcessObservation =
  | Readonly<{ state: "running"; start: string }>
  | Readonly<{ state: "absent" }>
  | Readonly<{ state: "unreadable" }>;

/** Where the caller runs: compared with a holder's identity to decide what can be proven from here. */
export type LocalIdentity = Readonly<{ host: string; machine_id: string | null; boot_id: string | null; pidns: string | null }>;

export interface ProcessProbe {
  local(): LocalIdentity;
  observe(pid: number): ProcessObservation;
}
