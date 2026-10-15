/** In-memory SeatStore: the reference adapter for the contract suite (AT10). */
import type { SeatStore, StoredTenure } from "./ports.js";
import type { SeatTenure, TenureHistoryEntry } from "./types.js";

/** Copy and freeze, so neither the caller nor a reader can change stored state. */
const freezeTenure = (tenure: SeatTenure): SeatTenure =>
  Object.freeze({ ...tenure, ...(tenure.holder ? { holder: Object.freeze({ ...tenure.holder }) } : {}) });

export const createMemorySeatStore = (): SeatStore => {
  const tenures = new Map<string, StoredTenure>();
  const histories = new Map<string, readonly TenureHistoryEntry[]>();
  return Object.freeze({
    read: async (seat_id: string) => ({ ok: true, stored: tenures.get(seat_id) ?? null }) as const,
    commit: async (seat_id: string, expected_rev: number, tenure: SeatTenure, entries: readonly TenureHistoryEntry[]) => {
      // The check and both writes run synchronously, so they are atomic with respect to other callers.
      if ((tenures.get(seat_id)?.rev ?? 0) !== expected_rev) return { ok: true, committed: false } as const;
      tenures.set(seat_id, Object.freeze({ tenure: freezeTenure(tenure), rev: expected_rev + 1 }));
      histories.set(seat_id, Object.freeze([...(histories.get(seat_id) ?? []), ...entries.map((e) => Object.freeze({ ...e }))]));
      return { ok: true, committed: true } as const;
    },
    history: async (seat_id: string) => ({ ok: true, entries: histories.get(seat_id) ?? [] }) as const,
  });
};
