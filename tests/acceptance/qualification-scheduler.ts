import { createHash } from "node:crypto";
import type { PairLease, PairLeasePool, PairSpec, Participant, ParticipantIdentity, RouteId } from "./qualification-types.js";

export type LeaseWork<T> = (lease: Awaited<ReturnType<PairLeasePool["acquirePair"]>>) => Promise<T>;

export type CohortMember = Readonly<{ participant: Participant; identity: ParticipantIdentity }>;
type Slot = CohortMember & { readonly route: RouteId; held: boolean };
type Waiter = { pair: PairSpec; signal: AbortSignal; resolve: (value: AcquiredPair) => void; reject: (reason: unknown) => void; cleanup: () => void };
export type AcquiredPair = Readonly<{
  lease: PairLease; sender: Participant; receiver: Participant; release: () => Promise<void>;
}>;

const validKind = (participant: Participant): boolean => {
  const genericRoute = participant.identity.route.startsWith("generic-");
  return genericRoute ? participant.kind === "generic" : participant.kind === "model";
};
const identityKey = (identity: ParticipantIdentity): string => `${identity.route}:${identity.participantId}`;
const sameIdentity = (left: ParticipantIdentity, right: ParticipantIdentity): boolean =>
  identityKey(left) === identityKey(right) && left.hostRuntimeId === right.hostRuntimeId && left.agent === right.agent &&
  left.cwdHash === right.cwdHash && left.profileHash === right.profileHash && left.epochHash === right.epochHash;

/** A single-process cohort ledger. The reservation transition is synchronous. */
export class CohortLeasePool implements PairLeasePool {
  private readonly slots: Slot[];
  private readonly waiters: Waiter[] = [];
  private sequence = 0;

  public constructor(members: readonly CohortMember[]) {
    const keys = new Set<string>(), agents = new Set<string>(), runtimesByHost = new Set<string>();
    this.slots = members.map(({ participant, identity }) => {
      if (!validKind(participant) || !sameIdentity(participant.identity, identity)) throw new Error("participant identity/kind mismatch");
      if ([identity.participantId, identity.hostRuntimeId, identity.agent, identity.cwdHash, identity.profileHash, identity.epochHash].some((value) => value.length === 0)) throw new Error("participant identity fields must be nonempty");
      const key = identityKey(identity);
      if (keys.has(key)) throw new Error(`duplicate cohort identity ${key}`);
      if (agents.has(identity.agent)) throw new Error(`duplicate cohort agent ${identity.agent}`);
      const host = identity.route.split("-", 1)[0];
      const runtimeKey = `${host}:${identity.hostRuntimeId}`;
      if (runtimesByHost.has(runtimeKey)) throw new Error(`duplicate cohort runtime ${runtimeKey}`);
      keys.add(key);
      agents.add(identity.agent);
      runtimesByHost.add(runtimeKey);
      return { participant, identity, route: identity.route, held: false };
    });
    if (this.slots.length === 0) throw new Error("cohort must contain participants");
  }

  public acquirePair(pair: PairSpec, signal: AbortSignal): Promise<AcquiredPair> {
    this.validatePair(pair);
    if (signal.aborted) return Promise.reject(signal.reason ?? new Error("qualification operation aborted"));
    const available = this.tryReserve(pair);
    if (available) return Promise.resolve(available);
    return new Promise<AcquiredPair>((resolve, reject) => {
      let waiter!: Waiter;
      const onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        waiter.cleanup();
        reject(signal.reason ?? new Error("qualification operation aborted"));
      };
      waiter = { pair, signal, resolve, reject, cleanup: () => signal.removeEventListener("abort", onAbort) };
      signal.addEventListener("abort", onAbort, { once: true });
      this.waiters.push(waiter);
      this.pump();
    });
  }

  private validatePair(pair: PairSpec): void {
    if (!pair.pairId || !pair.nonce || !pair.sender || !pair.receiver) throw new Error("invalid pair specification");
    const sender = this.slots.filter(({ route }) => route === pair.sender);
    const receiver = this.slots.filter(({ route }) => route === pair.receiver);
    if (sender.length === 0 || receiver.length === 0) throw new Error(`missing cohort route for ${pair.pairId}`);
    if (pair.sender === pair.receiver && sender.length < 2) throw new Error(`same-route pair requires two participants: ${pair.sender}`);
  }

  private tryReserve(pair: PairSpec): AcquiredPair | undefined {
    const candidates = this.slots.filter(({ route, held }) => route === pair.sender && !held);
    const receiverCandidates = this.slots.filter(({ route, held }) => route === pair.receiver && !held);
    const sender = candidates[0];
    const receiver = pair.sender === pair.receiver ? candidates[1] : receiverCandidates[0];
    if (!sender || !receiver || sender.identity.participantId === receiver.identity.participantId) return undefined;
    sender.held = true; receiver.held = true;
    let released = false;
    const release = async (): Promise<void> => {
      if (released) return;
      released = true; sender.held = false; receiver.held = false; this.pump();
    };
    const leaseHash = createHash("sha256").update(JSON.stringify({ sequence: ++this.sequence, pair, sender: sender.identity, receiver: receiver.identity })).digest("hex");
    const lease: PairLease = Object.freeze({
      pair, sender: sender.identity, receiver: receiver.identity, leaseHash,
    });
    return Object.freeze({ lease, sender: sender.participant, receiver: receiver.participant, release });
  }

  private pump(): void {
    for (let index = 0; index < this.waiters.length;) {
      const waiter = this.waiters[index];
      if (!waiter) break;
      if (waiter.signal.aborted) {
        this.waiters.splice(index, 1);
        waiter.cleanup();
        waiter.reject(waiter.signal.reason ?? new Error("qualification operation aborted"));
        continue;
      }
      const acquired = this.tryReserve(waiter.pair);
      if (!acquired) { index += 1; continue; }
      this.waiters.splice(index, 1);
      waiter.cleanup();
      waiter.resolve(acquired);
    }
  }
}

/**
 * Acquire both sides through one pool operation. The pool, rather than this
 * caller, owns ordering and atomicity, so reverse ordered pairs cannot
 * deadlock by taking one participant each.
 */
export const withPairLease = async <T>(
  pool: PairLeasePool,
  pair: PairSpec,
  signal: AbortSignal,
  work: LeaseWork<T>,
): Promise<T> => {
  if (signal.aborted) throw abortError(signal);
  const acquired = await pool.acquirePair(pair, signal);
  if (signal.aborted) {
    await acquired.release();
    throw abortError(signal);
  }
  try {
    return await work(acquired);
  } finally {
    await acquired.release();
  }
};

export const abortError = (signal: AbortSignal): Error =>
  signal.reason instanceof Error ? signal.reason : new Error("qualification operation aborted");
