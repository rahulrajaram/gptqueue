import { setTimeout as delay } from "node:timers/promises";
import type { Redis } from "ioredis";
import { InboxEvents, type InboxTraceStage } from "../core/inbox-events.js";
import { TaskClaimStore } from "../core/task-claim-store.js";
import { ActivationStore, type ActivationRecord } from "./activation-store.js";
import { activationOperationId, inboxPrompt, subscribeIdle, type RuntimeAdapter } from "./runtime.js";

export interface InboxDispatcher {
  readonly closed: Promise<void>;
  close(): Promise<void>;
}

export type InboxDispatcherOptions = Readonly<{
  intervalMs?: number;
  maxAttempts?: number;
  maxBackoffMs?: number;
  /**
   * How long a submitting (recovery), accepted or ambiguous record may wait
   * for its work to be claimed before a fresh operation re-attempts it.
   * Default 10 min. This is shorter than the longest claim lease (3600 s):
   * the deadline only fires while no claim is held, so a long claim never
   * triggers it; it bounds the wait for the agent to take its first claim.
   */
  settleTimeoutMs?: number;
  /** How long an exhausted record parks before a new bounded round re-arms it. */
  exhaustedCooldownMs?: number;
  /** Lease renewal period while one native activation is in flight. */
  leaseRenewMs?: number;
}>;

type ActivationState = ActivationRecord["state"];

/** States whose native outcome is still awaited; each one is bounded by the settle deadline. */
const awaitsSettlement = (state: ActivationState): boolean => {
  switch (state) {
    case "submitting":
    case "accepted":
    case "ambiguous":
      return true;
    case "pending":
    case "exhausted":
      return false;
    default: {
      const exhaustive: never = state;
      throw new Error(`Unhandled activation state: ${String(exhaustive)}`);
    }
  }
};

const since = (record: ActivationRecord): number => Date.parse(record.state_since ?? record.created_at);

/** One serial owner per binding. Notifications wake software; idle timers never wake a model. */
export const startInboxDispatcher = async (
  redis: Redis, agent: string, adapter: RuntimeAdapter, options: InboxDispatcherOptions = {},
): Promise<InboxDispatcher> => {
  const stop = new AbortController();
  const state = new ActivationStore(redis, agent);
  const events = new InboxEvents(redis);
  const claims = new TaskClaimStore(redis);
  const interval = options.intervalMs ?? 1_000;
  const maximum = options.maxAttempts ?? 3;
  const settleTimeout = options.settleTimeoutMs ?? 600_000;
  const cooldown = options.exhaustedCooldownMs ?? 900_000;
  const leaseRenew = options.leaseRenewMs ?? 10_000;
  // Backoff never outlasts a deadline, so a deferred record is re-examined in time.
  const maxBackoff = Math.max(Math.min(options.maxBackoffMs ?? 10_000, settleTimeout, cooldown), interval);
  // A busy or unavailable runtime is retried with exponential backoff, not on every tick or inbox event.
  let backoff = 0;
  let retryAt = 0;
  // One deferral trace per (operation, reason) streak, so a long busy or
  // unavailable period is provable without per-tick spam.
  let lastDeferred: Readonly<{ operation_id: string; code: string }> | undefined;
  await state.attach(adapter.binding);
  const trace = (stage: InboxTraceStage, record?: ActivationRecord, code?: string, turnId?: string) =>
    events.trace(agent, { stage, timestamp: new Date().toISOString(),
      runtime_id: adapter.binding.runtime_id, operation_id: record?.operation_id,
      message_id: record?.message_ids[0], code, turn_id: turnId });
  await trace("runtime_bound");

  const createRecord = (ids: readonly string[], attempt: number, roundStart = 1): ActivationRecord => {
    const now = new Date().toISOString();
    return Object.freeze({
      operation_id: activationOperationId(agent, adapter.binding, ids, attempt),
      message_ids: Object.freeze([...ids]), attempt, state: "pending", created_at: now, state_since: now, round_start: roundStart,
    });
  };
  /** A state change restarts the deadline clock; a same-state save keeps it. */
  const enter = (record: ActivationRecord, next: ActivationState): ActivationRecord => Object.freeze(
    next === record.state ? { ...record } : { ...record, state: next, state_since: new Date().toISOString() });
  const roundExhausted = (record: ActivationRecord): boolean => record.attempt - (record.round_start ?? 1) + 1 >= maximum;
  /** The next bounded attempt for the same work, or exhaustion when the round is spent. */
  const nextAttempt = (record: ActivationRecord, ids: readonly string[]): ActivationRecord => roundExhausted(record)
    ? enter(record, "exhausted")
    : createRecord(ids, record.attempt + 1, record.round_start ?? 1);

  /** Lease loss is fatal only when a live foreign owner holds the binding. */
  const renewLease = async (): Promise<boolean> => {
    const renewal = await state.renew();
    if (renewal === "reacquired") await trace("runtime_bound", undefined, "lease_reacquired");
    if (renewal !== "lost") return true;
    await trace("activation_failed", undefined, "dispatcher_lease_lost").catch(() => undefined);
    stop.abort();
    return false;
  };
  const save = async (record: ActivationRecord | null): Promise<boolean> => {
    if (await state.save(record)) return true;
    // The lease may have lapsed between renewals: re-acquire once, then retry.
    if (await renewLease() && await state.save(record)) return true;
    stop.abort();
    return false;
  };

  const reconcile = async (): Promise<void> => {
    if (!await renewLease()) return;
    const recovered = await claims.recoverExpired({ actor_id: agent, now: new Date().toISOString() });
    if (!recovered.ok) throw new Error(recovered.error.message);
    const pending = await events.pending(agent);
    const pendingIds = pending.map((message) => message.id);
    let record = await state.current();
    // Clear a record none of whose messages is pending any more. New work a
    // non-pending record does not cover re-arms activation instead of waiting
    // behind an exhausted, accepted or ambiguous operation.
    const stale = record !== null && !record.message_ids.some((id) => pendingIds.includes(id));
    const superseded = record !== null && record.state !== "pending" && record.state !== "submitting" &&
      pendingIds.some((id) => !record!.message_ids.includes(id));
    if (!pending.length || stale) {
      if (!await save(null)) return;
      record = null;
    }
    // Only unclaimed mail wakes the runtime, and an open claim does not block
    // that: an agent can hold claims across idle periods (claim, work, ack
    // later), and new mail must still reach it. A working runtime is protected
    // by its adapter (busy defers; Pi queues a follow-up), not by claim state.
    // Known, accepted race: mail claimed between the pending read and
    // adapter.activate can still be followed by one prompt, at worst one empty turn.
    if (!pending.length) return;
    if (record && superseded) {
      await trace("activation_rearmed", record, "activation_superseded_by_new_work");
      record = null;
    }
    if (record?.state === "exhausted") {
      if (Date.now() - since(record) < cooldown) return;
      // A cooldown starts a fresh bounded round with new operation ids.
      await trace("activation_rearmed", record, "activation_cooldown_elapsed");
      record = createRecord(pendingIds, record.attempt + 1, record.attempt + 1);
      if (!await save(record)) return;
    } else if (record && awaitsSettlement(record.state) && Date.now() - since(record) >= settleTimeout) {
      // No claim arrived within the deadline: re-attempt under a new operation id.
      await trace("activation_failed", record, "activation_deadline_expired");
      const next = nextAttempt(record, pendingIds);
      if (next.state === "exhausted") await trace("activation_failed", record, "activation_attempts_exhausted");
      if (!await save(next)) return;
      record = next;
      if (record.state === "exhausted") return;
    }
    if (!record) {
      record = createRecord(pendingIds, 1);
      if (!await save(record)) return;
    }
    if (Date.now() < retryAt) return;
    const recovery = record.state !== "pending";
    if (!recovery) {
      if (!await save(enter(record, "submitting"))) return;
      await trace("activation_requested", record);
    }
    // Save-before-submit permits recovery after a process dies at the native boundary.
    // Adapters resolve every failure at or after the native submit as
    // `ambiguous`; a throw therefore means nothing was submitted.
    const heartbeat = setInterval(() => { void renewLease().catch(() => undefined); }, leaseRenew);
    let outcome: Awaited<ReturnType<RuntimeAdapter["activate"]>>;
    try {
      outcome = await adapter.activate({ operation_id: record.operation_id,
        prompt: inboxPrompt(agent, record.operation_id, adapter.binding.client), recover_only: recovery }, stop.signal);
    } catch {
      clearInterval(heartbeat);
      if (stop.signal.aborted) return;
      backoff = Math.min(Math.max(backoff * 2, interval), maxBackoff);
      retryAt = Date.now() + backoff;
      await trace("activation_failed", record, "activation_submit_failed");
      // Recovery keeps its uncertain state; a pre-submit failure returns to pending.
      if (!recovery) await save(enter(record, "pending"));
      return;
    }
    clearInterval(heartbeat);
    if (stop.signal.aborted || !await renewLease()) return;
    const deferred = outcome.status === "busy" || outcome.status === "unavailable";
    backoff = deferred ? Math.min(Math.max(backoff * 2, interval), maxBackoff) : 0;
    retryAt = deferred ? Date.now() + backoff : 0;
    switch (outcome.status) {
      case "started":
      case "queued":
        await save(enter(record, "accepted"));
        if (record.state !== "accepted") await trace(outcome.status === "started" ? "turn_started" : "activation_queued", record,
          undefined, outcome.status === "started" ? outcome.turn_id : undefined);
        lastDeferred = undefined;
        return;
      case "completed":
      case "undelivered": {
        // A turn ended without consuming all notified work, or the submission
        // provably never ran: either way the next bounded attempt follows at
        // once (no backoff), and the attempt bound prevents loops.
        if (outcome.status === "undelivered") await trace("activation_failed", record, "native_delivery_dropped");
        const next = nextAttempt(record, pendingIds);
        await save(next);
        if (next.state === "exhausted") await trace("activation_failed", record, "activation_attempts_exhausted");
        return;
      }
      case "busy":
      case "unavailable": {
        // busy vs unavailable is retained as distinct durable codes so either
        // can be proven from the trace after the fact.
        const code = outcome.status === "busy" ? "runtime_busy" : "runtime_unavailable";
        if (lastDeferred?.operation_id !== record.operation_id || lastDeferred.code !== code) {
          lastDeferred = { operation_id: record.operation_id, code };
          await trace("activation_deferred", record, code);
        }
        // A previously uncertain submission remains uncertain until native evidence resolves it.
        await save(recovery ? record : enter(record, "pending"));
        return;
      }
      case "ambiguous":
        await save(enter(record, "ambiguous"));
        if (record.state !== "ambiguous") await trace("activation_failed", record, "native_delivery_ambiguous");
        return;
      default: {
        const exhaustive: never = outcome;
        throw new Error(`Unhandled activation outcome: ${String(exhaustive)}`);
      }
    }
  };

  const closed = (async () => {
    let cursor = "0-0";
    const waiter = events.createWaiter();
    // I-4: a runtime's idle hint ends the current wait and any backoff, so a
    // busy activation is retried as soon as the turn ends. activate() still
    // re-checks state; the backoff remains the fallback when no hint comes.
    let wake = new AbortController();
    const onStop = () => wake.abort();
    stop.signal.addEventListener("abort", onStop, { once: true });
    const unsubscribeIdle = subscribeIdle(adapter, () => {
      backoff = 0;
      retryAt = 0;
      wake.abort();
    });
    /** True (and re-armed) when an idle hint, not a stop, ended the wait. */
    const woken = (): boolean => {
      if (stop.signal.aborted || !wake.signal.aborted) return false;
      wake = new AbortController();
      return true;
    };
    try {
      while (!stop.signal.aborted) {
        try {
          await reconcile();
          if (stop.signal.aborted || woken()) continue;
          try {
            cursor = await waiter.wait(agent, cursor, wake.signal, interval) ?? cursor;
          } catch (error) {
            if (woken()) continue;
            throw error;
          }
        } catch {
          if (stop.signal.aborted) break;
          await trace("activation_failed", undefined, "dispatcher_reconcile_failed").catch(() => undefined);
          await delay(interval, undefined, { signal: stop.signal }).catch(() => undefined);
        }
      }
    } finally {
      unsubscribeIdle();
      stop.signal.removeEventListener("abort", onStop);
      waiter.close();
      await state.detach().catch(() => undefined);
      await adapter.close().catch(() => undefined);
      await trace("runtime_unbound").catch(() => undefined);
    }
  })();
  return Object.freeze({ closed, close: async () => { stop.abort(); await closed; } });
};
