import { setTimeout as delay } from "node:timers/promises";
import type { Redis } from "ioredis";
import { InboxEvents, type InboxTraceStage } from "../core/inbox-events.js";
import { TaskClaimStore } from "../core/task-claim-store.js";
import { CLAIM_KEYS } from "../core/keys.js";
import { ActivationStore, type ActivationRecord } from "./activation-store.js";
import { activationOperationId, inboxPrompt, type RuntimeAdapter } from "./runtime.js";

export interface InboxDispatcher {
  readonly closed: Promise<void>;
  close(): Promise<void>;
}

/** One serial owner per binding. Notifications wake software; idle timers never wake a model. */
export const startInboxDispatcher = async (
  redis: Redis, agent: string, adapter: RuntimeAdapter,
  options: Readonly<{ intervalMs?: number; maxAttempts?: number; maxBackoffMs?: number }> = {},
): Promise<InboxDispatcher> => {
  const stop = new AbortController();
  const state = new ActivationStore(redis, agent);
  const events = new InboxEvents(redis);
  const claims = new TaskClaimStore(redis);
  const interval = options.intervalMs ?? 1_000;
  const maximum = options.maxAttempts ?? 3;
  const maxBackoff = Math.max(options.maxBackoffMs ?? 10_000, interval);
  // A busy or unavailable runtime is retried with exponential backoff, not on every tick or inbox event.
  let backoff = 0;
  let retryAt = 0;
  await state.attach(adapter.binding);
  const trace = (stage: InboxTraceStage, record?: ActivationRecord, code?: string, turnId?: string) =>
    events.trace(agent, { stage, timestamp: new Date().toISOString(),
      runtime_id: adapter.binding.runtime_id, operation_id: record?.operation_id,
      message_id: record?.message_ids[0], code, turn_id: turnId });
  await trace("runtime_bound");

  const createRecord = (ids: readonly string[], attempt: number): ActivationRecord => Object.freeze({
    operation_id: activationOperationId(agent, adapter.binding, ids, attempt),
    message_ids: Object.freeze([...ids]), attempt, state: "pending", created_at: new Date().toISOString(),
  });

  const reconcile = async (): Promise<void> => {
    if (!await state.refresh()) { stop.abort(); return; }
    await claims.recoverExpired({ actor_id: agent, now: new Date().toISOString() });
    const pending = await events.pending(agent);
    let record = await state.current();
    // Claimed messages are durable in the claim store; do not inject another turn while owned.
    if (!pending.length || (record && !record.message_ids.some((id) => pending.some((message) => message.id === id)))) {
      if (!await state.save(null)) { stop.abort(); return; }
      record = null;
    }
    if (!pending.length || await redis.zcard(CLAIM_KEYS.index(agent)) > 0) return;
    if (!record) {
      record = createRecord(pending.map((message) => message.id), 1);
      if (!await state.save(record)) { stop.abort(); return; }
    }
    if (record.state === "exhausted" || Date.now() < retryAt) return;
    const recovery = record.state !== "pending";
    if (!recovery) {
      if (!await state.save({ ...record, state: "submitting" })) { stop.abort(); return; }
      await trace("activation_requested", record);
    }
    // Save-before-submit permits recovery after a process dies at the native boundary.
    const outcome = await adapter.activate({ operation_id: record.operation_id,
      prompt: inboxPrompt(agent, record.operation_id), recover_only: recovery }, stop.signal);
    if (stop.signal.aborted || !await state.refresh()) return;
    const deferred = outcome.status === "busy" || outcome.status === "unavailable";
    backoff = deferred ? Math.min(Math.max(backoff * 2, interval), maxBackoff) : 0;
    retryAt = deferred ? Date.now() + backoff : 0;
    switch (outcome.status) {
      case "started":
      case "queued":
        await state.save({ ...record, state: "accepted" });
        if (record.state !== "accepted") await trace(outcome.status === "started" ? "turn_started" : "activation_queued", record,
          undefined, outcome.status === "started" ? outcome.turn_id : undefined);
        return;
      case "completed":
        // A turn ended without consuming all notified work; bounded fresh attempts prevent loops.
        await state.save(record.attempt < maximum
          ? createRecord(pending.map((message) => message.id), record.attempt + 1)
          : { ...record, state: "exhausted" });
        if (record.attempt >= maximum) await trace("activation_failed", record, "activation_attempts_exhausted");
        return;
      case "busy":
      case "unavailable":
        // A previously uncertain submission remains uncertain until native evidence resolves it.
        await state.save({ ...record, state: recovery ? record.state : "pending" });
        return;
      case "ambiguous":
        await state.save({ ...record, state: "ambiguous" });
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
    try {
      while (!stop.signal.aborted) {
        try {
          await reconcile();
          if (!stop.signal.aborted) cursor = await events.wait(agent, cursor, stop.signal, interval) ?? cursor;
        } catch {
          if (stop.signal.aborted) break;
          await trace("activation_failed", undefined, "dispatcher_reconcile_failed").catch(() => undefined);
          await delay(interval, undefined, { signal: stop.signal }).catch(() => undefined);
        }
      }
    } finally {
      await state.detach().catch(() => undefined);
      await adapter.close().catch(() => undefined);
      await trace("runtime_unbound").catch(() => undefined);
    }
  })();
  return Object.freeze({ closed, close: async () => { stop.abort(); await closed; } });
};
