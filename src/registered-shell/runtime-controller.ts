import type { RedisClient } from "../mcp-server/redis-client.js";
import { startInboxDispatcher, type InboxDispatcher } from "./inbox-dispatcher.js";
import { subscribeStale, validateRuntimeBinding, type RuntimeAdapter, type RuntimeBinding } from "./runtime.js";
import type { RuntimeTools } from "./runtime-tools.js";
import { restoreRuntimeMailbox, RuntimeMailboxAdoptionError, RuntimeMailboxAdoptionRefused } from "./runtime-mailbox.js";
import { operatorActionRequired, type AdoptionRefusalReason, type AdoptionRefusalSignal } from "../core/adoption-refusal.js";

/**
 * A pending mailbox adoption the guard refused (status `continuity`; null when
 * none is pending). `signal` is the FIRST blocking check; others may also
 * block. `recheck_after` is a HINT, never a promise: other claims may remain,
 * renewals move expiry and further sends extend outbound activity.
 */
export type ContinuityState = Readonly<{
  /** The name this connection holds now (its provisional name). */
  current_mailbox: string | null;
  /** The mapped mailbox whose adoption was refused. */
  target: string;
  reason: AdoptionRefusalReason;
  signal: AdoptionRefusalSignal | null;
  code: number;
  first_refused_at: string;
  last_attempt_at: string;
  recheck_after: string | null;
  /** What `recheck_after` was derived from (null when no hint is known). */
  recheck_basis: "earliest_unexpired_claim_expiry" | "outbound_activity_ttl" | null;
  operator_action_required: boolean;
}>;

/** Pure: the continuity record after a refusal. first_refused_at survives repeated refusals of the same target. */
export const refusedContinuity = (
  previous: ContinuityState | null, refusal: RuntimeMailboxAdoptionRefused, currentMailbox: string | null, now: number,
): ContinuityState => {
  const at = new Date(now).toISOString();
  const hint = refusal.recheckAfterMs;
  return Object.freeze({
    current_mailbox: currentMailbox, target: refusal.target, reason: refusal.reason, signal: refusal.signal, code: refusal.code,
    first_refused_at: previous?.target === refusal.target ? previous.first_refused_at : at,
    last_attempt_at: at,
    recheck_after: hint === null ? null : new Date(hint).toISOString(),
    recheck_basis: hint === null ? null : refusal.signal === "outbound_activity" ? "outbound_activity_ttl" : "earliest_unexpired_claim_expiry",
    operator_action_required: operatorActionRequired(refusal.reason),
  });
};

export const createRuntimeController = (
  client: RedisClient,
  expected: Readonly<{ client: "codex" | "pi"; working_directory: string }>,
  createAdapter: (binding: RuntimeBinding) => Promise<RuntimeAdapter>,
  /** Called once the bound runtime reports it was closed by its host; the owner should retire the session. */
  onRuntimeStale: (binding: RuntimeBinding) => void = () => undefined,
): RuntimeTools & { close(): Promise<void> } => {
  let binding: RuntimeBinding | undefined;
  let host: RuntimeAdapter | undefined;
  let dispatcher: InboxDispatcher | undefined;
  let closed = false;
  // DL-2: the bound mailbox a failed adoption targeted. While this client
  // still holds its provisional name, closing retires that name into it.
  let refusedTarget: string | undefined;
  // The pending adoption the guard last refused; cleared by a successful restore.
  let continuity: ContinuityState | null = null;
  let operation: Promise<unknown> = Promise.resolve();
  const status = (): Record<string, unknown> => {
    const running = !!dispatcher && !closed;
    return {
      status: "ok", agent: client.agentName, activation_ready: running,
      // Reported separately: continuity can be pending while no dispatcher runs on any name.
      dispatcher: running ? "on" : "off",
      ...(continuity && !running ? { readiness: "awaiting_adoption" } : {}),
      continuity,
      runtime: binding ?? null,
      ...(host?.describe && running ? { runtime_host: host.describe() } : {}),
    };
  };
  return {
    status,
    bind: (value) => {
      const work = operation.then(async () => {
        if (closed) return { status: "error", code: "runtime_controller_closed" };
        const validation = validateRuntimeBinding(value, expected);
        if (!validation.ok) return { status: "error", code: validation.code };
        // A sidecar belongs to one conversation. A new conversation gets its own connection.
        if (binding && binding.runtime_id !== validation.binding.runtime_id) {
          return { status: "error", code: "runtime_rebind_requires_new_connection" };
        }
        if (dispatcher && binding?.epoch === validation.binding.epoch) return status();
        const adapter = await createAdapter(validation.binding);
        if (closed) { await adapter.close(); return { status: "error", code: "runtime_controller_closed" }; }
        await dispatcher?.close();
        // A closed dispatcher no longer runs: status must not report it while this bind is refused.
        dispatcher = undefined;
        binding = validation.binding;
        try {
          try {
            await restoreRuntimeMailbox(client, binding);
            refusedTarget = undefined;
            continuity = null;
          } catch (error) {
            if (error instanceof RuntimeMailboxAdoptionError) refusedTarget = error.target;
            // Only a classified refusal is recorded; an infrastructure failure is not one.
            if (error instanceof RuntimeMailboxAdoptionRefused) continuity = refusedContinuity(continuity, error, client.agentName, Date.now());
            throw error;
          }
          dispatcher = await startInboxDispatcher(client.adapterConnection, client.requireRegistered(), adapter);
          const active = dispatcher;
          host = adapter;
          void active.closed.finally(() => { if (dispatcher === active) dispatcher = undefined; });
          const bound = binding;
          // A closed host thread must not keep looking activation-ready, and must
          // release its dispatcher lease so a resumed session can take over.
          subscribeStale(adapter, () => {
            operation = operation.then(async () => {
              if (dispatcher !== active || binding !== bound) return;
              dispatcher = undefined;
              await active.close().catch(() => undefined);
              onRuntimeStale(bound);
            }).catch(() => undefined);
          });
          return status();
        } catch (error) { await adapter.close(); throw error; }
      });
      operation = work.catch(() => undefined);
      return work;
    },
    close: async () => {
      closed = true;
      await operation;
      await dispatcher?.close();
      dispatcher = undefined;
      const provisional = client.agentName;
      if (refusedTarget && provisional && provisional !== refusedTarget) {
        // Hand anything a peer sent to the provisional name to the bound
        // mailbox and stop the name being discoverable, in one atomic step.
        await client.retireProvisionalMailbox(provisional, refusedTarget).catch(() => false);
      }
    },
  };
};
