import { isAbsolute, resolve } from "node:path";
import type { RuntimeAdapter, RuntimeBinding } from "./runtime.js";

type Entry = { type?: string; customType?: string; data?: unknown; message?: { role?: string; customType?: unknown; details?: unknown } };
/** Optional user-facing surface of Pi's ExtensionContext (docs/extensions.md: ctx.ui). */
export type PiUi = { notify?(message: string, level?: "info" | "warning" | "error"): void; setStatus?(key: string, text: string | undefined): void };
export interface PiRuntimeHost {
  getContext(): {
    cwd?: string; sessionManager: { getSessionId(): string; getEntries?(): readonly Entry[] };
    /** Pi ctx.isIdle(): false while an agent run, retry or queued continuation is in progress. */
    isIdle?(): boolean;
    /** Pi ctx.hasPendingMessages(): true while steer/follow-up messages are still queued. */
    hasPendingMessages?(): boolean;
    hasUI?: boolean; ui?: PiUi;
  };
  getEpoch?(): string;
  isIdle?(): boolean;
  sendMessage(payload: unknown, options: { deliverAs: "followUp"; triggerTurn: true }): void;
}

/** A submission younger than this may not yet be visible to isIdle()/hasPendingMessages(). */
export const PI_SUBMIT_GRACE_MS = 5_000;
/** turn_id reported when native evidence shows the follow-up never ran; the dispatcher then retries. */
export const piUndeliveredTurnId = (operationId: string): string => `pi:undelivered:${operationId}`;

/**
 * Own-session Pi adapter. Follow-ups are queued by Pi for both idle and busy turns.
 * Recovery is evidence-based: the session transcript, plus Pi's idle and
 * pending-queue state, decide whether a follow-up is still queued, ran, or was
 * dropped. Only when neither the transcript nor the queue can show it is the
 * outcome ambiguous.
 */
export const createPiRuntime = (binding: RuntimeBinding, host: PiRuntimeHost,
  options: Readonly<{ now?: () => number; graceMs?: number }> = {}): RuntimeAdapter & { invalidate(): void } => {
  const now = options.now ?? Date.now;
  const grace = options.graceMs ?? PI_SUBMIT_GRACE_MS;
  const submitted = new Map<string, Readonly<{ state: "queued" | "ambiguous"; at: number }>>();
  const remember = (id: string, state: "queued" | "ambiguous") => {
    submitted.set(id, Object.freeze({ state, at: now() }));
    if (submitted.size > 1024) submitted.delete(submitted.keys().next().value!);
  };
  let invalidated = false;
  const current = () => {
    const context = host.getContext();
    return context.sessionManager.getSessionId() === binding.runtime_id &&
      typeof context.cwd === "string" && isAbsolute(context.cwd) && isAbsolute(binding.working_directory) &&
      resolve(context.cwd) === resolve(binding.working_directory) && (!host.getEpoch || host.getEpoch() === binding.epoch);
  };
  /** True only when Pi itself proves nothing is running or queued. Unknown host state is not evidence. */
  const quiescent = (): boolean => {
    const context = host.getContext();
    if (typeof context.isIdle !== "function" || typeof context.hasPendingMessages !== "function") return false;
    try { return context.isIdle() && !context.hasPendingMessages(); } catch { return false; }
  };
  const recorded = (operationId: string): false | "queued" | { completed: string } => {
    const entries = host.getContext().sessionManager.getEntries?.() ?? [];
    for (const entry of entries) {
      if (entry.type !== "custom" || entry.customType !== "gptqueue-inbox-activation-completed") continue;
      const data = entry.data as { operation_id?: unknown; runtime_id?: unknown; turn_id?: unknown } | undefined;
      if (data?.operation_id === operationId && data.runtime_id === binding.runtime_id && typeof data.turn_id === "string") {
        return { completed: data.turn_id };
      }
    }
    for (const entry of entries) {
      const message = entry.message;
      if (entry.type !== "message" || message?.role !== "custom" || message.customType !== "gptqueue-inbox-activation") continue;
      const details = message.details as { operation_id?: unknown; status?: unknown; turn_id?: unknown } | undefined;
      if (details?.operation_id !== operationId) continue;
      if (details.status === "completed" && typeof details.turn_id === "string") return { completed: details.turn_id };
      return "queued";
    }
    return false;
  };
  const adapter: RuntimeAdapter & { invalidate(): void } = {
    binding,
    invalidate: () => { invalidated = true; },
    activate: async (request, signal) => {
      if (signal.aborted || invalidated || !current()) return Object.freeze({ status: "unavailable" as const });
      const prior = recorded(request.operation_id);
      if (prior && typeof prior === "object") return Object.freeze({ status: "completed" as const, turn_id: prior.completed });
      const known = submitted.get(request.operation_id);
      const settled = quiescent() && (!known || now() - known.at >= grace);
      if (prior === "queued") {
        // The follow-up reached the transcript but no completion marker was
        // written (e.g. the extension restarted mid-turn). Once Pi is idle with
        // nothing queued, that turn is over: report it so pending work is re-checked.
        return settled ? Object.freeze({ status: "completed" as const, turn_id: `pi:${request.operation_id}` })
          : Object.freeze({ status: "queued" as const });
      }
      // Not in the transcript and Pi is idle with an empty queue: the follow-up
      // was dropped (abort, cleared queue) or lost with a restarted process.
      // A definite outcome lets the dispatcher submit a fresh bounded attempt.
      if ((known || request.recover_only) && settled) {
        submitted.delete(request.operation_id);
        return Object.freeze({ status: "completed" as const, turn_id: piUndeliveredTurnId(request.operation_id) });
      }
      if (known) return Object.freeze({ status: known.state });
      if (request.recover_only) return Object.freeze({ status: "ambiguous" as const });
      const payload = Object.freeze({ customType: "gptqueue-inbox-activation", content: request.prompt,
        display: false, details: Object.freeze({ operation_id: request.operation_id, runtime_id: binding.runtime_id, epoch: binding.epoch }) });
      remember(request.operation_id, "ambiguous");
      try { host.sendMessage(payload, { deliverAs: "followUp", triggerTurn: true }); }
      catch { return Object.freeze({ status: "ambiguous" as const }); }
      remember(request.operation_id, "queued");
      return Object.freeze({ status: "queued" as const });
    },
    close: async () => { invalidated = true; },
  };
  return Object.freeze(adapter);
};
