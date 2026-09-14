import { isAbsolute, resolve } from "node:path";
import type { RuntimeAdapter, RuntimeBinding } from "./runtime.js";

type Entry = { type?: string; customType?: string; data?: unknown; message?: { role?: string; customType?: unknown; details?: unknown } };
export interface PiRuntimeHost {
  getContext(): { cwd?: string; sessionManager: { getSessionId(): string; getEntries?(): readonly Entry[] } };
  getEpoch?(): string;
  isIdle?(): boolean;
  sendMessage(payload: unknown, options: { deliverAs: "followUp"; triggerTurn: true }): void;
}

/** Own-session Pi adapter. Follow-ups are queued by Pi for both idle and busy turns. */
export const createPiRuntime = (binding: RuntimeBinding, host: PiRuntimeHost): RuntimeAdapter & { invalidate(): void } => {
  const submitted = new Map<string, "queued" | "ambiguous">();
  const remember = (id: string, state: "queued" | "ambiguous") => {
    submitted.set(id, state);
    if (submitted.size > 1024) submitted.delete(submitted.keys().next().value!);
  };
  let invalidated = false;
  const current = () => {
    const context = host.getContext();
    return context.sessionManager.getSessionId() === binding.runtime_id &&
      typeof context.cwd === "string" && isAbsolute(context.cwd) && isAbsolute(binding.working_directory) &&
      resolve(context.cwd) === resolve(binding.working_directory) && (!host.getEpoch || host.getEpoch() === binding.epoch);
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
      if (prior === "queued") return Object.freeze({ status: "queued" as const });
      const known = submitted.get(request.operation_id);
      if (known) return Object.freeze({ status: known });
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
