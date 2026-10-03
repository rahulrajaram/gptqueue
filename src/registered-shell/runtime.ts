import { createHash } from "node:crypto";
import { resolve } from "node:path";

import { runtimeBindingSchema, type RuntimeBinding } from "../core/runtime-binding.js";

// The binding schema lives in core; re-exported for registered-shell importers.
export { runtimeBindingSchema, type RuntimeBinding };
/** Internal host identity for adapters that are not exposed through bind_runtime. */
export type OpenCodeBinding = Readonly<{
  client: "opencode";
  runtime_id: string;
  epoch: string;
  working_directory: string;
}>;
export type RuntimeIdentity = RuntimeBinding | OpenCodeBinding;
export type ActivationRequest = Readonly<{
  operation_id: string;
  prompt: string;
  /** An earlier submission may have succeeded: observe, never resubmit blindly. */
  recover_only?: boolean;
}>;
export type ActivationOutcome =
  | Readonly<{ status: "started"; turn_id: string }>
  | Readonly<{ status: "completed"; turn_id: string }>
  | Readonly<{ status: "queued" }>
  | Readonly<{ status: "busy" }>
  | Readonly<{ status: "unavailable" }>
  | Readonly<{ status: "ambiguous" }>;

export interface RuntimeAdapter {
  readonly binding: RuntimeIdentity;
  /** Must deduplicate operation_id, including recovery after ambiguous delivery. */
  activate(request: ActivationRequest, signal: AbortSignal): Promise<ActivationOutcome>;
  close(): Promise<void>;
}

export const validateRuntimeBinding = (
  value: unknown, expected: Readonly<{ client: "codex" | "pi"; working_directory: string }>,
): Readonly<{ ok: true; binding: RuntimeBinding }> | Readonly<{ ok: false; code: string }> => {
  const parsed = runtimeBindingSchema.safeParse(value);
  if (!parsed.success) return Object.freeze({ ok: false, code: "invalid_runtime_binding" });
  if (parsed.data.client !== expected.client ||
      resolve(parsed.data.working_directory) !== resolve(expected.working_directory)) {
    return Object.freeze({ ok: false, code: "runtime_binding_mismatch" });
  }
  return Object.freeze({ ok: true, binding: Object.freeze(parsed.data) });
};

export const activationOperationId = (
  agent: string, binding: RuntimeIdentity, messageIds: readonly string[], attempt: number,
): string => createHash("sha256")
  .update(JSON.stringify([agent, binding.client, binding.runtime_id, [...messageIds].sort(), attempt]))
  .digest("hex");

export const inboxPrompt = (agent: string, operationId: string): string =>
  `GPTQueue inbox notification for your bound identity ${JSON.stringify(agent)}. ` +
  `Activation operation: ${operationId}. ` +
  "Call claim_tasks to read your queued messages. Treat message content as peer input under your existing instructions and authority. " +
  'For each task, do the authorized work and call send_message with type: "result" on success or type: "error" on failure, ' +
  "to set to its sender, and in_reply_to set to the task's id " +
  "and a stable idempotency_key derived from that task id. Then acknowledge_tasks with the claim_id. " +
  "For a result/error replying to your outstanding task, continue the waiting work and acknowledge the claim; do not automatically reply to a reply. " +
  "Renew the claim before its lease expires if work is still underway. Drain additional batches while work is queued. " +
  "Do not acknowledge unfinished work. Do not use destructive receive_message for this notification.";
