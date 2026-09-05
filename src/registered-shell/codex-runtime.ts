import { resolve } from "node:path";
import type { ActivationRequest, ActivationOutcome, RuntimeAdapter, RuntimeBinding } from "./runtime.js";
import { CodexSocketClient } from "./codex-socket.js";
import { CodexThreadReader, userMessageMatches } from "./codex-history.js";

export interface CodexRpcClient {
  request(method: string, params: Record<string, unknown>, signal: AbortSignal): Promise<Record<string, unknown>>;
  close(): Promise<void>;
}

export interface CodexRuntimeOptions {
  readonly client?: CodexRpcClient;
  readonly timeoutMs?: number;
  readonly expectedAgent?: string;
}

const activeTurn = (thread: Record<string, unknown>): boolean => {
  const turns = thread.turns;
  if (!Array.isArray(turns)) return false;
  return turns.some((turn) => {
    if (!turn || typeof turn !== "object") return false;
    const status = String((turn as Record<string, unknown>).status ?? "").toLowerCase();
    return ["inprogress", "in_progress", "running", "started"].includes(status);
  });
};

export const createCodexRuntime = async (
  binding: RuntimeBinding,
  options: CodexRuntimeOptions = {},
): Promise<RuntimeAdapter> => {
  if (binding.client !== "codex") throw new Error("Codex runtime requires a Codex binding");
  const client = options.client ?? new CodexSocketClient(undefined, options.timeoutMs);
  const signal = AbortSignal.timeout(options.timeoutMs ?? 10_000);
  const reader = new CodexThreadReader(client);
  let raw: Record<string, unknown>;
  try { raw = await reader.read(binding.runtime_id, signal); } catch (error) { await client.close(); throw error; }
  if (String(raw.id ?? "") !== binding.runtime_id || typeof raw.cwd !== "string" || resolve(raw.cwd) !== resolve(binding.working_directory)) { await client.close(); throw new Error("Codex thread identity does not match binding"); }
  if (options.expectedAgent) {
    try {
      const ownConnection = await client.request("mcpServer/tool/call", {
        threadId: binding.runtime_id, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {},
      }, signal);
      const status = ownConnection.structuredContent as Record<string, unknown> | undefined;
      if (ownConnection.isError || status?.agent !== options.expectedAgent) {
        throw new Error("Codex thread does not own this GPTQueue connection");
      }
    } catch (error) { await client.close(); throw error; }
  }
  const seen = new Map<string, string>();
  return {
    binding,
    async activate(request: ActivationRequest, abortSignal: AbortSignal): Promise<ActivationOutcome> {
      const current = await reader.read(binding.runtime_id, abortSignal);
      if (String(current.id ?? "") !== binding.runtime_id || typeof current.cwd !== "string" || resolve(current.cwd) !== resolve(binding.working_directory)) throw new Error("Codex thread identity does not match binding");
      if (!Array.isArray(current.turns)) return { status: "unavailable" };
      const turns = current.turns;
      for (const turn of turns) {
        const items = turn && typeof turn === "object" ? (turn as Record<string, unknown>).items : undefined;
        if (Array.isArray(items)) for (const item of items) {
          if (item && typeof item === "object" && userMessageMatches(item as Record<string, unknown>, request.operation_id, request.prompt)) {
            const id = String((turn as Record<string, unknown>).id ?? "");
            if (id) {
              const status = String((turn as Record<string, unknown>).status ?? "").toLowerCase();
              return ["completed", "succeeded", "failed", "interrupted"].includes(status) ? { status: "completed", turn_id: id } : { status: "started", turn_id: id };
            }
          }
        }
      }
      if (activeTurn(current)) return { status: "busy" };
      if (turns.some((turn) => !turn || typeof turn !== "object" || !Array.isArray(turn.items) || (turn.itemsView && turn.itemsView !== "full"))) return { status: "ambiguous" };
      if (seen.has(request.operation_id) || request.recover_only) return { status: "ambiguous" };
      seen.set(request.operation_id, "pending");
      if (seen.size > 1024) seen.delete(seen.keys().next().value!);
      try {
        const result = await client.request("turn/start", { threadId: binding.runtime_id, input: [{ type: "text", text: request.prompt }], clientUserMessageId: request.operation_id }, abortSignal);
        const id = String(((result.turn ?? result) as Record<string, unknown>).id ?? "");
        if (!id) return { status: "ambiguous" };
        seen.set(request.operation_id, id); return { status: "started", turn_id: id };
      } catch (error) {
        const message = String(error);
        if (abortSignal.aborted) return { status: "ambiguous" };
        if (/timeout|disconnect|closed|network|aborted/i.test(message)) return { status: "ambiguous" };
        if (/Codex RPC rejected:.*(?:busy|active|in progress)/i.test(message)) { seen.delete(request.operation_id); return { status: "busy" }; }
        return { status: "ambiguous" };
      }
    },
    close: () => client.close(),
  };
};
