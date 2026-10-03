import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { CodexSocketClient } from "./codex-socket.js";
import type { CodexRpcClient } from "./codex-runtime.js";
import type { RuntimeTools } from "./runtime-tools.js";

/** Discover only a uniquely identified, native-loaded connection owned by this sidecar. */
export const findOwnCodexThread = async (
  rpc: CodexRpcClient, agent: string, cwd: string, signal: AbortSignal,
): Promise<string | undefined> => {
  const loaded = await rpc.request("thread/loaded/list", {}, signal);
  if (!Array.isArray(loaded.data) || loaded.data.length > 128 ||
      loaded.data.some(id => typeof id !== "string")) return undefined;
  const matches: string[] = [];
  for (const id of loaded.data as string[]) {
    signal.throwIfAborted();
    const read = await rpc.request("thread/read", { threadId: id, includeTurns: false }, signal).catch(() => undefined);
    const thread = (read?.thread ?? read) as Record<string, unknown> | undefined;
    if (thread?.id !== id || typeof thread.cwd !== "string" || resolve(thread.cwd) !== resolve(cwd)) continue;
    const response = await rpc.request("mcpServer/tool/call", {
      threadId: id, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {},
    }, signal).catch(() => undefined);
    const status = response?.structuredContent as Record<string, unknown> | undefined;
    if (!response?.isError && status?.status === "ok" && status.agent === agent &&
        (status.runtime === null || (typeof status.runtime === "object" &&
          (status.runtime as Record<string, unknown>).runtime_id === id))) matches.push(id);
    if (matches.length > 1) return undefined;
  }
  return matches.length === 1 ? matches[0] : undefined;
};

/** Startup is asynchronous so MCP initialize and daemon RPC cannot deadlock. */
export const bootstrapCodexBinding = async (
  runtime: RuntimeTools, agent: string, cwd: string, signal: AbortSignal,
  rpc: CodexRpcClient = new CodexSocketClient(),
): Promise<boolean> => {
  const boundedSignal = AbortSignal.any([signal, AbortSignal.timeout(45_000)]);
  try {
    for (let attempt = 0; attempt < 30 && !boundedSignal.aborted; attempt++) {
      const id = await findOwnCodexThread(rpc, agent, cwd, boundedSignal).catch(() => undefined);
      if (boundedSignal.aborted) return false;
      if (id) {
        const result = await runtime.bind({ client: "codex", runtime_id: id, epoch: id, working_directory: cwd });
        return result.activation_ready === true &&
          (result.runtime as Record<string, unknown> | undefined)?.runtime_id === id;
      }
      await delay(1_000, undefined, { signal: boundedSignal }).catch(() => undefined);
    }
    return false;
  } finally { await rpc.close(); }
};
