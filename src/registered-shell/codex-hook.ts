import { z } from "zod";
import { setTimeout as delay } from "node:timers/promises";
import { isAbsolute } from "node:path";
import { CodexSocketClient } from "./codex-socket.js";
import type { CodexRpcClient } from "./codex-runtime.js";

const eventSchema = z.object({
  session_id: z.string().uuid(),
  cwd: z.string().refine(isAbsolute),
  hook_event_name: z.enum(["SessionStart", "UserPromptSubmit"]),
});

/** The daemon routes this call through the exact thread's own MCP connection. */
export const bindCodexHook = async (
  event: unknown, rpc: CodexRpcClient,
  options: Readonly<{ server?: string; timeoutMs?: number; retryMs?: number }> = {},
): Promise<boolean> => {
  const input = eventSchema.parse(event);
  const deadline = Date.now() + (options.timeoutMs ?? 30_000);
  const signal = AbortSignal.timeout(options.timeoutMs ?? 30_000);
  while (!signal.aborted && Date.now() < deadline) {
    try {
      const result = await rpc.request("mcpServer/tool/call", {
        threadId: input.session_id, server: options.server ?? "gptqueue-shared", tool: "bind_runtime",
        arguments: { client: "codex", runtime_id: input.session_id, epoch: input.session_id, working_directory: input.cwd },
      }, signal);
      const status = result.structuredContent as Record<string, unknown> | undefined;
      if (!result.isError && status?.activation_ready === true) return true;
    } catch { if (signal.aborted) break; }
    await delay(options.retryMs ?? 250, undefined, { signal }).catch(() => undefined);
  }
  return false;
};

export const runCodexHook = async (): Promise<void> => {
  let input = "";
  for await (const chunk of process.stdin) {
    input += String(chunk);
    if (Buffer.byteLength(input) > 65_536) throw new Error("Hook input exceeds size limit");
  }
  const rpc = new CodexSocketClient();
  try {
    if (!await bindCodexHook(JSON.parse(input), rpc)) {
      console.error('[gptqueue] {"event":"runtime_binding_failed","code":"codex_binding_unavailable"}');
      process.exitCode = 1;
    }
  } finally { await rpc.close(); }
};
