import { z } from "zod";
import { setTimeout as delay } from "node:timers/promises";
import { isAbsolute } from "node:path";
import { CodexSocketClient } from "./codex-socket.js";
import { createHookLog, type HookBindingLog } from "./lifecycle-log.js";
import type { CodexRpcClient } from "./codex-runtime.js";

const eventSchema = z.object({
  session_id: z.string().uuid(),
  cwd: z.string().refine(isAbsolute),
  hook_event_name: z.enum(["SessionStart", "UserPromptSubmit"]),
});

export type HookBindingOutcome =
  | "ready" | "not_ready" | "legacy_connection_requires_reconnect"
  | "timeout" | "transport_error";

/** A null logger keeps the binding path unchanged when diagnostics are disabled. */
const noopLog: HookBindingLog = { emit: () => undefined, close: async () => undefined };

/**
 * The daemon routes this call through the exact thread's own MCP connection.
 * Each attempt and terminal outcome is emitted to the durable hook log with
 * safe codes only; logging failures never interrupt binding.
 */
export const bindCodexHook = async (
  event: unknown, rpc: CodexRpcClient,
  options: Readonly<{
    server?: string; timeoutMs?: number; retryMs?: number;
    onUnavailable?: (code: string) => void; log?: HookBindingLog;
  }> = {},
): Promise<boolean> => {
  const input = eventSchema.parse(event);
  const log = options.log ?? noopLog;
  const deadline = Date.now() + (options.timeoutMs ?? 30_000);
  const signal = AbortSignal.timeout(options.timeoutMs ?? 30_000);
  let attempt = 0;
  let outcome: HookBindingOutcome = "timeout";
  while (!signal.aborted) {
    if (Date.now() >= deadline) { outcome = "timeout"; break; }
    attempt += 1;
    log.emit("hook_binding_attempt", { attempt });
    try {
      const result = await rpc.request("mcpServer/tool/call", {
        threadId: input.session_id, server: options.server ?? "gptqueue-shared", tool: "bind_runtime",
        arguments: { client: "codex", runtime_id: input.session_id, epoch: input.session_id, working_directory: input.cwd },
      }, signal);
      const content = Array.isArray(result.content) ? result.content : [];
      if (result.isError && content.some(item => item?.type === "text" &&
          typeof item.text === "string" && /Tool bind_runtime not found/u.test(item.text))) {
        options.onUnavailable?.("legacy_connection_requires_reconnect");
        outcome = "legacy_connection_requires_reconnect";
        log.emit("hook_binding_outcome", { attempt, outcome });
        return false;
      }
      const status = result.structuredContent as Record<string, unknown> | undefined;
      if (!result.isError && status?.activation_ready === true) {
        outcome = "ready";
        log.emit("hook_binding_outcome", { attempt, outcome });
        return true;
      }
      outcome = "not_ready";
    } catch {
      if (signal.aborted) { outcome = "timeout"; break; }
      outcome = "transport_error";
    }
    log.emit("hook_binding_outcome", { attempt, outcome });
    await delay(options.retryMs ?? 250, undefined, { signal }).catch(() => undefined);
  }
  if (signal.aborted || Date.now() >= deadline) outcome = "timeout";
  log.emit("hook_binding_outcome", { attempt, outcome });
  return false;
};

export const runCodexHook = async (): Promise<void> => {
  let input = "";
  for await (const chunk of process.stdin) {
    input += String(chunk);
    if (Buffer.byteLength(input) > 65_536) throw new Error("Hook input exceeds size limit");
  }
  let parsed: { session_id: string; hook_event_name: string } | undefined;
  try {
    parsed = eventSchema.parse(JSON.parse(input));
  } catch {
    // Identity never parsed: log without echoing any raw payload.
    const log = createHookLog("codex", "unparsed", "invalid");
    log.emit("hook_binding_outcome", { outcome: "invalid_hook_input" });
    await log.close();
    throw new Error("invalid_hook_or_transport");
  }
  const log = createHookLog("codex", parsed.session_id, parsed.hook_event_name);
  const rpc = new CodexSocketClient();
  try {
    log.emit("hook_binding_started", {});
    let code = "codex_binding_unavailable";
    if (!await bindCodexHook(parsed, rpc, { onUnavailable: value => { code = value; }, log })) {
      log.emit("hook_exit", { code });
      console.error('[gptqueue] ' + JSON.stringify({ event: 'runtime_binding_failed', code }));
      process.exitCode = 1;
    }
  } finally {
    try { await rpc.close(); } finally { await log.close(); }
  }
};
