import { z } from "zod";
import type { Redis } from "ioredis";
import { setTimeout as delay } from "node:timers/promises";
import { isAbsolute } from "node:path";
import { CodexSocketClient } from "./codex-socket.js";
import { createHookLog, type HookBindingLog } from "./lifecycle-log.js";
import type { CodexRpcClient } from "./codex-runtime.js";
import { runCodexStop } from "./codex-stop-gate.js";
import { probeClient } from "../claude/inbox-probe.js";
import { SESSION_KEYS } from "../core/keys.js";
import { TurnStateStore } from "../core/turn-state.js";

const eventSchema = z.object({
  session_id: z.string().uuid(),
  cwd: z.string().refine(isAbsolute),
  hook_event_name: z.enum(["SessionStart", "UserPromptSubmit", "Stop"]),
  stop_hook_active: z.boolean().optional(),
  turn_id: z.string().optional(),
});
type CodexHookEvent = z.infer<typeof eventSchema>;

/**
 * A bound thread's new prompt starts a turn: record the agent as running. Best
 * effort. `at` is the prompt event's time, captured when the hook process
 * started and before binding (S2): this hook runs asynchronously, so it can
 * finish after the turn's Stop recorded idle, and a later timestamp here would
 * wrongly flip a finished turn back to running.
 */
export const recordRunning = async (event: CodexHookEvent, at: string, client?: Redis): Promise<void> => {
  const redis = client ?? probeClient();
  try {
    const raw = await redis.get(SESSION_KEYS.runtimeMailbox("codex", event.session_id));
    const agent = raw === null ? null : (JSON.parse(raw) as { agent?: unknown }).agent;
    if (typeof agent !== "string" || agent.length === 0) return;
    await new TurnStateStore(redis).report(agent, {
      phase: "running", at, runtime: "codex", session_id: event.session_id, turn_id: event.turn_id ?? null,
    });
  } finally {
    if (client === undefined) redis.disconnect();
  }
};

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
  const startedAt = new Date().toISOString(); // the event's time, before any binding work (S2)
  let input = "";
  for await (const chunk of process.stdin) {
    input += String(chunk);
    if (Buffer.byteLength(input) > 65_536) throw new Error("Hook input exceeds size limit");
  }
  let parsed: CodexHookEvent | undefined;
  try {
    parsed = eventSchema.parse(JSON.parse(input));
  } catch {
    // Identity never parsed: log without echoing any raw payload.
    const log = createHookLog("codex", "unparsed", "invalid");
    log.emit("hook_binding_outcome", { outcome: "invalid_hook_input" });
    await log.close();
    throw new Error("invalid_hook_or_transport");
  }
  if (parsed.hook_event_name === "Stop") {
    // Synchronous, per turn, on every daemon thread: no binding work and no per-turn log file.
    // A hung Redis must not hold the turn: every command gives up after 3 s and the gate fails open.
    const redis = probeClient(undefined, { commandTimeoutMs: 3000 });
    try {
      process.stdout.write(await runCodexStop(parsed, redis));
    } finally {
      redis.disconnect();
    }
    return;
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
    } else if (parsed.hook_event_name === "UserPromptSubmit") {
      await recordRunning(parsed, startedAt).catch(() => log.emit("turn_state_unrecorded", {}));
    }
  } finally {
    try { await rpc.close(); } finally { await log.close(); }
  }
};
