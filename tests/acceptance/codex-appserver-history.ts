import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { isAbsolute } from "node:path";
import type { CodexRpcClient } from "../../src/registered-shell/codex-runtime.js";

type Json = Record<string, unknown>;
type RolloutThread = Json & Readonly<{ id: string; cwd: string; path: string }>;
type RolloutRecord = Readonly<{ type?: unknown; payload?: unknown }>;
type NativeFragment = { arguments?: unknown; result?: unknown };

const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const abortError = (signal: AbortSignal): Error => signal.reason instanceof Error ? signal.reason : new Error("Codex history read aborted");
const checkAbort = (signal: AbortSignal): void => { if (signal.aborted) throw abortError(signal); };
const unsupportedTurns = (error: unknown): boolean => String(error).includes("list_turns is not supported");
const canonicalItemType = (value: unknown): unknown => {
  if (value === "McpToolCall") return "mcpToolCall";
  if (value === "AgentMessage") return "agentMessage";
  if (value === "UserMessage") return "userMessage";
  return value;
};
const canonicalItem = (item: Json): Json => Object.freeze({
  ...item,
  type: canonicalItemType(item.type),
  ...(typeof item.status === "string" ? { status: item.status.toLowerCase() } : {}),
}) as Json;
const decodeJson = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value) as unknown; } catch { return value; }
};

const readRollout = async (thread: RolloutThread, signal: AbortSignal): Promise<Json> => {
  const file = await open(thread.path, constants.O_RDONLY | constants.O_NOFOLLOW);
  const turns = new Map<string, { id: string; status: string; items: Json[] }>();
  const fragments = new Map<string, NativeFragment>();
  const locations = new Map<string, Readonly<{ turnId: string; index: number }>>();
  let pending = Buffer.alloc(0);
  let verified = false;
  let offset = 0;
  try {
    const info = await file.stat();
    if (!info.isFile()) throw new Error("Codex rollout path is not a regular file");
    const buffer = Buffer.alloc(64 * 1024);
    const consume = (line: string): void => {
      let record: RolloutRecord;
      try { record = JSON.parse(line) as RolloutRecord; } catch { throw new Error("Invalid Codex rollout record"); }
      const payload = object(record.payload);
      if (!payload) return;
      if (!verified) {
        if (record.type !== "session_meta" || payload.id !== thread.id || payload.cwd !== thread.cwd) throw new Error("Codex rollout identity does not match exact thread");
        verified = true;
        return;
      }
      if (record.type === "response_item") {
        const callId = payload.call_id;
        if (!nonEmpty(callId)) return;
        const fragment = fragments.get(callId) ?? {};
        if (payload.type === "function_call" && payload.arguments !== undefined) fragment.arguments = decodeJson(payload.arguments);
        if (payload.type === "function_call_output" && payload.output !== undefined) fragment.result = decodeJson(payload.output);
        fragments.set(callId, fragment);
        const location = locations.get(callId);
        if (location) {
          const turn = turns.get(location.turnId);
          const current = turn?.items[location.index];
          if (turn && current) {
            const merged = canonicalItem({ ...current, ...(current.arguments === undefined && fragment.arguments !== undefined ? { arguments: fragment.arguments } : {}), ...(current.result === undefined && fragment.result !== undefined ? { result: fragment.result } : {}) });
            turn.items = [...turn.items.slice(0, location.index), merged, ...turn.items.slice(location.index + 1)];
          }
        }
        return;
      }
      if (record.type !== "event_msg") return;
      if (payload.thread_id !== undefined && payload.thread_id !== thread.id) throw new Error("Foreign event in Codex rollout");
      const turnId = payload.turn_id;
      if (!nonEmpty(turnId)) return;
      const turn = turns.get(turnId) ?? { id: turnId, status: "inProgress", items: [] };
      switch (payload.type) {
        case "task_started":
        case "turn_started":
          turn.status = "inProgress";
          break;
        case "task_complete":
        case "turn_completed":
          turn.status = payload.error ? "failed" : "completed";
          break;
        case "turn_aborted":
          turn.status = "interrupted";
          break;
        case "item_started":
        case "item_completed": {
          const item = object(payload.item);
          if (!item || !nonEmpty(item.id)) throw new Error("Codex rollout item has no exact source ID");
          const fragment = fragments.get(item.id);
          const retained = canonicalItem({
            ...item,
            ...(item.arguments === undefined && fragment?.arguments !== undefined ? { arguments: fragment.arguments } : {}),
            ...(item.result === undefined && fragment?.result !== undefined ? { result: fragment.result } : {}),
          });
          turn.items = [...turn.items.filter((current) => current.id !== retained.id), retained];
          locations.set(retained.id as string, { turnId, index: turn.items.length - 1 });
          break;
        }
        default:
          break;
      }
      turns.set(turnId, turn);
    };
    while (offset < info.size) {
      checkAbort(signal);
      const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, info.size - offset), offset);
      if (!bytesRead) break;
      offset += bytesRead;
      pending = Buffer.concat([pending, buffer.subarray(0, bytesRead)]);
      let end = pending.indexOf(10);
      while (end >= 0) {
        const line = pending.subarray(0, end).toString("utf8");
        pending = pending.subarray(end + 1);
        if (line.length > 0) consume(line);
        end = pending.indexOf(10);
      }
      if (pending.length > 32 * 1024 * 1024) throw new Error("Codex rollout record exceeds size limit");
    }
    // A live rollout may end between writes. Leave an unterminated frame for
    // the next bounded read; only newline-terminated JSON is admissible.
    if (!verified) throw new Error("Codex rollout identity is not ready");
    return Object.freeze({ ...thread, turns: Object.freeze([...turns.values()].map((turn) => Object.freeze({ ...turn, items: Object.freeze([...turn.items]) }))) });
  } finally {
    await file.close();
  }
};

const threadFromResponse = (value: unknown): Json => {
  const response = object(value);
  const thread = object(response?.thread ?? response);
  if (!thread) throw new Error("Codex thread/read returned no thread");
  return thread;
};

/** Read complete native Codex history, including MCP arguments/results on older app-server builds. */
export const readCodexAppserverHistory = async (rpc: CodexRpcClient, threadId: string, signal: AbortSignal): Promise<Json> => {
  try {
    const thread = threadFromResponse(await rpc.request("thread/read", { threadId, includeTurns: true }, signal));
    if (thread.id !== threadId) throw new Error("Codex returned a different thread");
    return thread;
  } catch (error) {
    if (!unsupportedTurns(error)) throw error;
    const thread = threadFromResponse(await rpc.request("thread/read", { threadId, includeTurns: false }, signal));
    if (thread.id !== threadId || !nonEmpty(thread.cwd) || !nonEmpty(thread.path) || !isAbsolute(thread.path)) throw new Error("Codex fallback thread metadata is not exact");
    return readRollout(Object.freeze({ ...thread, id: thread.id, cwd: thread.cwd, path: thread.path }), signal);
  }
};
