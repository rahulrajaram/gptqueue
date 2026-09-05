import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { isAbsolute } from "node:path";
import type { CodexRpcClient } from "./codex-runtime.js";

type Item = Record<string, unknown>;
type Turn = { id: string; status: string; items: Item[] };

/** Incremental projection of the exact daemon-returned transcript on older daemons. */
class RolloutHistory {
  private path = "";
  private offset = 0;
  private pending: Buffer = Buffer.alloc(0);
  private verified = false;
  private invalid = false;
  private readonly turns = new Map<string, Turn>();

  private consume(raw: string, thread: Record<string, unknown>): void {
    let record: Record<string, unknown>;
    try { record = JSON.parse(raw); } catch { throw new Error("Invalid Codex transcript record"); }
    const payload = record.payload as Record<string, unknown> | undefined;
    if (!payload) return;
    if (!this.verified) {
      if (record.type !== "session_meta" || payload.id !== thread.id || payload.cwd !== thread.cwd) {
        throw new Error("Codex transcript identity does not match exact thread");
      }
      this.verified = true;
      return;
    }
    if (record.type !== "event_msg" || typeof payload.turn_id !== "string") return;
    if (payload.thread_id !== undefined && payload.thread_id !== thread.id) throw new Error("Foreign event in Codex transcript");
    const id = payload.turn_id;
    const turn = this.turns.get(id) ?? { id, status: "inProgress", items: [] };
    switch (payload.type) {
      case "task_started": turn.status = "inProgress"; break;
      case "task_complete": turn.status = payload.error ? "failed" : "completed"; break;
      case "turn_aborted": turn.status = "interrupted"; break;
      case "item_completed": {
        const item = payload.item as Item | undefined;
        if (!item || typeof item.id !== "string") break;
        const content = Array.isArray(item.content) ? item.content.map((value) =>
          value?.type === "Text" ? { ...value, type: "text" } : value) : [];
        const text = content.filter((value) => value?.type === "text").map((value) => value.text ?? "").join("");
        const projected = item.type === "UserMessage" ? { type: "userMessage", id: item.id, content, clientId: item.client_id }
          : item.type === "AgentMessage" ? { type: "agentMessage", id: item.id, text }
          : item.type === "McpToolCall" ? { type: "mcpToolCall", id: item.id, server: item.server, tool: item.tool,
              status: String(item.status).toLowerCase() }
          : null;
        if (projected) turn.items = [...turn.items.filter((value) => value.id !== item.id), projected].slice(-128);
        break;
      }
      default: return;
    }
    this.turns.set(id, turn);
    if (this.turns.size > 256) this.turns.delete(this.turns.keys().next().value!);
  }

  async read(thread: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (typeof thread.path !== "string" || !isAbsolute(thread.path)) throw new Error("Codex transcript path unavailable");
    if (this.path !== thread.path) {
      this.path = thread.path; this.offset = 0; this.pending = Buffer.alloc(0); this.verified = false; this.invalid = false; this.turns.clear();
    }
    if (this.invalid) throw new Error("Invalid Codex transcript requires a new runtime reader");
    const file = await open(this.path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = await file.stat();
      if (!info.isFile() || info.size < this.offset) throw new Error("Codex transcript changed unexpectedly");
      const buffer = Buffer.alloc(64 * 1024);
      while (this.offset < info.size) {
        const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, info.size - this.offset), this.offset);
        if (!bytesRead) break;
        this.offset += bytesRead;
        this.pending = Buffer.concat([this.pending, buffer.subarray(0, bytesRead)]);
        let end: number;
        while ((end = this.pending.indexOf(10)) >= 0) {
          const line = this.pending.subarray(0, end).toString("utf8");
          this.pending = this.pending.subarray(end + 1);
          if (line) this.consume(line, thread);
        }
        if (this.pending.length > 32 * 1024 * 1024) throw new Error("Codex transcript record exceeds size limit");
      }
      if (!this.verified) throw new Error("Codex transcript identity is not ready");
      return { ...thread, turns: [...this.turns.values()] };
    } catch (error) {
      this.invalid = true;
      throw error;
    } finally { await file.close(); }
  }
}

export class CodexThreadReader {
  private historySupported = true;
  private readonly rollout = new RolloutHistory();
  constructor(private readonly rpc: CodexRpcClient) {}

  async read(threadId: string, signal: AbortSignal): Promise<Record<string, unknown>> {
    if (this.historySupported) {
      try {
        const result = await this.rpc.request("thread/read", { threadId, includeTurns: true }, signal);
        return (result.thread ?? result) as Record<string, unknown>;
      } catch (error) {
        if (!String(error).includes("list_turns is not supported")) throw error;
        this.historySupported = false;
      }
    }
    const result = await this.rpc.request("thread/read", { threadId, includeTurns: false }, signal);
    const thread = result.thread as Record<string, unknown>;
    if (!thread || thread.id !== threadId) throw new Error("Codex returned a different thread");
    return this.rollout.read(thread);
  }
}

export const userMessageMatches = (item: Item, operationId: string, prompt: string): boolean => {
  if (item.type !== "userMessage") return false;
  if (item.clientId === operationId) return true;
  // The older local rollout omits clientId. Match the entire controller-generated prompt.
  return Array.isArray(item.content) && item.content.length === 1 &&
    item.content[0]?.type === "text" && item.content[0]?.text === prompt;
};
