import { appendFile, mkdtemp, rm, truncate, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { afterEach, describe, expect, it } from "vitest";
import { CodexThreadReader, userMessageMatches } from "../src/registered-shell/codex-history.js";

const dirs: string[] = [];
const binding = (path: string, id = "thread-1") => ({ id, path, cwd: "/workspace" });
const rpc = (responses: Array<unknown>) => ({ calls: [] as Array<{ method: string; params: unknown }>, request: async function (method: string, params: unknown) { this.calls.push({ method, params }); const value = responses.length > 1 ? responses.shift() : responses[0]; if (value instanceof Error) throw value; return value; } });
const record = (value: unknown) => `${JSON.stringify(value)}\n`;
const meta = (thread = "thread-1", cwd = "/workspace") => record({ type: "session_meta", payload: { id: thread, cwd } });
const event = (turn: string, type: string, item?: Record<string, unknown>) => record({ type: "event_msg", payload: { turn_id: turn, type, ...(item ? { item } : {}) } });
const item = (id: string, type: string, content: unknown[] = [{ type: "Text", text: "hello" }]) => ({ id, type, content });

afterEach(async () => { await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true }))); });

describe("Codex thread history", () => {
  it("uses native history when supported and passes clientId through", async () => {
    const client = rpc([{ thread: { id: "thread-1", turns: [{ id: "turn-1" }] } }]);
    const result = await new CodexThreadReader(client as never).read("thread-1", new AbortController().signal);
    expect(result).toEqual({ id: "thread-1", turns: [{ id: "turn-1" }] });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0]?.method).toBe("thread/read");
    expect(client.calls[0]?.params).toEqual({ threadId: "thread-1", includeTurns: true });
  });

  it("falls back to the exact verified transcript identity and projects incremental items", async () => {
    const dir = await mkdtemp(join(tmpdir(), "codex-history-")); dirs.push(dir); const path = join(dir, "thread.jsonl");
    await writeFile(path, meta() + event("turn-1", "task_started") + event("turn-1", "item_completed", item("u", "UserMessage")) + event("turn-1", "item_completed", item("a", "AgentMessage")) + event("turn-1", "item_completed", item("m", "McpToolCall")) + event("turn-1", "task_complete"));
    const client = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]); const reader = new CodexThreadReader(client as never);
    const result = await reader.read("thread-1", new AbortController().signal);
    expect(result.turns).toEqual([{ id: "turn-1", status: "completed", items: [
      { type: "userMessage", id: "u", content: [{ type: "text", text: "hello" }], clientId: undefined },
      { type: "agentMessage", id: "a", text: "hello" }, { type: "mcpToolCall", id: "m", server: undefined, tool: undefined, status: "undefined" },
    ] }]);
    expect(client.calls[1]?.params).toEqual({ threadId: "thread-1", includeTurns: false });
  });

  it("rejects foreign identity, cwd mismatch, truncation, and malformed records", async () => {
    const dir = await mkdtemp(join(tmpdir(), "codex-history-")); dirs.push(dir); const path = join(dir, "thread.jsonl");
    await writeFile(path, meta("foreign") ); const client = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]);
    await expect(new CodexThreadReader(client as never).read("thread-1", new AbortController().signal)).rejects.toThrow(/identity/u);
    await writeFile(path, meta("thread-1", "/other")); const cwdClient = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]);
    await expect(new CodexThreadReader(cwdClient as never).read("thread-1", new AbortController().signal)).rejects.toThrow(/identity/u);
    await writeFile(path, meta() + "{\"type\":\"event_msg\",\"payload\":"); const partialClient = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]);
    const partialReader = new CodexThreadReader(partialClient as never);
    const partial = await partialReader.read("thread-1", new AbortController().signal);
    expect(partial.turns).toEqual([]); await appendFile(path, "{\"turn_id\":\"t\",\"type\":\"task_started\"}}\n");
    expect((await partialReader.read("thread-1", new AbortController().signal)).turns?.[0]?.status).toBe("inProgress");
    const malformed = join(dir, "malformed.jsonl"); await writeFile(malformed, meta() + "not-json\n");
    const malformedClient = rpc([new Error("list_turns is not supported"), { thread: binding(malformed) }]);
    await expect(new CodexThreadReader(malformedClient as never).read("thread-1", new AbortController().signal)).rejects.toThrow(/Invalid/u);
    const truncRpc = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]);
    const truncReader = new CodexThreadReader(truncRpc as never); await truncReader.read("thread-1", new AbortController().signal);
    await truncate(path, 0); await expect(truncReader.read("thread-1", new AbortController().signal)).rejects.toThrow(/changed unexpectedly|identity/u);
  });

  it("caches appended complete lines without duplicates and matches whole prompts only", async () => {
    const dir = await mkdtemp(join(tmpdir(), "codex-history-")); dirs.push(dir); const path = join(dir, "thread.jsonl"); await writeFile(path, meta() + event("t", "task_started"));
    const client = rpc([new Error("list_turns is not supported"), { thread: binding(path) }]); const reader = new CodexThreadReader(client as never); const signal = new AbortController().signal;
    expect((await reader.read("thread-1", signal)).turns).toEqual([{ id: "t", status: "inProgress", items: [] }]);
    await appendFile(path, event("t", "item_completed", item("u", "UserMessage", [{ type: "Text", text: "prompt" }])));
    const twice = await reader.read("thread-1", signal); expect(twice.turns[0]?.items).toHaveLength(1); expect((await reader.read("thread-1", signal)).turns[0]?.items).toHaveLength(1);
    const prompt = "full controller prompt";
    expect(userMessageMatches({ type: "userMessage", content: [{ type: "text", text: prompt }] }, "operation", prompt)).toBe(true);
    expect(userMessageMatches({ type: "userMessage", content: [{ type: "text", text: `prefix ${prompt}` }] }, "operation", prompt)).toBe(false);
    expect(userMessageMatches({ type: "assistantMessage", content: [{ type: "text", text: prompt }] }, "operation", prompt)).toBe(false);
    expect(userMessageMatches({ type: "userMessage", clientId: "operation" }, "operation", prompt)).toBe(true);
  });
});
