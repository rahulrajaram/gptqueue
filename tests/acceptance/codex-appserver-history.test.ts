import { existsSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { readCodexAppserverHistory } from "./codex-appserver-history.js";

describe("Codex app-server acceptance history", () => {
  it("falls back to the exact rollout and retains MCP arguments and results", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptqueue-codex-history-test-"));
    const path = join(root, "rollout.jsonl");
    try {
      await writeFile(path, [
        JSON.stringify({ type: "session_meta", payload: { id: "thread-1", cwd: root } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "task_started" } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "item_completed", item: { id: "call-1", type: "McpToolCall", server: "gptqueue-shared", tool: "get_runtime_status", arguments: { exact: true }, result: { structuredContent: { status: "ok" } }, status: "COMPLETED" } } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "task_complete" } }),
      ].join("\n") + "\n");
      let count = 0;
      const rpc = { request: async (method: string, params: unknown) => {
        count += 1;
        if (method !== "thread/read") throw new Error("unexpected RPC");
        if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
        return { thread: { id: "thread-1", cwd: root, path } };
      }, close: async () => undefined };
      const history = await readCodexAppserverHistory(rpc, "thread-1", AbortSignal.timeout(5_000));
      const item = ((history.turns as Array<{ items: Array<Record<string, unknown>> }>)[0]!).items[0]!;
      expect(item).toMatchObject({ id: "call-1", type: "mcpToolCall", status: "completed", arguments: { exact: true }, result: { structuredContent: { status: "ok" } } });
      expect(count).toBe(2);
    } finally { await rm(root, { recursive: true, force: true }); }
  });

  it("keeps an incomplete trailing rollout frame for the next read", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptqueue-codex-history-test-"));
    const path = join(root, "rollout.jsonl");
    try {
      await writeFile(path, [
        JSON.stringify({ type: "session_meta", payload: { id: "thread-1", cwd: root } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "task_started" } }),
        '{"type":"event_msg","payload":{"thread_id":"thread-1","turn_id":"turn-1"',
      ].join("\n"));
      let count = 0;
      const rpc = { request: async (method: string) => {
        count += 1;
        if (method !== "thread/read") throw new Error("unexpected RPC");
        if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
        return { thread: { id: "thread-1", cwd: root, path } };
      }, close: async () => undefined };
      const history = await readCodexAppserverHistory(rpc, "thread-1", AbortSignal.timeout(5_000));
      expect(history.turns).toEqual([{ id: "turn-1", status: "inProgress", items: [] }]);
    } finally { await rm(root, { recursive: true, force: true }); }
  });

  it("joins only observed MCP items to matching response item call and output records", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptqueue-codex-history-test-"));
    const path = join(root, "rollout.jsonl");
    try {
      await writeFile(path, [
        JSON.stringify({ type: "session_meta", payload: { id: "thread-1", cwd: root } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "task_started" } }),
        JSON.stringify({ type: "response_item", payload: { type: "function_call", call_id: "call-1", name: "gptqueue_get_runtime_status", arguments: "{\"exact\":true}" } }),
        JSON.stringify({ type: "event_msg", payload: { thread_id: "thread-1", turn_id: "turn-1", type: "item_completed", item: { id: "call-1", type: "McpToolCall", server: "gptqueue-shared", tool: "get_runtime_status", status: "completed" } } }),
        JSON.stringify({ type: "response_item", payload: { type: "function_call_output", call_id: "call-1", output: "{\"structuredContent\":{\"status\":\"ok\"}}" } }),
      ].join("\n") + "\n");
      let count = 0;
      const rpc = { request: async (method: string) => {
        count += 1;
        if (method !== "thread/read") throw new Error("unexpected RPC");
        if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
        return { thread: { id: "thread-1", cwd: root, path } };
      }, close: async () => undefined };
      const history = await readCodexAppserverHistory(rpc, "thread-1", AbortSignal.timeout(5_000));
      const item = ((history.turns as Array<{ items: Array<Record<string, unknown>> }>)[0]!).items[0]!;
      expect(item).toMatchObject({ id: "call-1", arguments: { exact: true }, result: { structuredContent: { status: "ok" } } });
    } finally { await rm(root, { recursive: true, force: true }); }
  });

  it("reads the retained failed cross-model rollout with exact session metadata", async () => {
    const path = "/home/rahul/.codex/archived_sessions/rollout-2026-09-12T17-34-52-01a0978b-6f90-7d80-90d2-ead886e09c5b.jsonl";
    if (!existsSync(path)) return;
    const threadId = "01a0978b-6f90-7d80-90d2-ead886e09c5b";
    const cwd = "/tmp/gptqueue-qualification-cross-model-qSJ5sY/codex/49022be8dfbc13d2/codex-appserver/sender/87dc614676c2a976";
    let count = 0;
    const rpc = { request: async (method: string) => {
      count += 1;
      if (method !== "thread/read") throw new Error("unexpected RPC");
      if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
      return { thread: { id: threadId, cwd, path } };
    }, close: async () => undefined };
    const history = await readCodexAppserverHistory(rpc, threadId, AbortSignal.timeout(5_000));
    expect(history.id).toBe(threadId);
    expect(history.cwd).toBe(cwd);
    expect((history.turns as Array<{ items: Array<Record<string, unknown>> }>).flatMap((turn) => turn.items).some((item) => item.type === "mcpToolCall")).toBe(false);
    expect(count).toBe(2);
  });

  it("reads the retained successful Codex app-server rollout with native MCP payloads", async () => {
    const path = "/home/rahul/.codex/archived_sessions/rollout-2026-09-12T16-59-21-01a0976a-e880-7270-b0ee-2b5c19d1f949.jsonl";
    if (!existsSync(path)) return;
    const threadId = "01a0976a-e880-7270-b0ee-2b5c19d1f949";
    const cwd = "/tmp/gptqueue-qualification-codex-appserver-fE7BTC/56cf98d932ca9244/codex-appserver/sender/70fed238cff30280";
    let count = 0;
    const rpc = { request: async (method: string) => {
      count += 1;
      if (method !== "thread/read") throw new Error("unexpected RPC");
      if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
      return { thread: { id: threadId, cwd, path } };
    }, close: async () => undefined };
    const history = await readCodexAppserverHistory(rpc, threadId, AbortSignal.timeout(5_000));
    const calls = (history.turns as Array<{ items: Array<Record<string, unknown>> }>).flatMap((turn) => turn.items).filter((item) => item.type === "mcpToolCall");
    expect(calls.length).toBeGreaterThan(0);
    expect(calls.some((item) => item.tool === "get_runtime_status" && item.arguments !== undefined && item.result !== undefined && item.status === "completed")).toBe(true);
    expect(count).toBe(2);
  });

  it("rejects a rollout whose session metadata belongs to another thread", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptqueue-codex-history-test-"));
    const path = join(root, "rollout.jsonl");
    try {
      await writeFile(path, JSON.stringify({ type: "session_meta", payload: { id: "foreign", cwd: root } }) + "\n");
      let count = 0;
      const rpc = { request: async (method: string) => {
        count += 1;
        if (method !== "thread/read") throw new Error("unexpected RPC");
        if (count === 1) throw new Error('Codex RPC rejected: {"code":-32601,"message":"list_turns is not supported yet"}');
        return { thread: { id: "thread-1", cwd: root, path } };
      }, close: async () => undefined };
      await expect(readCodexAppserverHistory(rpc, "thread-1", AbortSignal.timeout(5_000))).rejects.toThrow("identity does not match");
    } finally { await rm(root, { recursive: true, force: true }); }
  });
});
