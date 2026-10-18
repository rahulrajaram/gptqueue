import { afterEach, describe, expect, it, vi } from "vitest";
import { createCodexRuntime, CODEX_TURN_WINDOW, type CodexNotification, type CodexRpcClient } from "../src/registered-shell/codex-runtime.js";

const binding = { client: "codex" as const, runtime_id: "thread-1", epoch: "e1", working_directory: "/workspace" };
const fake = (responses: Record<string, Record<string, unknown>>): CodexRpcClient & { calls: string[] } => {
  const value = { calls: [] as string[], request: async (method: string) => { value.calls.push(method); return responses[method] ?? {}; }, close: async () => {} };
  return value;
};

describe("Codex runtime", () => {
  it("validates exact thread and starts an idle turn", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "turn/start": { turn: { id: "turn-1" } } });
    const runtime = await createCodexRuntime(binding, { client }); const result = await runtime.activate({ operation_id: "op-1", prompt: "hello" }, new AbortController().signal);
    expect(result).toEqual({ status: "started", turn_id: "turn-1" }); expect(client.calls).toEqual(["thread/read", "thread/read", "thread/turns/list", "thread/read", "turn/start"]);
  });
  it("returns busy and never steers an active thread", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "t", status: "inProgress", items: [] }] } }); const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-2", prompt: "hello" }, new AbortController().signal)).toEqual({ status: "busy" }); expect(client.calls).not.toContain("turn/start");
  });
  it("rejects a mismatched exact thread", async () => {
    await expect(createCodexRuntime(binding, { client: fake({ "thread/read": { id: "thread-1", cwd: "/other" } }) })).rejects.toThrow(/identity/);
  });
  it("requires the exact GPTQueue owner for an expected agent", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "mcpServer/tool/call": { structuredContent: { agent: "agent-a" } } });
    await expect(createCodexRuntime(binding, { client, expectedAgent: "agent-a" })).resolves.toBeDefined();
    await expect(createCodexRuntime(binding, { client: fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "mcpServer/tool/call": { structuredContent: { agent: "agent-b" } } }), expectedAgent: "agent-a" })).rejects.toThrow(/own this GPTQueue/);
  });
  it("recovers terminal history without replaying and honors recover_only", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "turn-old", status: "failed", items: [{ type: "userMessage", clientId: "op-old", content: [{ type: "text", text: "old" }] }] }] } });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-old", prompt: "old" }, new AbortController().signal)).toEqual({ status: "completed", turn_id: "turn-old" });
    expect(await runtime.activate({ operation_id: "op-new", prompt: "new", recover_only: true }, new AbortController().signal)).toEqual({ status: "ambiguous" });
    expect(client.calls).not.toContain("turn/start");
  });
  it("deduplicates an existing client operation", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "turn-existing", status: "inProgress", items: [{ type: "userMessage", clientId: "op-1", content: [{ type: "text", text: "hello" }] }] }] } });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-1", prompt: "hello" }, new AbortController().signal)).toEqual({ status: "started", turn_id: "turn-existing" });
    expect(client.calls).not.toContain("turn/start");
  });
});

/**
 * Faithful to `codex app-server generate-json-schema` (codex-cli 0.160.0), v2/:
 * Thread.status is ThreadStatus {type: notLoaded|idle|systemError|active}; Turn has
 * {id, status: completed|interrupted|failed|inProgress, items, itemsView: notLoaded|summary|full};
 * thread/turns/list -> {data: Turn[], nextCursor, backwardsCursor} (default descending);
 * thread/items/list -> {data: ThreadItemEntry{turnId, item}[], nextCursor};
 * UserMessage item {type: "userMessage", id, clientId?, content}.
 */
type V2Turn = { id: string; status: "completed" | "interrupted" | "failed" | "inProgress"; items: Record<string, unknown>[] };
const v2 = (turns: V2Turn[], status: Record<string, unknown> = { type: "idle" }) => {
  const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
  const listeners = new Set<(n: CodexNotification) => void>();
  const client: CodexRpcClient & { calls: typeof calls; emit(n: CodexNotification): void } = {
    calls,
    emit: (n) => { for (const l of listeners) l(n); },
    onNotification: (l) => { listeners.add(l); return () => listeners.delete(l); },
    close: async () => {},
    request: async (method, params) => {
      calls.push({ method, params });
      switch (method) {
        case "thread/read":
          if (params.includeTurns) throw new Error("full history must not be read");
          return { thread: { id: "thread-1", cwd: "/workspace", status, turns: [] } };
        case "thread/turns/list": {
          const newest = [...turns].reverse();
          const limit = Number(params.limit);
          return { data: newest.slice(0, limit).map((t) => ({ id: t.id, status: t.status, items: [], itemsView: "notLoaded" })),
            nextCursor: newest.length > limit ? "cursor" : null, backwardsCursor: null };
        }
        case "thread/items/list": {
          const turn = turns.find((t) => t.id === params.turnId)!;
          return { data: turn.items.slice(0, Number(params.limit)).map((item) => ({ turnId: turn.id, item })), nextCursor: null };
        }
        case "thread/resume": return { thread: { id: "thread-1" } };
        case "turn/start": return { turn: { id: "turn-new", status: "inProgress", items: [] } };
        default: return {};
      }
    },
  };
  return client;
};
const user = (clientId: string, text = "x") => ({ type: "userMessage", id: `u-${clientId}`, clientId, content: [{ type: "text", text }] });
const filler = (n: number): V2Turn[] => Array.from({ length: n }, (_, i) => ({ id: `old-${i}`, status: "completed", items: [user(`other-${i}`), { type: "agentMessage", id: `a-${i}`, text: "y".repeat(1000) }] }));
const sig = () => new AbortController().signal;

describe("Codex runtime, bounded v2 history", () => {
  afterEach(() => { vi.useRealTimers(); });
  it("does not go ambiguous on long or summarized history and reads only a bounded window", async () => {
    const client = v2(filler(500));
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-new", prompt: "p" }, sig())).toEqual({ status: "started", turn_id: "turn-new" });
    expect(client.calls.filter((c) => c.method === "thread/items/list")).toHaveLength(CODEX_TURN_WINDOW);
    expect(client.calls.find((c) => c.method === "thread/turns/list")?.params).toMatchObject({ itemsView: "notLoaded", sortDirection: "desc" });
    expect(client.calls.every((c) => c.method !== "thread/read" || c.params.includeTurns === false)).toBe(true);
  });
  it("keeps dedupe: a recent prior submission is found by clientId and never resubmitted", async () => {
    const turns = [...filler(300), { id: "turn-mine", status: "completed" as const, items: [user("op-1", "hello")] }, ...filler(3).map((t) => ({ ...t, id: `n-${t.id}` }))];
    const runtime = await createCodexRuntime(binding, { client: v2(turns) });
    const client = v2(turns); const again = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-1", prompt: "hello", recover_only: true }, sig())).toEqual({ status: "completed", turn_id: "turn-mine" });
    expect(await again.activate({ operation_id: "op-1", prompt: "hello" }, sig())).toEqual({ status: "completed", turn_id: "turn-mine" });
    expect(client.calls.map((c) => c.method)).not.toContain("turn/start");
    // Outside the window with no match, recovery stays conservative.
    expect(await runtime.activate({ operation_id: "op-missing", prompt: "z", recover_only: true }, sig())).toEqual({ status: "ambiguous" });
  });
  it("reports busy for an in-progress turn or active thread", async () => {
    expect(await (await createCodexRuntime(binding, { client: v2([...filler(2), { id: "t", status: "inProgress", items: [user("someone")] }]) })).activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
    expect(await (await createCodexRuntime(binding, { client: v2(filler(1), { type: "active", activeFlags: [] }) })).activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
  });
  it("still delivers to a notLoaded (detached TUI) thread headless and reports it truthfully", async () => {
    const unloaded = v2(filler(1), { type: "notLoaded" });
    const runtime = await createCodexRuntime(binding, { client: unloaded });
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "started", turn_id: "turn-new" });
    expect(unloaded.calls.map((c) => c.method)).toContain("turn/start");
    expect(runtime.describe!()).toEqual({ host_state: "notLoaded", attached: false, stale: false });
  });
  it("defers delivery to a systemError thread but never retires it; only thread/closed does", async () => {
    // A long systemError run once retired a live lead's sidecar, closing its GPTQueue MCP;
    // the thread recovered on its next turn without its tools.
    vi.useFakeTimers({ toFake: ["Date"] });
    const status: Record<string, unknown> = { type: "systemError" };
    const client = v2([], status);
    const runtime = await createCodexRuntime(binding, { client });
    let stale = 0; runtime.onStale!(() => { stale++; });
    const go = (operation_id = "op") => runtime.activate({ operation_id, prompt: "p" }, sig());
    // Span ten minutes of observations: no count or duration of systemError retires the thread.
    for (let i = 0; i < 10; i++) { expect(await go()).toEqual({ status: "unavailable" }); vi.setSystemTime(Date.now() + 60_000); }
    expect(stale).toBe(0);
    expect(client.calls.map((c) => c.method)).not.toContain("turn/start");
    expect(runtime.describe!()).toEqual({ host_state: "systemError", attached: false, stale: false });
    status.type = "idle";
    expect(await go()).toEqual({ status: "started", turn_id: "turn-new" });
    client.emit({ method: "thread/closed", params: { threadId: "thread-1" } });
    expect(stale).toBe(1);
    expect(await go("op-after-close")).toEqual({ status: "unavailable" });
  });
  it("fires the idle-again signal on turn/completed for its own thread only, after subscribing while busy", async () => {
    const client = v2([{ id: "t", status: "inProgress", items: [user("someone")] }]);
    const runtime = await createCodexRuntime(binding, { client });
    let idle = 0; runtime.onIdle!(() => { idle++; });
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
    expect(client.calls.find((c) => c.method === "thread/resume")?.params).toEqual({ threadId: "thread-1", excludeTurns: true });
    client.emit({ method: "turn/completed", params: { threadId: "other", turn: { id: "x", status: "completed", items: [] } } });
    expect(idle).toBe(0);
    client.emit({ method: "turn/completed", params: { threadId: "thread-1", turn: { id: "t", status: "completed", items: [] } } });
    client.emit({ method: "thread/status/changed", params: { threadId: "thread-1", status: { type: "idle" } } });
    expect(idle).toBe(2);
  });
  it("re-sends thread/resume after the app-server connection is replaced, and its idle notification wakes", async () => {
    const base = v2([{ id: "t", status: "inProgress", items: [user("someone")] }]);
    let generation = 1;
    const client: CodexRpcClient & { calls: typeof base.calls; emit: typeof base.emit } = {
      calls: base.calls, emit: base.emit, onNotification: base.onNotification!, close: base.close,
      request: base.request,
      get connectionGeneration() { return generation; },
    };
    const runtime = await createCodexRuntime(binding, { client });
    let idle = 0; runtime.onIdle!(() => { idle++; });
    const resumes = () => client.calls.filter((c) => c.method === "thread/resume").length;
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
    expect(resumes()).toBe(1);
    generation = 2; // the socket dropped after the first resume and reconnected
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "busy" });
    expect(resumes()).toBe(2);
    client.emit({ method: "turn/completed", params: { threadId: "thread-1", turn: { id: "t", status: "completed", items: [] } } });
    expect(idle).toBe(1);
  });
  it("marks itself stale on thread/closed and stops activating", async () => {
    const client = v2([]);
    const runtime = await createCodexRuntime(binding, { client });
    let stale = 0; runtime.onStale!(() => { stale++; });
    client.emit({ method: "thread/closed", params: { threadId: "thread-1" } });
    client.emit({ method: "thread/closed", params: { threadId: "thread-1" } });
    expect(stale).toBe(1);
    expect(await runtime.activate({ operation_id: "op", prompt: "p" }, sig())).toEqual({ status: "unavailable" });
    expect(client.calls.map((c) => c.method)).not.toContain("turn/start");
  });
});


// Exact rejections from the live Codex 0.160.1 probe (live/codex-history-probe.txt
// and the codex-2026-10-06T03-17-00-951Z receipt).
const rejected = (code: number, message: string) => new Error(`Codex RPC rejected: ${JSON.stringify({ code, message })}`);
const NOT_MATERIALIZED = (id: string) => rejected(-32600, `thread ${id} is not materialized yet; thread/turns/list is unavailable before first user message`);
const EPHEMERAL = rejected(-32600, "ephemeral threads do not support includeTurns");

describe("Codex runtime on a never-prompted or ephemeral thread (live 0.160.1)", () => {
  const unprompted = (opts: { ephemeral: boolean; turnsList: "not_materialized" | "unsupported" }) => {
    const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
    const client: CodexRpcClient & { calls: typeof calls } = {
      calls, close: async () => {},
      request: async (method, params) => {
        calls.push({ method, params });
        switch (method) {
          case "thread/read":
            if (params.includeTurns) throw opts.ephemeral ? EPHEMERAL : new Error("unexpected includeTurns");
            return { thread: { id: "thread-1", cwd: "/workspace", status: { type: "idle" }, ephemeral: opts.ephemeral } };
          case "thread/turns/list":
            throw opts.turnsList === "not_materialized" ? NOT_MATERIALIZED("thread-1") : rejected(-32601, "list_turns is not supported yet");
          case "turn/start": return { turn: { id: "turn-first", status: "inProgress", items: [] } };
          default: return {};
        }
      },
    };
    return client;
  };

  it("starts the first turn of a never-materialized thread instead of failing every activation", async () => {
    const client = unprompted({ ephemeral: false, turnsList: "not_materialized" });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-first", prompt: "wake" }, new AbortController().signal))
      .toEqual({ status: "started", turn_id: "turn-first" });
    // Dedupe: the same operation is never submitted twice.
    expect(await runtime.activate({ operation_id: "op-first", prompt: "wake" }, new AbortController().signal))
      .toEqual({ status: "ambiguous" });
    expect(client.calls.filter((c) => c.method === "turn/start")).toHaveLength(1);
  });

  it("serves a never-materialized ephemeral thread without any includeTurns read", async () => {
    const client = unprompted({ ephemeral: true, turnsList: "not_materialized" });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-eph", prompt: "wake" }, new AbortController().signal))
      .toEqual({ status: "started", turn_id: "turn-first" });
    expect(client.calls.some((c) => c.method === "thread/read" && c.params.includeTurns === true)).toBe(false);
  });

  it("does not treat other -32600 rejections, or another thread's, as an empty history", async () => {
    for (const error of [rejected(-32600, "thread thread-1 is not loaded"), NOT_MATERIALIZED("thread-other")]) {
      const client: CodexRpcClient = { close: async () => {}, request: async (method, params) => {
        if (method === "thread/read") return { thread: { id: "thread-1", cwd: "/workspace", status: { type: "idle" } } };
        if (method === "thread/turns/list") throw error;
        if (method === "turn/start") throw new Error("must not submit");
        void params; return {};
      } };
      const runtime = await createCodexRuntime(binding, { client });
      await expect(runtime.activate({ operation_id: "op-x", prompt: "wake" }, new AbortController().signal)).rejects.toThrow(/Codex RPC rejected/);
    }
  });

  it("an ephemeral thread on a daemon without turn paging falls back without requiring includeTurns", async () => {
    const client = unprompted({ ephemeral: true, turnsList: "unsupported" });
    const runtime = await createCodexRuntime(binding, { client });
    // No rollout exists for an ephemeral thread: the fallback fails before any submit, never via includeTurns.
    await expect(runtime.activate({ operation_id: "op-legacy", prompt: "wake" }, new AbortController().signal)).rejects.toThrow(/transcript path unavailable/);
    expect(client.calls.filter((c) => c.method === "turn/start")).toHaveLength(0);
  });
});
