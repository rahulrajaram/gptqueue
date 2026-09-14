import { describe, expect, it } from "vitest";
import { codexCommand, codexEffectiveConfig, codexHeadlessEffectiveConfig, codexHeadlessRuntimeAgent, codexHeadlessThreadStartedId, codexRouteIds, codexTurnCompleted, createCodexAdapters, decodeHeadlessControllerTask, encodeHeadlessControllerTask, isCodexRoute, promptCodexTurn, startLiveProcess, validateCodexRedisUrl } from "./qualification-codex.js";
import { isModelParticipant } from "./qualification-types.js";

const abort = new AbortController().signal;

describe("Codex qualification adapters", () => {
  it("interrupts the accepted turn on cancellation and preserves the original abort", async () => {
    const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
    const rpc = { request: async (method: string, params: Record<string, unknown>, signal: AbortSignal) => {
      calls.push({ method, params });
      if (method === "turn/start") return { turn: { id: "owned-turn" } };
      if (method === "thread/read") return { turns: [{ id: "owned-turn", status: "inProgress" }] };
      if (method === "turn/interrupt") return {};
      if (signal.aborted) throw signal.reason;
      return {};
    } } as never;
    const controller = new AbortController();
    const pending = promptCodexTurn(rpc, "thread-1", "wait", controller.signal, async (_rpc, _threadId, signal) => {
      if (signal.aborted) throw signal.reason;
      await new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));
      return {};
    });
    const reason = new Error("caller cancelled");
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
    await new Promise(resolve => setImmediate(resolve));
    expect(calls.filter(call => call.method === "turn/interrupt")).toEqual([{ method: "turn/interrupt", params: { threadId: "thread-1", turnId: "owned-turn" } }]);
  });

  it("does not interrupt a successfully completed turn", async () => {
    const calls: string[] = [];
    const rpc = { request: async (method: string) => {
      calls.push(method);
      if (method === "turn/start") return { turn: { id: "completed-turn" } };
      return { turns: [{ id: "completed-turn", status: "completed" }] };
    } } as never;
    await expect(promptCodexTurn(rpc, "thread-2", "finish", new AbortController().signal, async () => ({ turns: [{ id: "completed-turn", status: "completed" }] }))).resolves.toEqual({ turn: { id: "completed-turn" } });
    expect(calls).not.toContain("turn/interrupt");
  });

  it("waits for the accepted turn when history still shows an older completed turn", () => {
    const old = { id: "old", status: "completed" };
    expect(codexTurnCompleted({ turns: [old] }, "new")).toBe(false);
    expect(codexTurnCompleted({ turns: [old, { id: "new", status: "inProgress" }] }, "new")).toBe(false);
    expect(codexTurnCompleted({ turns: [old, { id: "new", status: "completed" }] }, "new")).toBe(true);
  });
  it("binds headless identity to native thread.started and runtime status agent", () => {
    const events = [
      { type: "thread.started", thread_id: "thread-native-1" },
      { type: "mcp_tool_call", name: "get_runtime_status", result: { content: [{ type: "text", text: { status: "ok", agent: "codex-agent-1", runtime: null } }] } },
    ];
    expect(codexHeadlessThreadStartedId(events)).toBe("thread-native-1");
    expect(codexHeadlessRuntimeAgent(events)).toBe("codex-agent-1");
  });
  it("does not fabricate headless identity from incomplete runtime status", () => {
    expect(codexHeadlessThreadStartedId([{ type: "turn.started", thread_id: "thread-native-1" }])).toBeUndefined();
    expect(codexHeadlessRuntimeAgent([{ type: "mcp_tool_call", name: "get_runtime_status", result: { status: "error", agent: "codex-agent-1" } }])).toBeUndefined();
  });
  it("preserves arbitrary controller instructions inside an inner-task boundary", () => {
    const instruction = "Stop immediately after result send succeeds; preserve `x:y`, quotes, and unicode λ.";
    const encoded = encodeHeadlessControllerTask(instruction);
    expect(encoded).toContain("GPTQUEUE_HEADLESS_CONTROLLER_TASK_V1:");
    expect(decodeHeadlessControllerTask(encoded)).toBe(instruction);
    expect(decodeHeadlessControllerTask("ordinary peer content")).toBeUndefined();
    expect(decodeHeadlessControllerTask("GPTQUEUE_HEADLESS_CONTROLLER_TASK_V1:{\"instruction\":7}")).toBeUndefined();
  });
  it("registers six route-specific launch entries", () => {
    const set = createCodexAdapters({ codexBin: "/does/not/launch" });
    expect(set.adapters.map(adapter => adapter.spec.id)).toEqual([...codexRouteIds]);
    expect(set.adapters.every(adapter => adapter.spec.host === "codex" && adapter.spec.modelBacked)).toBe(true);
  });

  it("keeps native command mechanisms distinct", () => {
    expect(codexCommand("codex-appserver")).toEqual(["app-server", "--listen", "unix://owned"]);
    expect(codexCommand("codex-interactive")).toContain("--remote");
    expect(codexCommand("codex-headless")).toEqual(["exec", "--json", "--ephemeral"]);
    expect(codexCommand("codex-native-child")).toContain("native-collaboration");
    expect(codexCommand("codex-fork")).toContain("<parent-thread>");
    expect(codexCommand("codex-resume")).toEqual(["exec", "resume"]);
  });

  it("rejects non-owned Redis endpoints before any host process can start", () => {
    expect(() => validateCodexRedisUrl("redis://127.0.0.1:6379/15")).not.toThrow();
    expect(() => validateCodexRedisUrl("redis://127.0.0.1:6379/0")).toThrow(/db15/iu);
    expect(() => validateCodexRedisUrl("redis://example.invalid:6379/15")).toThrow(/loopback/iu);
    expect(() => validateCodexRedisUrl("rediss://127.0.0.1:6379/15")).toThrow(/loopback/iu);
  });

  it("passes the owned Redis and app-server socket into the MCP child environment", () => {
    const config = codexEffectiveConfig({}, "redis://127.0.0.1:43123/15", "/tmp/owned-codex/control.sock");
    expect(config["mcp_servers.gptqueue-shared.env.REDIS_URL"]).toBe("redis://127.0.0.1:43123/15");
    expect(config["mcp_servers.gptqueue-shared.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET"]).toBe("/tmp/owned-codex/control.sock");
    expect(config["mcp_servers.gptqueue-shared.args"]).toEqual(expect.arrayContaining(["--redis-url", "redis://127.0.0.1:43123/15"]));
  });
  it("omits unrelated disabled MCP placeholders from ignored-user-config headless config", () => {
    const config = codexHeadlessEffectiveConfig({
      "mcp_servers.haake-memory.enabled": false,
      "mcp_servers.haake-memory.required": false,
      "mcp_servers.gptqueue-shared.enabled": false,
    }, "redis://127.0.0.1:43123/15");
    expect(config["mcp_servers.haake-memory.enabled"]).toBeUndefined();
    expect(config["mcp_servers.haake-memory.required"]).toBeUndefined();
    expect(config["mcp_servers.gptqueue-shared.enabled"]).toBe(true);
    expect(config["mcp_servers.gptqueue-shared.args"]).toEqual(expect.arrayContaining(["--redis-url", "redis://127.0.0.1:43123/15"]));
  });

  it("identifies only Codex routes and leaves launch side effects behind launch", async () => {
    expect(isCodexRoute("codex-native-child")).toBe(true);
    expect(isCodexRoute("pi-sdk")).toBe(false);
    const result = await createCodexAdapters({ codexBin: "/does/not/launch" }).adapters[0]!.preflight(abort);
    expect(result.kind).toBe("setup_gap");
  });

  it("does not misclassify route contracts as generic participants", () => {
    expect(isModelParticipant({ kind: "model" } as never)).toBe(true);
  });

  it("retains bounded live-process exit diagnostics for an unsolicited failure", async () => {
    const live = startLiveProcess(process.execPath, ["-e", "process.stderr.write('headless-test-failure'); process.exit(7)"], process.cwd(), process.env);
    await new Promise<void>((resolveClose) => live.child.once("close", () => resolveClose()));
    expect(live.snapshot()).toMatchObject({ exit_code: 7, signal_code: null, caller_requested_close: false, spawn_error: null });
    expect(live.snapshot().stderr).toContain("headless-test-failure");
    expect(live.snapshot().closed_at).toEqual(expect.any(String));
  });
});
