import { describe, expect, it } from "vitest";
import { repairConfig } from "./opencode-repair-support.js";
import { assistantNonceFromNativeHistory, assistantTextFromNativeEvents, exactAssistantNonce, forkSessionLineage, nativeRuntimeObservationFromNativeEvents, opencodeRouteAdapter, opencodeRouteAdapters, opencodeRouteIds } from "./qualification-opencode.js";

describe("OpenCode qualification route adapters", () => {
  it("covers the seven native route IDs without substituting a generic route", () => {
    expect(opencodeRouteIds).toEqual([
      "opencode-interactive", "opencode-run", "opencode-native-task", "opencode-fork",
      "opencode-resume", "opencode-serve-attach", "opencode-acp",
    ]);
    expect(opencodeRouteAdapters).toHaveLength(7);
    expect(opencodeRouteAdapters.every(({ spec }) => spec.host === "opencode" && spec.modelBacked)).toBe(true);
  });

  it("keeps repaired plugin configuration isolated from the legacy shared MCP", () => {
    const config = repairConfig("/owned/dist/registered-shell/opencode-plugin.js");
    expect(config.plugin).toEqual(["/owned/dist/registered-shell/opencode-plugin.js"]);
    expect(config.mcp).toMatchObject({ gptqueue: { enabled: false } });
  });

  it("reports the interactive identity/history observer as a setup gap", async (ctx) => {
    const adapter = opencodeRouteAdapter("opencode-interactive");
    expect(adapter).toBeDefined();
    const result = await adapter!.preflight(new AbortController().signal);
    ctx.skip(result.kind === "blocked_prerequisite", "OpenCode runtime prerequisites are not installed");
    if (result.kind === "available") {
      throw new Error("interactive route must retain its explicit native identity/history setup gap");
    }
    expect(result.kind).toBe("setup_gap");
    expect(result.detail).toMatch(/identity|history/u);
  });

  it("keeps ACP as a controllable route with factual prerequisite status", async () => {
    const adapter = opencodeRouteAdapter("opencode-acp");
    expect(adapter).toBeDefined();
    const result = await adapter!.preflight(new AbortController().signal);
    expect(result.kind).not.toBe("setup_gap");
  });

  it("exposes run, fork, and resume through their native qualified lifecycles", async (ctx) => {
    for (const route of ["opencode-run", "opencode-fork", "opencode-resume"] as const) {
      const adapter = opencodeRouteAdapter(route);
      expect(adapter).toBeDefined();
      const result = await adapter!.preflight(new AbortController().signal);
      ctx.skip(result.kind === "blocked_prerequisite", `OpenCode runtime prerequisites are not installed (${route})`);
      expect(result.kind).toBe("available");
    }
  });

  it("accepts only fresh native text events for an exact assistant nonce", () => {
    const nonce = "fresh-native-nonce";
    const events = [
      { type: "session.created", properties: { info: { id: "ses-1", directory: "/tmp/work" } } },
      { type: "text", part: { type: "text", text: "fresh-" } },
      { type: "text", part: { type: "text", text: "native-nonce" } },
    ];
    expect(assistantTextFromNativeEvents(events)).toBe(nonce);
    expect(exactAssistantNonce(events, nonce)).toBe(nonce);
    expect(exactAssistantNonce([{ type: "user", text: nonce }], nonce)).toBeUndefined();
    expect(exactAssistantNonce([...events, { type: "text", part: { type: "text", text: " extra" } }], nonce)).toBeUndefined();
  });

  it("recovers an exact assistant nonce from the native session history when CLI stdout is truncated", () => {
    const nonce = "history-fallback nonce 123";
    const history = [
      { info: { id: "msg-u", role: "user" }, parts: [{ type: "text", text: `Reply with this exact nonce and no other text: ${nonce}` }] },
      { info: { id: "msg-a", role: "assistant", finish: "tool-calls" }, parts: [{ type: "step-start" }, { type: "tool", tool: "gptqueue_get_runtime_status" }] },
      { info: { id: "msg-b", role: "assistant", finish: "stop" }, parts: [
        { type: "step-start" },
        { type: "text", text: nonce },
        { type: "step-finish", reason: "stop" },
      ] },
    ];
    expect(assistantNonceFromNativeHistory(history, nonce)).toBe(nonce);
    expect(assistantNonceFromNativeHistory([], nonce)).toBeUndefined();
    expect(assistantNonceFromNativeHistory([{ info: { role: "user" }, parts: [{ type: "text", text: nonce }] }], nonce)).toBeUndefined();
    expect(assistantNonceFromNativeHistory([{ info: { role: "assistant" }, parts: [{ type: "text", text: `${nonce} extra` }] }], nonce)).toBeUndefined();
    expect(assistantNonceFromNativeHistory([{ info: { role: "assistant" }, parts: [{ type: "step-start" }] }], nonce)).toBeUndefined();
    const stray = [{ info: { role: "assistant" }, parts: [{ type: "text", text: "preamble" }] }, ...history];
    expect(assistantNonceFromNativeHistory(stray, nonce)).toBe(nonce);
  });

  it("verifies fork lineage through the child title suffix instead of a session parentID", () => {
    const seed = { id: "ses-seed", directory: "/tmp/work", title: "GPTQueue identity repair" };
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", title: "GPTQueue identity repair (fork #1)" })).toBe(true);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", title: "GPTQueue identity repair (fork #12)" })).toBe(true);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/other", title: "GPTQueue identity repair (fork #1)" })).toBe(false);
    expect(forkSessionLineage(seed, { id: "ses-seed", directory: "/tmp/work", title: "GPTQueue identity repair (fork #1)" })).toBe(false);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", title: "GPTQueue identity repair" })).toBe(false);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", title: "GPTQueue identity repair (fork #)" })).toBe(false);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", title: "other (fork #1)" })).toBe(false);
    expect(forkSessionLineage(seed, { id: "ses-child", directory: "/tmp/work", parentID: "ses-seed", title: "GPTQueue identity repair" })).toBe(false);
    expect(forkSessionLineage(undefined, { id: "ses-child" })).toBe(false);
  });

  it("binds runtime evidence only from a structured native tool event", () => {
    const runtime = nativeRuntimeObservationFromNativeEvents([
      { type: "tool_use", sessionID: "ses-native", part: { type: "tool", sessionID: "ses-native", tool: "gptqueue_get_runtime_status", state: {
        status: "completed", output: { structuredContent: { status: "ok", agent: "gptqueue-opencode-ses-native", runtime: { runtime_id: "ses-native", working_directory: "/tmp/private" } } },
      } } },
    ]);
    expect(runtime).toMatchObject({ agent: "gptqueue-opencode-ses-native", runtimeId: "ses-native" });
    expect(nativeRuntimeObservationFromNativeEvents([{ payload: { tool: "gptqueue_get_runtime_status", state: { status: "completed", output: { structuredContent: { agent: "gptqueue-opencode-ses-native", runtime: { runtime_id: "ses-native" } } } } } }])).toBeUndefined();
  });

  it("honors cancellation before any route launch or preflight work", async () => {
    const controller = new AbortController();
    controller.abort(new Error("test abort"));
    for (const adapter of opencodeRouteAdapters) {
      await expect(adapter.preflight(controller.signal)).rejects.toThrow("test abort");
    }
  });
});
