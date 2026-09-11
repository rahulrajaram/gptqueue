import { spawn } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import { bindCodexHook } from "../src/registered-shell/codex-hook.js";

const session = "123e4567-e89b-12d3-a456-426614174000";
const event = (overrides: Record<string, unknown> = {}) => ({ session_id: session, cwd: "/workspace/project", hook_event_name: "SessionStart", ...overrides });

/**
 * F10: STRICT RPC mock — records every request and asserts the exact method,
 * thread, server, tool, and bind_runtime argument set (client, runtime_id,
 * epoch, working_directory). The previous loose mock ignored the method and
 * most params, so a wrong method, server, or bad client/epoch would pass.
 */
interface RecordedCall {
  method: string;
  params: Record<string, unknown>;
  signal: AbortSignal;
}
const call = (responses: unknown[]) => {
  const calls: RecordedCall[] = [];
  return {
    calls,
    request: async (method: string, params: unknown, signal: AbortSignal) => {
      calls.push({ method, params: params as Record<string, unknown>, signal });
      const result = responses.length > 1 ? responses.shift() : responses[0];
      if (result instanceof Error) throw result;
      return result;
    },
  };
};

const expectedBindParams = {
  threadId: session,
  server: "gptqueue-shared",
  tool: "bind_runtime",
  arguments: {
    client: "codex",
    runtime_id: session,
    epoch: session,
    working_directory: "/workspace/project",
  },
};

describe("Codex runtime hook", () => {
  it("binds through the MCP call after transient failures, with the exact RPC contract", async () => {
    const rpc = call([new Error("not ready"), { isError: false, structuredContent: { activation_ready: true } }]);
    await expect(bindCodexHook(event(), rpc as never, { retryMs: 1, timeoutMs: 100 })).resolves.toBe(true);
    expect(rpc.calls).toHaveLength(2);
    expect(rpc.calls[0]).toEqual({ method: "mcpServer/tool/call", params: expectedBindParams, signal: expect.any(AbortSignal) });
    expect(rpc.calls[1]).toEqual({ method: "mcpServer/tool/call", params: expectedBindParams, signal: expect.any(AbortSignal) });
  });

  it("returns false for errors and wrong structured readiness, still with the exact RPC contract", async () => {
    const failing = call([new Error("down")]);
    await expect(bindCodexHook(event(), failing as never, { retryMs: 1, timeoutMs: 10 })).resolves.toBe(false);
    expect(failing.calls[0]?.method).toBe("mcpServer/tool/call");
    expect(failing.calls[0]?.params).toEqual(expectedBindParams);

    const notReady = call([{ isError: false, structuredContent: { activation_ready: false } }]);
    await expect(bindCodexHook(event(), notReady as never, { retryMs: 1, timeoutMs: 10 })).resolves.toBe(false);
    expect(notReady.calls[0]?.params).toEqual(expectedBindParams);
  });

  it("rejects invalid identity events before any RPC", async () => {
    const badSession = call([]);
    await expect(bindCodexHook(event({ session_id: "not-a-uuid" }), badSession as never)).rejects.toThrow();
    expect(badSession.calls).toHaveLength(0);
    const badCwd = call([]);
    await expect(bindCodexHook(event({ cwd: "relative" }), badCwd as never)).rejects.toThrow();
    expect(badCwd.calls).toHaveLength(0);
  });

  it("fails closed for malformed or oversized child input without echoing secrets", async () => {
    const run = (input: string) => new Promise<{ code: number | null; stderr: string }>((resolve) => {
      const child = spawn(process.execPath, ["bin/gptqueue-codex-hook"], { cwd: process.cwd(), stdio: ["pipe", "ignore", "pipe"] }); let stderr = "";
      child.stderr.on("data", (chunk) => { stderr += String(chunk); }); child.on("close", (code) => resolve({ code, stderr })); child.stdin.end(input);
    });
    const redactionFixture = "redis://user:secret@example.test/15";
    const invalidJson = await run("{not-json");
    expect(invalidJson.code).not.toBe(0);
    expect(invalidJson.stderr).toContain("invalid_hook_or_transport");
    const malformed = await run(JSON.stringify({ session_id: "bad", cwd: redactionFixture, hook_event_name: "SessionStart" }));
    expect(malformed.code).not.toBe(0); expect(malformed.stderr).not.toContain(redactionFixture);
    const oversized = await run("x".repeat(70_000));
    expect(oversized.code).not.toBe(0); expect(oversized.stderr).not.toContain("x".repeat(100));
  });

  it("reports a legacy loaded connection without retrying or echoing payloads, with the exact RPC contract", async () => {
    const request = vi.fn(async () => ({ isError: true, content: [{ type: "text", text: "Tool bind_runtime not found" }] }));
    const unavailable = vi.fn();
    expect(await bindCodexHook({ session_id: session, cwd: "/tmp", hook_event_name: "UserPromptSubmit" },
      { request, close: async () => {} }, { onUnavailable: unavailable })).toBe(false);
    expect(request).toHaveBeenCalledTimes(1);
    expect(request.mock.calls[0]?.[0]).toBe("mcpServer/tool/call");
    expect(request.mock.calls[0]?.[1]).toEqual({
      threadId: session, server: "gptqueue-shared", tool: "bind_runtime",
      arguments: { client: "codex", runtime_id: session, epoch: session, working_directory: "/tmp" },
    });
    expect(unavailable).toHaveBeenCalledWith("legacy_connection_requires_reconnect");
  });
});
