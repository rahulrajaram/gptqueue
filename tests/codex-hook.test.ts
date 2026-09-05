import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { bindCodexHook } from "../src/registered-shell/codex-hook.js";

const session = "123e4567-e89b-12d3-a456-426614174000";
const event = (overrides: Record<string, unknown> = {}) => ({ session_id: session, cwd: "/workspace/project", hook_event_name: "SessionStart", ...overrides });
const call = (responses: unknown[]) => ({ calls: [] as unknown[], request: async (_method: string, params: unknown, _signal: AbortSignal) => { const result = responses.length > 1 ? responses.shift() : responses[0]; (call as any).last = params; if (result instanceof Error) throw result; return result; } });

describe("Codex runtime hook", () => {
  it("binds through the MCP call after transient failures", async () => {
    const rpc = call([new Error("not ready"), { isError: false, structuredContent: { activation_ready: true } }]);
    await expect(bindCodexHook(event(), rpc as never, { retryMs: 1, timeoutMs: 100 })).resolves.toBe(true);
    expect((call as any).last).toMatchObject({ threadId: session, tool: "bind_runtime", arguments: { runtime_id: session, working_directory: "/workspace/project" } });
  });

  it("returns false for errors, wrong structured readiness, and invalid identity", async () => {
    await expect(bindCodexHook(event(), call([new Error("down")]) as never, { retryMs: 1, timeoutMs: 10 })).resolves.toBe(false);
    await expect(bindCodexHook(event(), call([{ isError: false, structuredContent: { activation_ready: false } }]) as never, { retryMs: 1, timeoutMs: 10 })).resolves.toBe(false);
    await expect(bindCodexHook(event({ session_id: "not-a-uuid" }), call([]) as never)).rejects.toThrow();
    await expect(bindCodexHook(event({ cwd: "relative" }), call([]) as never)).rejects.toThrow();
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
});
