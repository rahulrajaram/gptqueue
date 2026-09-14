import { describe, expect, it, vi } from "vitest";
import { createOpenCodeRuntime, nativeMessageId, type OpenCodeRuntimePort } from "../src/registered-shell/opencode-runtime.js";

const binding = { client: "opencode" as const, runtime_id: "ses-1", epoch: "epoch-1", working_directory: "/workspace" };
const identity = { runtime_id: "ses-1", working_directory: "/workspace", epoch: "epoch-1" };
const request = (overrides: Partial<{ operation_id: string; prompt: string; recover_only?: boolean }> = {}) => ({ operation_id: "op-1", prompt: "wake", ...overrides });
const fakePort = (overrides: Partial<OpenCodeRuntimePort> = {}): OpenCodeRuntimePort & { prompts: string[] } => {
  const port = {
    prompts: [] as string[],
    readIdentity: vi.fn(async () => identity),
    status: vi.fn(async () => "idle" as const),
    history: vi.fn(async () => []),
    promptAsync: vi.fn(async (prompt: { messageID: string; text: string }) => { port.prompts.push(prompt.text); }),
    close: vi.fn(async () => {}),
    ...overrides,
  };
  return port;
};

describe("OpenCode runtime", () => {
  it("returns busy without submitting, then queues after idle", async () => {
    let current: "busy" | "idle" = "busy";
    const port = fakePort({ status: vi.fn(async () => current) });
    const runtime = await createOpenCodeRuntime(binding, port);
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "busy" });
    expect(port.prompts).toHaveLength(0);
    current = "idle";
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "queued" });
    expect(port.prompts).toHaveLength(1);
    expect(port.prompts[0]).toContain("GPTQueue activation operation: op-1");
  });

  it("reconciles a timed-out submit from history without a duplicate", async () => {
    let submitted = false;
    const port = fakePort({
      promptAsync: vi.fn(async () => { submitted = true; throw new Error("OpenCode runtime deadline exceeded"); }),
      history: vi.fn(async () => submitted ? [{ info: { id: nativeMessageId("op-1"), role: "user", sessionID: "ses-1" }, parts: [{ type: "text", text: "GPTQueue activation operation: op-1" }] }] : []),
    });
    const runtime = await createOpenCodeRuntime(binding, port, { timeoutMs: 20 });
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "ambiguous" });
    await expect(runtime.activate(request({ recover_only: true }), new AbortController().signal)).resolves.toEqual({ status: "completed", turn_id: nativeMessageId("op-1") });
    expect(port.promptAsync).toHaveBeenCalledTimes(1);
  });

  it("does not submit the same operation twice when history records it", async () => {
    const port = fakePort({ history: vi.fn(async () => [{ info: { id: nativeMessageId("op-1"), role: "user", sessionID: "ses-1" }, parts: [{ type: "text", text: "GPTQueue activation operation: op-1" }] }]) });
    const runtime = await createOpenCodeRuntime(binding, port);
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "completed", turn_id: nativeMessageId("op-1") });
    expect(port.promptAsync).not.toHaveBeenCalled();
  });

  it("rejects exact session or directory drift", async () => {
    const port = fakePort({ readIdentity: vi.fn(async () => ({ ...identity, working_directory: "/other" })) });
    await expect(createOpenCodeRuntime(binding, port)).rejects.toThrow(/identity/);
  });

  it("returns unavailable for a caller-aborted activation", async () => {
    const port = fakePort();
    const controller = new AbortController(); controller.abort();
    const runtime = await createOpenCodeRuntime(binding, port);
    await expect(runtime.activate(request(), controller.signal)).resolves.toEqual({ status: "unavailable" });
    expect(port.promptAsync).not.toHaveBeenCalled();
  });

  it("ignores assistant echoes and incidental text", async () => {
    const port = fakePort({ history: vi.fn(async () => [
      { info: { id: "assistant-1", role: "assistant", sessionID: "ses-1" }, parts: [{ type: "text", text: "GPTQueue activation operation: op-1" }] },
      { info: { id: "other-1", role: "user", sessionID: "other-session" }, parts: [{ type: "text", text: "quoted GPTQueue activation operation: op-1" }] },
    ]) });
    const runtime = await createOpenCodeRuntime(binding, port);
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "queued" });
    expect(port.promptAsync).toHaveBeenCalledWith({ messageID: nativeMessageId("op-1"), text: expect.stringContaining("GPTQueue activation operation: op-1") }, expect.any(AbortSignal));
  });

  it("requires the deterministic native ID during reconciliation", async () => {
    const port = fakePort({ history: vi.fn(async () => [{ info: { id: "msg_wrong", role: "user", sessionID: "ses-1" }, parts: [{ type: "text", text: "GPTQueue activation operation: op-1" }] }]) });
    const runtime = await createOpenCodeRuntime(binding, port);
    await expect(runtime.activate(request({ recover_only: true }), new AbortController().signal)).resolves.toEqual({ status: "ambiguous" });
  });

  it("closes once and rejects activation after terminal close", async () => {
    const port = fakePort();
    const runtime = await createOpenCodeRuntime(binding, port);
    await Promise.all([runtime.close(), runtime.close()]);
    expect(port.close).toHaveBeenCalledTimes(1);
    await expect(runtime.activate(request(), new AbortController().signal)).resolves.toEqual({ status: "unavailable" });
  });
});
