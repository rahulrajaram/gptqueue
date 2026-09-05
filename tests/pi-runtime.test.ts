import { describe, expect, it, vi } from "vitest";
import { createPiRuntime } from "../src/registered-shell/pi-runtime.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";

const binding: RuntimeBinding = Object.freeze({ client: "pi", runtime_id: "session-1", epoch: "epoch-1", working_directory: "/tmp/project" });
const host = (entries: any[] = []) => {
  const sendMessage = vi.fn();
  const sessionManager = { getSessionId: () => "session-1", getEntries: () => entries };
  return { host: { getContext: () => ({ cwd: "/tmp/project", sessionManager }), sendMessage }, sendMessage, entries };
};

describe("Pi runtime adapter", () => {
  it("submits a follow-up with the activation marker", async () => {
    const fake = host(); const runtime = createPiRuntime(binding, fake.host);
    await expect(runtime.activate({ operation_id: "op-1", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "queued" });
    expect(fake.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ customType: "gptqueue-inbox-activation", content: "wake", details: expect.objectContaining({ operation_id: "op-1" }) }), { deliverAs: "followUp", triggerTurn: true });
  });
  it("deduplicates process and transcript retries", async () => {
    const fake = host(); const runtime = createPiRuntime(binding, fake.host); const signal = new AbortController().signal;
    await runtime.activate({ operation_id: "op-1", prompt: "wake" }, signal); await runtime.activate({ operation_id: "op-1", prompt: "wake" }, signal);
    expect(fake.sendMessage).toHaveBeenCalledTimes(1);
    const recovered = host([{ type: "message", message: { role: "custom", customType: "gptqueue-inbox-activation", details: { operation_id: "op-2" } } }]);
    await expect(createPiRuntime(binding, recovered.host).activate({ operation_id: "op-2", prompt: "wake" }, signal)).resolves.toEqual({ status: "queued" });
    expect(recovered.sendMessage).not.toHaveBeenCalled();
    const uncertain = host(); const uncertainRuntime = createPiRuntime(binding, uncertain.host);
    await expect(uncertainRuntime.activate({ operation_id: "op-3", prompt: "wake", recover_only: true }, signal)).resolves.toEqual({ status: "ambiguous" });
    expect(uncertain.sendMessage).not.toHaveBeenCalled();
  });
  it("rejects stale session or cwd and invalidates on switch", async () => {
    const fake = host(); const runtime = createPiRuntime(binding, fake.host); const manager = fake.host.getContext().sessionManager; fake.host.getContext = () => ({ cwd: "/tmp/other", sessionManager: manager });
    await expect(runtime.activate({ operation_id: "op-1", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "unavailable" });
    runtime.invalidate(); expect((await runtime.activate({ operation_id: "op-2", prompt: "wake" }, new AbortController().signal)).status).toBe("unavailable");
  });
});
