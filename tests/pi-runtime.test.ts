import { describe, expect, it, vi } from "vitest";
import { createPiRuntime, type PiRuntimeHost } from "../src/registered-shell/pi-runtime.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";

const binding: RuntimeBinding = Object.freeze({ client: "pi", runtime_id: "session-1", epoch: "epoch-1", working_directory: "/tmp/project" });
const host = (entries: any[] = []) => {
  const sendMessage = vi.fn();
  const sessionManager = { getSessionId: () => "session-1", getEntries: () => entries };
  const runtimeHost: PiRuntimeHost = { getContext: () => ({ cwd: "/tmp/project", sessionManager }), sendMessage };
  return { host: runtimeHost, sendMessage, entries };
};

describe("Pi runtime adapter", () => {
  it("submits a follow-up with the activation marker", async () => {
    const fake = host(); const runtime = createPiRuntime(binding, fake.host);
    await expect(runtime.activate({ operation_id: "op-1", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "queued" });
    expect(fake.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ customType: "gptqueue-inbox-activation", content: "wake", details: expect.objectContaining({ operation_id: "op-1" }) }), { deliverAs: "followUp", triggerTurn: true });
  });
  it("accepts a cwd alias that resolves to the bound working directory", async () => {
    const fake = host();
    const sessionManager = fake.host.getContext().sessionManager;
    fake.host.getContext = () => ({ cwd: "/tmp/project/../project", sessionManager });
    await expect(createPiRuntime(binding, fake.host).activate({ operation_id: "op-alias", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "queued" });
    const aliasBinding = Object.freeze({ ...binding, working_directory: "/tmp/project/.." });
    const aliasHost = host();
    const aliasManager = aliasHost.host.getContext().sessionManager;
    aliasHost.host.getContext = () => ({ cwd: "/tmp", sessionManager: aliasManager });
    await expect(createPiRuntime(aliasBinding, aliasHost.host).activate({ operation_id: "op-alias-reverse", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "queued" });
  });
  it.each([undefined, "project"])("rejects a missing or relative host cwd: %s", async (cwd) => {
    const fake = host();
    const sessionManager = fake.host.getContext().sessionManager;
    fake.host.getContext = () => ({ cwd, sessionManager });
    await expect(createPiRuntime(binding, fake.host).activate({ operation_id: "op-invalid-cwd", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "unavailable" });
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
    const stale = host(); const staleManager = stale.host.getContext().sessionManager;
    staleManager.getSessionId = () => "session-stale";
    expect((await createPiRuntime(binding, stale.host).activate({ operation_id: "op-stale-session", prompt: "wake" }, new AbortController().signal)).status).toBe("unavailable");
    const epochHost = host(); epochHost.host.getEpoch = () => "epoch-stale";
    expect((await createPiRuntime(binding, epochHost.host).activate({ operation_id: "op-stale-epoch", prompt: "wake" }, new AbortController().signal)).status).toBe("unavailable");
  });
});
