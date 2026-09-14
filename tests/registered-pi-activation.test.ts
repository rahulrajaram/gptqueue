import { describe, expect, it, vi } from "vitest";
import { createPiExtension, GPTQUEUE_TOOLS, RUNTIME_TOOL_NAMES, type PiAPI, type SessionClient } from "../src/registered-shell/pi-extension.js";

const catalog = { tools: [...GPTQUEUE_TOOLS, ...RUNTIME_TOOL_NAMES].map((name) => ({ name, inputSchema: { type: "object" } })) };
const makeClient = () => {
  let handler: any;
  const client: SessionClient & { handler?: any } = {
    listTools: vi.fn(async () => catalog),
    callTool: vi.fn(async () => ({ content: [{ type: "text", text: "ok" }], structuredContent: { activation_ready: true } })),
    getInstructions: () => "instructions",
    setActivationHandler: (value) => { handler = value; client.handler = handler; },
    close: vi.fn(async () => {}),
  };
  return client;
};

const fakePi = () => {
  const handlers = new Map<string, any>(); const entries: any[] = []; let current = { cwd: "/tmp/project", sessionManager: { getSessionId: () => "session-1", getEntries: () => entries } };
  const pi = {
    registerTool: vi.fn(), on: vi.fn((name, handler) => handlers.set(name, handler)), getActiveTools: vi.fn(() => [...GPTQUEUE_TOOLS, ...RUNTIME_TOOL_NAMES]), setActiveTools: vi.fn(),
    sendMessage: vi.fn(), appendEntry: vi.fn(), getContext: () => current,
  } satisfies PiAPI;
  return { pi, handlers, context: () => current, entries, switchSession: (id: string, cwd = current.cwd) => { current = { ...current, cwd, sessionManager: { ...current.sessionManager, getSessionId: () => id } }; }, setCwd: (cwd: string) => { current = { ...current, cwd }; } };
};

describe("registered Pi activation lifecycle", () => {
  it("does not create the sidecar client before session_start and gives the factory host context", async () => {
    const client = makeClient(); const fake = fakePi();
    const calls: unknown[][] = [];
    await createPiExtension(async (...args: unknown[]) => { calls.push(args); return client; }, 100, { runtimeEnabled: true })(fake.pi);
    expect(calls).toHaveLength(0);
    expect(fake.pi.registerTool).not.toHaveBeenCalled();
    await fake.handlers.get("session_start")({}, fake.context());
    expect(calls).toHaveLength(1);
    expect(calls[0]?.[1]).toMatchObject({ cwd: "/tmp/project", sessionManager: expect.any(Object) });
    expect(fake.pi.registerTool.mock.calls.map(([definition]) => definition.name)).toEqual([...GPTQUEUE_TOOLS, ...RUNTIME_TOOL_NAMES]);
  });

  it("creates a fresh sidecar client for a switched session and passes its new cwd", async () => {
    const first = makeClient(); const second = makeClient(); const fake = fakePi();
    const contexts: unknown[] = [];
    await createPiExtension(async (...args: unknown[]) => { contexts.push(args[1]); return contexts.length === 1 ? first : second; }, 100, { runtimeEnabled: true })(fake.pi);
    await fake.handlers.get("session_start")({}, fake.context());
    await fake.handlers.get("session_before_switch")({}, fake.context());
    fake.switchSession("session-2", "/tmp/other-project");
    await fake.handlers.get("session_start")({}, fake.context());
    expect(first.close).toHaveBeenCalledTimes(1);
    expect(fake.pi.registerTool).toHaveBeenCalledTimes(GPTQUEUE_TOOLS.length + RUNTIME_TOOL_NAMES.length);
    expect(contexts).toHaveLength(2);
    expect(contexts[1]).toMatchObject({ cwd: "/tmp/other-project", sessionManager: expect.any(Object) });
    const bind = (second.callTool as any).mock.calls.find((call: any[]) => call[0]?.name === "bind_runtime");
    expect(bind?.[0].arguments).toMatchObject({ runtime_id: "session-2", working_directory: "/tmp/other-project" });
  });

  it("rejects the old activation binding after a session switch", async () => {
    const first = makeClient(); const second = makeClient(); const fake = fakePi();
    const clientFactory = vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second);
    await createPiExtension(clientFactory, 100, { runtimeEnabled: true })(fake.pi);
    await fake.handlers.get("session_start")({}, fake.context());
    const oldBind = (first.callTool as any).mock.calls.find((call: any[]) => call[0]?.name === "bind_runtime")[0].arguments;
    await fake.handlers.get("session_before_switch")({}, fake.context());
    fake.switchSession("session-2", "/tmp/other-project");
    await fake.handlers.get("session_start")({}, fake.context());
    await expect(first.handler(oldBind, { operation_id: "old", prompt: "stale" }, new AbortController().signal)).resolves.toEqual({ status: "unavailable" });
  });

  it("rejects a relative cwd without starting a client", async () => {
    const fake = fakePi(); fake.setCwd("relative/project"); const calls: unknown[][] = [];
    const exit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    try {
      await createPiExtension(async (...args: unknown[]) => { calls.push(args); return makeClient(); }, 100, { runtimeEnabled: true })(fake.pi);
      await expect(fake.handlers.get("session_start")({}, fake.context())).rejects.toThrow(/session identity/);
      expect(calls).toHaveLength(0);
      expect(exit).toHaveBeenCalledWith(1);
    } finally { exit.mockRestore(); }
  });

  it("binds the exact session identity and submits native follow-ups", async () => {
    const client = makeClient(); const fake = fakePi();
    await createPiExtension(async () => client, 100, { runtimeEnabled: true })(fake.pi);
    await fake.handlers.get("session_start")({}, fake.context());
    const bind = (client.callTool as any).mock.calls.find((call: any[]) => call[0]?.name === "bind_runtime");
    expect(bind?.[0].arguments).toMatchObject({ client: "pi", runtime_id: "session-1", working_directory: "/tmp/project" });
    const result = await client.handler(bind[0].arguments, { operation_id: "op", prompt: "wake" }, new AbortController().signal);
    expect(result).toEqual({ status: "queued" });
    expect(fake.pi.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ customType: "gptqueue-inbox-activation", content: "wake" }), { deliverAs: "followUp", triggerTurn: true });
    await expect(client.handler({ ...bind[0].arguments, epoch: "stale" }, { operation_id: "op2", prompt: "wake" }, new AbortController().signal)).resolves.toEqual({ status: "unavailable" });
  });

  it("invalidates the adapter before a session switch", async () => {
    const client = makeClient(); const fake = fakePi();
    await createPiExtension(async () => client, 100, { runtimeEnabled: true })(fake.pi);
    await fake.handlers.get("session_start")({}, fake.context()); fake.switchSession("session-2");
    await fake.handlers.get("session_before_switch")({}, fake.context());
    expect(client.close).toHaveBeenCalled();
  });
});
