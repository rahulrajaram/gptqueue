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
  return { pi, handlers, context: () => current, entries, switchSession: (id: string) => { current = { ...current, sessionManager: { ...current.sessionManager, getSessionId: () => id } }; } };
};

describe("registered Pi activation lifecycle", () => {
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
