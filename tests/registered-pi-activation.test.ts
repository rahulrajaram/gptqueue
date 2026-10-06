import { describe, expect, it, vi } from "vitest";
import { createPiExtension, GPTQUEUE_TOOLS, SHELL_TOOLS, type PiAPI, type SessionClient } from "../src/registered-shell/pi-extension.js";

const catalog = { tools: [...GPTQUEUE_TOOLS, ...SHELL_TOOLS].map((name) => ({ name, inputSchema: { type: "object" } })) };
const makeClient = () => {
  let handler: any;
  const client: SessionClient & { handler?: any } = {
    listTools: vi.fn(async () => catalog),
    callTool: vi.fn(async () => ({ content: [{ type: "text", text: "ok" }], structuredContent: { activation_ready: true } })) as unknown as SessionClient["callTool"],
    getInstructions: () => "instructions",
    setActivationHandler: (value) => { handler = value; client.handler = handler; },
    close: vi.fn(async () => {}),
  };
  return client;
};

const fakePi = () => {
  const handlers = new Map<string, any>(); const entries: any[] = []; let current = { cwd: "/tmp/project", sessionManager: { getSessionId: () => "session-1", getEntries: () => entries } };
  const pi = {
    registerTool: vi.fn(), on: vi.fn((name, handler) => handlers.set(name, handler)), getActiveTools: vi.fn(() => [...GPTQUEUE_TOOLS, ...SHELL_TOOLS]), setActiveTools: vi.fn(),
    sendMessage: vi.fn(), appendEntry: vi.fn(),
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
    expect(fake.pi.registerTool.mock.calls.map(([definition]) => definition.name)).toEqual([...GPTQUEUE_TOOLS, ...SHELL_TOOLS]);
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
    expect(fake.pi.registerTool).toHaveBeenCalledTimes(GPTQUEUE_TOOLS.length + SHELL_TOOLS.length);
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

  // Previously asserted process.exit(1); a host misconfiguration must degrade, not kill Pi (WK-7).
  it("rejects a relative cwd without starting a client, keeping Pi running and telling the user", async () => {
    const fake = fakePi(); fake.setCwd("relative/project"); const calls: unknown[][] = [];
    const exit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const ui = { notify: vi.fn(), setStatus: vi.fn() };
    try {
      await createPiExtension(async (...args: unknown[]) => { calls.push(args); return makeClient(); }, 100, { runtimeEnabled: true })(fake.pi);
      await expect(fake.handlers.get("session_start")({}, { ...fake.context(), ui })).resolves.toBeUndefined();
      expect(calls).toHaveLength(0);
      expect(exit).not.toHaveBeenCalled();
      expect(ui.setStatus).toHaveBeenCalledWith("gptqueue", expect.stringMatching(/runtime_unsupported/));
      expect(ui.notify).toHaveBeenCalledWith(expect.stringMatching(/session identity/), "error");
    } finally { exit.mockRestore(); errors.mockRestore(); }
  });

  it("surfaces a refused duplicate-resume bind, keeps Pi running, and binds once the owner is gone", async () => {
    const refused = makeClient(); const accepted = makeClient(); const fake = fakePi();
    (refused.callTool as any).mockImplementation(async ({ name }: { name: string }) => name === "bind_runtime"
      ? { isError: true, content: [{ type: "text", text: "occupied_target: runtime mailbox is owned by a live session" }],
        structuredContent: { status: "error", error: { code: "RUNTIME_BINDING_HELD", message: "held", retryable: false } } }
      : { content: [{ type: "text", text: "ok" }] });
    const exit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const ui = { notify: vi.fn(), setStatus: vi.fn() };
    const factory = vi.fn().mockResolvedValueOnce(refused).mockResolvedValue(accepted);
    try {
      await createPiExtension(factory, 100, { runtimeEnabled: true, retry: { baseMs: 10, maxMs: 20 } })(fake.pi);
      await fake.handlers.get("session_start")({}, { ...fake.context(), ui });
      expect(exit).not.toHaveBeenCalled();
      expect(refused.close).toHaveBeenCalled();
      expect(ui.notify).toHaveBeenCalledWith(expect.stringMatching(/refused this Pi session's mailbox binding.*occupied_target/), "warning");
      const before = await fake.handlers.get("before_agent_start")({ systemPrompt: "p" });
      expect(before.systemPrompt).toContain("(runtime_binding_refused)");
      await vi.waitFor(() => expect((accepted.callTool as any).mock.calls.some((call: any[]) => call[0]?.name === "bind_runtime")).toBe(true));
      await vi.waitFor(() => expect(ui.notify).toHaveBeenCalledWith("GPTQueue reconnected", "info"));
      expect(ui.setStatus).toHaveBeenLastCalledWith("gptqueue", undefined);
      expect((await fake.handlers.get("before_agent_start")({ systemPrompt: "p" })).systemPrompt).toBe("p\n\ninstructions");
    } finally { exit.mockRestore(); errors.mockRestore(); }
  });

  it("aborts and closes a superseded in-flight sidecar so exactly one client stays live", async () => {
    const first = makeClient(); const second = makeClient(); const fake = fakePi();
    const pending: Array<{ signal: AbortSignal; resolve: (client: SessionClient) => void }> = [];
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      await createPiExtension((signal) => new Promise<SessionClient>((resolve) => { pending.push({ signal, resolve }); }), 1_000, { runtimeEnabled: true })(fake.pi);
      const stale = fake.handlers.get("session_start")({}, fake.context());
      await vi.waitFor(() => expect(pending).toHaveLength(1));
      await fake.handlers.get("session_before_switch")({}, fake.context());
      fake.switchSession("session-2");
      const current = fake.handlers.get("session_start")({}, fake.context());
      await vi.waitFor(() => expect(pending).toHaveLength(2));
      expect(pending[0]!.signal.aborted).toBe(true);
      expect(pending[1]!.signal.aborted).toBe(false);
      pending[0]!.resolve(first); pending[1]!.resolve(second);
      await Promise.all([stale, current]);
      expect(first.close).toHaveBeenCalledTimes(1);
      expect(second.close).not.toHaveBeenCalled();
      expect((first.callTool as any).mock.calls.some((call: any[]) => call[0]?.name === "bind_runtime")).toBe(false);
      const bind = (second.callTool as any).mock.calls.find((call: any[]) => call[0]?.name === "bind_runtime");
      expect(bind?.[0].arguments).toMatchObject({ runtime_id: "session-2" });
    } finally { errors.mockRestore(); }
  });

  it("aborts a superseded in-flight bind_runtime and binds the new session only after it settled and its client closed", async () => {
    const first = makeClient(); const second = makeClient(); const fake = fakePi();
    const events: string[] = [];
    let oldSignal: AbortSignal | undefined;
    (first.callTool as any).mockImplementation(({ name }: { name: string }, _r: unknown, opts: { signal: AbortSignal }) => {
      if (name !== "bind_runtime") return Promise.resolve({ content: [], structuredContent: {} });
      oldSignal = opts.signal;
      events.push("old-bind-start");
      return new Promise((_resolve, reject) => opts.signal.addEventListener("abort", () => {
        setTimeout(() => { events.push("old-bind-settled"); reject(new Error("aborted")); }, 20);
      }));
    });
    (first.close as any).mockImplementation(async () => { events.push("old-client-closed"); });
    (second.callTool as any).mockImplementation(async ({ name }: { name: string }) => {
      if (name === "bind_runtime") events.push("new-bind");
      return { content: [{ type: "text", text: "ok" }], structuredContent: { activation_ready: true } };
    });
    const clients = [first, second];
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      await createPiExtension(async () => clients.shift()!, 5_000, { runtimeEnabled: true, retry: { baseMs: 60_000 } })(fake.pi);
      const stale = fake.handlers.get("session_start")({}, fake.context());
      await vi.waitFor(() => expect(events).toContain("old-bind-start"));
      fake.switchSession("session-2");
      await fake.handlers.get("session_start")({}, fake.context());
      await stale;
      expect(oldSignal?.aborted).toBe(true);
      expect(events).toEqual(["old-bind-start", "old-client-closed", "old-bind-settled", "new-bind"]);
    } finally { errors.mockRestore(); }
  });

  it("shares one in-flight sidecar between concurrent starts of the same session", async () => {
    const only = makeClient(); const fake = fakePi();
    const factory = vi.fn(async () => { await new Promise((resolve) => setTimeout(resolve, 10)); return only; });
    await createPiExtension(factory, 1_000, { runtimeEnabled: true })(fake.pi);
    await Promise.all([fake.handlers.get("session_start")({}, fake.context()),
      fake.handlers.get("before_agent_start")({ systemPrompt: "p" })]);
    expect(factory).toHaveBeenCalledTimes(1);
    expect(only.close).not.toHaveBeenCalled();
  });

  it.each([
    ["RUNTIME_BINDING_HELD", "runtime_binding_refused", /refused this Pi session's mailbox binding/, "warning"],
    ["REDIS_UNAVAILABLE", "gptqueue_unavailable", /GPTQueue unavailable \(gptqueue_unavailable\)/, "error"],
    ["GPTQUEUE_ERROR", "runtime_binding_failed", /GPTQueue unavailable \(runtime_binding_failed\).*\(GPTQUEUE_ERROR\)/, "error"],
  ] as const)("reports a bind_runtime failure with code %s as %s", async (code, state, text, level) => {
    const failing = makeClient(); const fake = fakePi();
    (failing.callTool as any).mockImplementation(async ({ name }: { name: string }) => name === "bind_runtime"
      ? { isError: true, content: [{ type: "text", text: "bind failed" }],
        structuredContent: { status: "error", error: { code, message: "bind failed", retryable: false } } }
      : { content: [{ type: "text", text: "ok" }] });
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const ui = { notify: vi.fn(), setStatus: vi.fn() };
    try {
      await createPiExtension(async () => failing, 100, { runtimeEnabled: true, retry: { baseMs: 60_000, maxMs: 60_000 } })(fake.pi);
      await fake.handlers.get("session_start")({}, { ...fake.context(), ui });
      expect(ui.notify).toHaveBeenCalledWith(expect.stringMatching(text), level);
      expect((await fake.handlers.get("before_agent_start")({ systemPrompt: "p" })).systemPrompt).toContain(`(${state})`);
    } finally { errors.mockRestore(); await fake.handlers.get("session_shutdown")({}, fake.context()); }
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
