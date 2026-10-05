import { afterEach, describe, expect, it, vi } from "vitest";
import { RUNTIME_TOOL_NAMES } from "../src/registered-shell/tool-names.js";
import { createPiExtension, validateCatalog, GPTQUEUE_TOOLS, type PiAPI, type SessionClient } from "../src/registered-shell/pi-extension.js";

const catalog = { tools: GPTQUEUE_TOOLS.map((name) => ({ name, inputSchema: { type: "object" } })) };
const makeClient = () => ({
  listTools: vi.fn<SessionClient["listTools"]>(async () => catalog),
  callTool: vi.fn<SessionClient["callTool"]>(async () => ({ content: [{ type: "text", text: "ok" }] })),
  close: vi.fn(async () => {}), getInstructions: vi.fn(() => "Bound to this registered session"),
});
const fakePi = () => {
  let active = ["read", "bash", "edit"];
  const handlers = new Map<string, (...args: any[]) => Promise<any>>();
  const tools: Array<Parameters<PiAPI["registerTool"]>[0]> = [];
  const pi = {
    registerTool: vi.fn((tool) => { tools.push(tool); }),
    on: vi.fn((name, handler) => { handlers.set(name, handler); }),
    getActiveTools: vi.fn(() => [...active]),
    setActiveTools: vi.fn((names: string[]) => { active = names; }),
  } satisfies PiAPI;
  return { pi, handlers, tools };
};
const interceptExit = () => vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
afterEach(() => vi.restoreAllMocks());

describe("registered Pi extension", () => {
  it("requires exactly the four bound tools and rejects session override schemas", () => {
    expect(validateCatalog(catalog)).toHaveLength(4);
    expect(() => validateCatalog({ tools: catalog.tools.slice(1) })).toThrow(/mismatch/);
    expect(() => validateCatalog({ tools: catalog.tools.map((tool) => ({ ...tool,
      inputSchema: { type: "object", properties: { session_id: { type: "string" } } } })) })).toThrow(/session_id/);
    expect(() => validateCatalog({ tools: catalog.tools.map((tool) => ({ ...tool, inputSchema: {} })) })).toThrow(/schema/);
  });
  it("preserves the complete prompt and ordinary tools while forwarding cancellation", async () => {
    const client = makeClient(); const { pi, handlers, tools } = fakePi();
    await createPiExtension(async () => client)(pi);
    const original = "Original policies\n\n# Skills\nKeep all of this.";
    expect(await handlers.get("before_agent_start")!({ systemPrompt: original })).toEqual({ systemPrompt: `${original}\n\n${client.getInstructions()}` });
    expect(pi.getActiveTools()).toEqual(["read", "bash", "edit", ...GPTQUEUE_TOOLS]);
    const signal = new AbortController().signal;
    await tools.find((tool) => tool.name === "receive_message")!.execute("call", { timeout: 30 }, signal);
    expect(client.callTool).toHaveBeenCalledWith({ name: "receive_message", arguments: { timeout: 30 } }, undefined, { signal });
    await handlers.get("session_shutdown")!();
    expect(client.close).toHaveBeenCalledOnce();
  });
  it("destroys a sidecar whose close() hangs past the bound", async () => {
    vi.useFakeTimers();
    try {
      const destroy = vi.fn();
      const client = { ...makeClient(), close: vi.fn(() => new Promise<void>(() => undefined)), destroy };
      const { pi, handlers } = fakePi();
      await createPiExtension(async () => client)(pi);
      const shutdown = handlers.get("session_shutdown")!().catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(2_500);
      expect(await shutdown).toBeInstanceOf(Error);
      expect(destroy).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });
  // These previously asserted process.exit(1). A GPTQueue fault must never
  // kill the user's Pi session (WK-7), so they now assert degrade-and-retry.
  it("degrades without exiting for an invalid catalog", async () => {
    const exit = interceptExit(); const client = makeClient();
    client.listTools.mockResolvedValue({ tools: [] });
    const { pi } = fakePi();
    await expect(createPiExtension(async () => client)(pi)).resolves.toBeUndefined();
    expect(client.close).toHaveBeenCalledOnce(); expect(exit).not.toHaveBeenCalled();
  });
  it("aborts a hanging connect within the deadline and keeps Pi running", async () => {
    const exit = interceptExit(); let connectSignal: AbortSignal | undefined;
    await expect(createPiExtension(async (signal) => {
      connectSignal = signal; return new Promise<SessionClient>(() => {});
    }, 30)(fakePi().pi)).resolves.toBeUndefined();
    expect(connectSignal?.aborted).toBe(true); expect(exit).not.toHaveBeenCalled();
  });
  it("closes without exiting when tool discovery hangs", async () => {
    const exit = interceptExit(); const client = makeClient();
    client.listTools.mockImplementation(() => new Promise(() => {}));
    await expect(createPiExtension(async () => client, 30)(fakePi().pi)).resolves.toBeUndefined();
    expect(client.close).toHaveBeenCalledOnce(); expect(exit).not.toHaveBeenCalled();
  });
  it("closes a client that resolves after the startup deadline", async () => {
    interceptExit(); const client = makeClient(); let resolve!: (value: SessionClient) => void;
    await createPiExtension(() => new Promise((done) => { resolve = done; }), 30, { retry: { baseMs: 60_000 } })(fakePi().pi);
    resolve(client);
    await vi.waitFor(() => expect(client.close).toHaveBeenCalledOnce());
  });
  it("degrades when the runtime refuses required tool activation", async () => {
    const exit = interceptExit(); const client = makeClient(); const { pi, handlers } = fakePi();
    pi.setActiveTools.mockImplementation(() => {});
    await createPiExtension(async () => client)(pi);
    const result = await handlers.get("before_agent_start")!({ systemPrompt: "original" });
    expect(result.systemPrompt).toMatch(/^original\n\nGPTQueue is currently unavailable/);
    expect(client.close).toHaveBeenCalled(); expect(exit).not.toHaveBeenCalled();
  });
  it("keeps the turn, returns typed tool errors and reconnects after a connection failure", async () => {
    const exit = interceptExit(); const client = makeClient(); const { pi, handlers, tools } = fakePi();
    let connects = 0;
    await createPiExtension(async () => { connects += 1; return client; }, 1_000, { retry: { baseMs: 10, maxMs: 20 } })(pi);
    client.listTools.mockRejectedValueOnce(new Error("connection closed"));
    const result = await handlers.get("before_agent_start")!({ systemPrompt: "original" });
    expect(result.systemPrompt).toContain("GPTQueue is currently unavailable in this session (gptqueue_unavailable)");
    expect(exit).not.toHaveBeenCalled(); expect(client.close).toHaveBeenCalled();
    const error = await tools.find((tool) => tool.name === "send_message")!.execute("call", {}).catch((caught: unknown) => caught);
    expect(error).toMatchObject({ name: "PiGptqueueUnavailableError", code: "gptqueue_unavailable" });
    expect(JSON.parse((error as Error).message)).toMatchObject({ status: "error", code: "gptqueue_unavailable" });
    await vi.waitFor(() => expect(connects).toBe(2));
    await vi.waitFor(async () => expect(await handlers.get("before_agent_start")!({ systemPrompt: "original" }))
      .toEqual({ systemPrompt: `original\n\n${client.getInstructions()}` }));
    await expect(tools.find((tool) => tool.name === "send_message")!.execute("call", {})).resolves.toMatchObject({ content: [{ text: "ok" }] });
  });
  it("requires instructions identifying the registered session, degrading instead of exiting", async () => {
    const exit = interceptExit(); const client = makeClient(); client.getInstructions.mockReturnValue("");
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await createPiExtension(async () => client, 1_000, { retry: { baseMs: 60_000 } })(fakePi().pi);
    expect(errors.mock.calls.flat().join("\n")).toMatch(/instructions/);
    expect(client.close).toHaveBeenCalled(); expect(exit).not.toHaveBeenCalled();
  });
});


it("accepts previous runtime catalogs and optional diagnostics without accepting foreign tools", () => {
  const tools = [...GPTQUEUE_TOOLS, ...RUNTIME_TOOL_NAMES].map(name => ({ name, inputSchema: { type: "object" } }));
  expect(validateCatalog({ tools }, true)).toHaveLength(9);
  expect(validateCatalog({ tools: [...tools, { name: "find_agents", inputSchema: { type: "object" } }] }, true)).toHaveLength(10);
  expect(() => validateCatalog({ tools: [...tools, { name: "foreign_tool", inputSchema: { type: "object" } }] }, true)).toThrow(/mismatch/);
  expect(() => validateCatalog({ tools: [...tools, tools[0]] }, true)).toThrow(/mismatch/);
});
