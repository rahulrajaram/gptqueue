import { afterEach, describe, expect, it, vi } from "vitest";
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
  it("closes and exits for an invalid catalog", async () => {
    const exit = interceptExit(); const client = makeClient();
    client.listTools.mockResolvedValue({ tools: [] });
    await expect(createPiExtension(async () => client)(fakePi().pi)).rejects.toThrow(/mismatch/);
    expect(client.close).toHaveBeenCalledOnce(); expect(exit).toHaveBeenCalledWith(1);
  });
  it("aborts a hanging connect and exits within the configured deadline", async () => {
    const exit = interceptExit(); let connectSignal: AbortSignal | undefined;
    await expect(createPiExtension(async (signal) => {
      connectSignal = signal; return new Promise<SessionClient>(() => {});
    }, 30)(fakePi().pi)).rejects.toThrow(/timed out/);
    expect(connectSignal?.aborted).toBe(true); expect(exit).toHaveBeenCalledWith(1);
  });
  it("closes and exits when tool discovery hangs", async () => {
    const exit = interceptExit(); const client = makeClient();
    client.listTools.mockImplementation(() => new Promise(() => {}));
    await expect(createPiExtension(async () => client, 30)(fakePi().pi)).rejects.toThrow(/timed out/);
    expect(client.close).toHaveBeenCalledOnce(); expect(exit).toHaveBeenCalledWith(1);
  });
  it("closes a client that resolves after the startup deadline", async () => {
    interceptExit(); const client = makeClient(); let resolve!: (value: SessionClient) => void;
    await expect(createPiExtension(() => new Promise((done) => { resolve = done; }), 30)(fakePi().pi)).rejects.toThrow(/timed out/);
    resolve(client);
    await vi.waitFor(() => expect(client.close).toHaveBeenCalledOnce());
  });
  it("exits when the runtime refuses required tool activation", async () => {
    const exit = interceptExit(); const client = makeClient(); const { pi, handlers } = fakePi();
    pi.setActiveTools.mockImplementation(() => {});
    await createPiExtension(async () => client)(pi);
    await expect(handlers.get("before_agent_start")!({ systemPrompt: "original" })).rejects.toThrow(/not active/);
    expect(client.close).toHaveBeenCalled(); expect(exit).toHaveBeenCalledWith(1);
  });
  it("exits before a turn when the MCP connection has failed", async () => {
    const exit = interceptExit(); const client = makeClient(); const { pi, handlers } = fakePi();
    await createPiExtension(async () => client)(pi);
    client.listTools.mockRejectedValue(new Error("connection closed"));
    await expect(handlers.get("before_agent_start")!({ systemPrompt: "original" })).rejects.toThrow(/connection closed/);
    expect(client.close).toHaveBeenCalled(); expect(exit).toHaveBeenCalledWith(1);
  });
  it("requires instructions identifying the registered session", async () => {
    const exit = interceptExit(); const client = makeClient(); client.getInstructions.mockReturnValue("");
    await expect(createPiExtension(async () => client)(fakePi().pi)).rejects.toThrow(/instructions/);
    expect(client.close).toHaveBeenCalled(); expect(exit).toHaveBeenCalledWith(1);
  });
});
