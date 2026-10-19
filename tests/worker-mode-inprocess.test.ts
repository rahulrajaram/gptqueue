/**
 * Worker mode (GPTQUEUE_WORKER=1) in the in-process entry points: the Pi
 * extension (checked in the Pi process, before any sidecar spawn) and the
 * OpenCode plugin (before any backend or Redis connection).
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { createPiExtension, createRegisteredPiExtension, GPTQUEUE_TOOLS, PI_HEADLESS_WORKER_REASON,
  type PiAPI, type SessionClient } from "../src/registered-shell/pi-extension.js";
import { createOpenCodePlugin, type OpenCodeNativeClient } from "../src/registered-shell/opencode-plugin-factory.js";
import { createOpenCodeBackend, opencodeAgentName } from "../src/registered-shell/opencode-backend.js";
import type { OpenCodeRuntimePort } from "../src/registered-shell/opencode-runtime.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const URL = testRedisUrl();
afterEach(() => { vi.restoreAllMocks(); delete process.env.GPTQUEUE_WORKER; });

const fakePi = () => {
  const handlers = new Map<string, (...args: any[]) => Promise<any> | any>();
  const pi = {
    registerTool: vi.fn(), on: vi.fn((name, handler) => { handlers.set(name, handler); }),
    getActiveTools: vi.fn(() => ["read"]), setActiveTools: vi.fn(),
  } satisfies PiAPI;
  return { pi, handlers };
};
const piClient = (): SessionClient => ({
  listTools: vi.fn(async () => ({ tools: GPTQUEUE_TOOLS.map((name) => ({ name, inputSchema: { type: "object" } })) })),
  callTool: vi.fn(async () => ({ content: [] })), close: vi.fn(async () => {}), getInstructions: () => "bound",
});

describe("Pi extension worker mode", () => {
  it("GPTQUEUE_WORKER=1: never creates a client, registers no tools, reports the headless reason", async () => {
    const make = vi.fn(async () => piClient());
    const { pi, handlers } = fakePi();
    await createPiExtension(make, 1_000, { runtimeEnabled: true, env: { GPTQUEUE_WORKER: "1" } })(pi);
    const setStatus = vi.fn();
    await handlers.get("session_start")!({}, { cwd: "/w", ui: { setStatus }, sessionManager: { getSessionId: () => "s" } });
    const prompt = await handlers.get("before_agent_start")!({ systemPrompt: "base" });
    expect(prompt.systemPrompt).toContain("headless_worker");
    expect(prompt.systemPrompt).toContain("GPTQUEUE_WORKER=1");
    expect(setStatus).toHaveBeenCalledWith("gptqueue", expect.stringContaining("headless_worker"));
    await handlers.get("session_shutdown")!();
    expect(make).not.toHaveBeenCalled();
    expect(pi.registerTool).not.toHaveBeenCalled();
  });
  it("reads the Pi process env by default: the registered extension spawns no sidecar", async () => {
    process.env.GPTQUEUE_WORKER = "1";
    const start = vi.spyOn(StdioClientTransport.prototype, "start");
    const { pi, handlers } = fakePi();
    await createRegisteredPiExtension({ redisUrl: URL, sidecarPath: "/nonexistent/gptqueue-session" })(pi);
    await handlers.get("session_start")!({}, { cwd: "/w", sessionManager: { getSessionId: () => "s" } });
    expect((await handlers.get("before_agent_start")!({ systemPrompt: "" })).systemPrompt).toContain(PI_HEADLESS_WORKER_REASON);
    expect(start).not.toHaveBeenCalled();
    expect(pi.registerTool).not.toHaveBeenCalled();
  });
  it.each([{ GPTQUEUE_WORKER: "0" }, {}])("env %j connects as before", async (env) => {
    const make = vi.fn(async () => piClient());
    const { pi } = fakePi();
    await createPiExtension(make, 1_000, { env })(pi);
    expect(make).toHaveBeenCalledOnce();
    expect(pi.registerTool).toHaveBeenCalledTimes(GPTQUEUE_TOOLS.length);
  });
});

const sessionID = `ses-worker-${Date.now()}`;
const port: OpenCodeRuntimePort = {
  readIdentity: vi.fn(async () => ({ runtime_id: sessionID, working_directory: "/workspace", epoch: "e" })),
  status: vi.fn(async () => "idle" as const), history: vi.fn(async () => []), promptAsync: vi.fn(async () => undefined),
};
const nativeClient = (): OpenCodeNativeClient => ({ session: {
  get: vi.fn(async () => ({ id: sessionID, directory: "/workspace" })), status: vi.fn(async () => ({})),
  messages: vi.fn(async () => []), promptAsync: vi.fn(async () => undefined), list: vi.fn(async () => [{ id: sessionID, directory: "/workspace" }]),
} } as unknown as OpenCodeNativeClient);
const ctx = { sessionID, directory: "/workspace", abort: new AbortController().signal };

describe("OpenCode plugin worker mode", () => {
  let redis: Redis;
  beforeAll(() => { redis = new Redis(URL); });
  afterAll(async () => { await redis.quit(); });
  const snapshot = async () => ({ keys: (await redis.keys("*")).sort(), audit: await redis.xlen(SESSION_KEYS.registrationAudit) });

  it.each([{ via: "options.env" }, { via: "process.env" }])("GPTQUEUE_WORKER=1 ($via): no register, no keys, HEADLESS_WORKER tools", async ({ via }) => {
    const register = vi.spyOn(RedisClient.prototype, "register");
    const before = await snapshot();
    if (via === "process.env") process.env.GPTQUEUE_WORKER = "1";
    const native = nativeClient();
    const plugin = createOpenCodePlugin({ client: native, directory: "/workspace" },
      { redisUrl: URL, runtimePort: async () => port, ...(via === "options.env" ? { env: { GPTQUEUE_WORKER: "1" } } : {}) });
    await plugin.event({ event: { type: "session.created", properties: { info: { id: sessionID, directory: "/workspace" } } } as never });
    await plugin["chat.message"]({ sessionID });
    const output = { system: [] as string[] };
    await plugin["experimental.chat.system.transform"]({ sessionID }, output);
    expect(output.system.join("\n")).toContain("GPTQUEUE_WORKER=1");
    expect(Object.keys(plugin.tool).every((name) => name.startsWith("gptqueue_"))).toBe(true);
    expect(plugin.tool.gptqueue_dlq_status!.description).toMatch(/^\[safety: readonly\]/u);
    for (const name of ["gptqueue_send_message", "gptqueue_claim_tasks", "gptqueue_get_runtime_status"]) {
      await expect(plugin.tool[name]!.execute({}, ctx)).rejects.toThrow(/HEADLESS_WORKER/u);
    }
    await plugin.dispose();
    expect(register).not.toHaveBeenCalled();
    expect(native.session.get).not.toHaveBeenCalled();
    expect(native.session.list).not.toHaveBeenCalled();
    expect(await snapshot()).toEqual(before);
  });
  it("the backend refuses before opening Redis", async () => {
    const register = vi.spyOn(RedisClient.prototype, "register");
    await expect(createOpenCodeBackend({ sessionID, directory: "/workspace" }, { redisUrl: URL, runtimePort: port, env: { GPTQUEUE_WORKER: "1" } }))
      .rejects.toThrow(/HEADLESS_WORKER/u);
    expect(register).not.toHaveBeenCalled();
  });
  it("GPTQUEUE_WORKER=0 registers as before", async () => {
    const plugin = createOpenCodePlugin({ client: nativeClient(), directory: "/workspace" },
      { redisUrl: URL, epoch: "e", runtimePort: async () => port, env: { GPTQUEUE_WORKER: "0" }, dispatcherOptions: { intervalMs: 60_000 } });
    try {
      await plugin["chat.message"]({ sessionID });
      expect(await redis.hexists(SESSION_KEYS.registry, opencodeAgentName(sessionID))).toBe(1);
    } finally { await plugin.dispose(); }
  });
});
