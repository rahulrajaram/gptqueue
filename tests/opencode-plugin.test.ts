import { describe, expect, it, vi } from "vitest";
import {
  createOpenCodePlugin,
  createOpenCodeRuntimePort,
  type OpenCodeNativeClient,
} from "../src/registered-shell/opencode-plugin-factory.js";
import defaultPlugin from "../src/registered-shell/opencode-plugin.js";
import type { OpenCodeRuntimePort } from "../src/registered-shell/opencode-runtime.js";

const identity = { sessionID: `ses-plugin-${Date.now()}`, directory: "/workspace" };
const testRedisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";

const native = (port: OpenCodeRuntimePort): OpenCodeNativeClient => ({
  session: {
    get: vi.fn(async () => ({ id: identity.sessionID, directory: identity.directory })),
    status: vi.fn(async () => ({ type: "idle" })),
    messages: vi.fn(async () => []),
    promptAsync: vi.fn(async () => undefined),
  },
});

describe("OpenCode plugin frontend", () => {
  it("keeps a created session through idle and retires only on deletion", async () => {
    const port: OpenCodeRuntimePort = {
      readIdentity: vi.fn(async () => ({ runtime_id: identity.sessionID, working_directory: identity.directory, epoch: "epoch" })),
      status: vi.fn(async () => "idle"),
      history: vi.fn(async () => []),
      promptAsync: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const client = native(port);
    const plugin = createOpenCodePlugin(clientInput(client), {
      redisUrl: testRedisUrl,
      epoch: "epoch",
      runtimePort: async () => port,
      dispatcherOptions: { intervalMs: 60_000 },
    });

    await plugin.event({ event: { type: "session.created", properties: { info: { id: identity.sessionID, directory: identity.directory } } } });
    await plugin.event({ event: { type: "session.idle", properties: { sessionID: identity.sessionID } } });
    expect(port.close).not.toHaveBeenCalled();
    await plugin.event({ event: { type: "session.deleted", properties: { info: { id: identity.sessionID, directory: "/other" } } } });
    expect(port.close).not.toHaveBeenCalled();
    await plugin.event({ event: { type: "session.deleted", properties: { info: { id: identity.sessionID, directory: identity.directory } } } });
    expect(port.close).toHaveBeenCalledTimes(1);
  });

  it("uses chat.message as the lazy exact-identity registration path", async () => {
    const port: OpenCodeRuntimePort = {
      readIdentity: vi.fn(async () => ({ runtime_id: identity.sessionID, working_directory: identity.directory, epoch: "epoch" })),
      status: vi.fn(async () => "idle"), history: vi.fn(async () => []),
      promptAsync: vi.fn(async () => undefined), close: vi.fn(async () => undefined),
    };
    const plugin = createOpenCodePlugin(clientInput(native(port)), {
      redisUrl: testRedisUrl, epoch: "epoch",
      runtimePort: async () => port, dispatcherOptions: { intervalMs: 60_000 },
    });
    await plugin["chat.message"]({ sessionID: identity.sessionID });
    await plugin["chat.message"]({ sessionID: identity.sessionID });
    expect(port.readIdentity).toHaveBeenCalledTimes(1);
    await plugin.dispose();
  });

  it("exposes only identity-bound gptqueue tools and system guidance", async () => {
    const port: OpenCodeRuntimePort = {
      readIdentity: vi.fn(async () => ({ runtime_id: identity.sessionID, working_directory: identity.directory, epoch: "epoch" })),
      status: vi.fn(async () => "idle"), history: vi.fn(async () => []),
      promptAsync: vi.fn(async () => undefined), close: vi.fn(async () => undefined),
    };
    const plugin = createOpenCodePlugin(clientInput(native(port)), {
      redisUrl: testRedisUrl, epoch: "epoch",
      runtimePort: async () => port, dispatcherOptions: { intervalMs: 60_000 },
    });
    expect(Object.keys(plugin.tool)).toEqual([
      "gptqueue_claim_tasks", "gptqueue_acknowledge_tasks", "gptqueue_renew_claim",
      "gptqueue_send_message", "gptqueue_receive_message", "gptqueue_get_queue_status", "gptqueue_list_agents",
      "gptqueue_get_runtime_status", "gptqueue_find_agents", "gptqueue_get_agent_details", "gptqueue_get_delivery_status",
    ]);
    expect(plugin.tool.gptqueue_claim_tasks.args).not.toHaveProperty("session_id");
    const output = { system: [] as string[] };
    await plugin["experimental.chat.system.transform"]({ sessionID: identity.sessionID }, output);
    expect(output.system[0]).toContain("already registered");
    const toolResult = await plugin.tool.gptqueue_get_queue_status.execute({}, {
      sessionID: identity.sessionID, directory: identity.directory, abort: new AbortController().signal,
    });
    expect(toolResult).toMatchObject({ output: expect.stringContaining("queues") });
    await plugin.dispose();
  });

  it("disposes all retained sessions on the supported server disposal event", async () => {
    const port: OpenCodeRuntimePort = {
      readIdentity: vi.fn(async () => ({ runtime_id: identity.sessionID, working_directory: identity.directory, epoch: "epoch" })),
      status: vi.fn(async () => "idle"), history: vi.fn(async () => []),
      promptAsync: vi.fn(async () => undefined), close: vi.fn(async () => undefined),
    };
    const plugin = createOpenCodePlugin(clientInput(native(port)), {
      redisUrl: testRedisUrl, epoch: "epoch",
      runtimePort: async () => port, dispatcherOptions: { intervalMs: 60_000 },
    });
    await plugin.event({ event: { type: "session.created", properties: { info: { id: identity.sessionID, directory: identity.directory } } } });
    await plugin.event({ event: { type: "server.instance.disposed", properties: { directory: identity.directory } } });
    expect(port.close).toHaveBeenCalledTimes(1);
  });

  it("requires explicit Redis configuration at the default entrypoint", async () => {
    const prior = process.env.GPTQUEUE_REDIS_URL;
    const priorRedis = process.env.REDIS_URL;
    delete process.env.GPTQUEUE_REDIS_URL;
    delete process.env.REDIS_URL;
    await expect(defaultPlugin(clientInput(native({} as OpenCodeRuntimePort)), {})).rejects.toThrow(/requires redisUrl/);
    if (prior === undefined) delete process.env.GPTQUEUE_REDIS_URL; else process.env.GPTQUEUE_REDIS_URL = prior;
    if (priorRedis === undefined) delete process.env.REDIS_URL; else process.env.REDIS_URL = priorRedis;
  });

  it("maps installed SDK get/status shapes and treats omitted status as idle", async () => {
    const client: OpenCodeNativeClient = {
      session: {
        get: vi.fn(async () => ({ id: identity.sessionID, directory: identity.directory, title: "child" })),
        status: vi.fn(async () => ({ [identity.sessionID]: { type: "busy" } })),
        messages: vi.fn(async () => []),
        promptAsync: vi.fn(async () => undefined),
      },
    };
    const port = createOpenCodeRuntimePort(client, identity);
    await expect(port.readIdentity(new AbortController().signal)).resolves.toEqual({
      runtime_id: identity.sessionID, working_directory: identity.directory,
    });
    await expect(port.status(new AbortController().signal)).resolves.toBe("busy");
    vi.mocked(client.session.status).mockResolvedValueOnce({});
    await expect(port.status(new AbortController().signal)).resolves.toBe("idle");
    expect(client.session.status).toHaveBeenLastCalledWith(expect.objectContaining({
      query: { directory: "/workspace" },
    }));
  });

  it("fails identity reads for a missing or drifted native session", async () => {
    const missing: OpenCodeNativeClient = {
      session: {
        get: vi.fn(async () => { throw new Error("404 Not Found"); }),
        status: vi.fn(async () => ({})), messages: vi.fn(async () => []), promptAsync: vi.fn(async () => undefined),
      },
    };
    await expect(createOpenCodeRuntimePort(missing, identity).readIdentity(new AbortController().signal)).rejects.toThrow("404");
    const drifted: OpenCodeNativeClient = {
      session: {
        get: vi.fn(async () => ({ id: identity.sessionID, directory: "/other" })),
        status: vi.fn(async () => ({})), messages: vi.fn(async () => []), promptAsync: vi.fn(async () => undefined),
      },
    };
    await expect(createOpenCodeRuntimePort(drifted, identity).readIdentity(new AbortController().signal)).rejects.toThrow(/identity/);
  });
});

const clientInput = (client: OpenCodeNativeClient) => ({ client, directory: identity.directory });
