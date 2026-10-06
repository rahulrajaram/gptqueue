import { Redis } from "ioredis";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { createOpenCodeBackend, opencodeAgentName } from "../src/registered-shell/opencode-backend.js";
import {
  createOpenCodePlugin,
  type OpenCodeBootstrapResult,
  type OpenCodeNativeClient,
} from "../src/registered-shell/opencode-plugin-factory.js";
import type { OpenCodeRuntimePort } from "../src/registered-shell/opencode-runtime.js";
import type { OpenCodeSessionBackend, OpenCodeSessionIdentity } from "../src/registered-shell/opencode-sessions.js";

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const redis = new Redis(REDIS_URL);
const live: Array<{ close(): Promise<void> }> = [];
const unique = (label: string) => `${label}-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;

/** Protocol-faithful native port: idle status map semantics, recorded prompts. */
const portFor = (identity: OpenCodeSessionIdentity) => {
  const prompts: string[] = [];
  const port: OpenCodeRuntimePort = {
    readIdentity: vi.fn(async () => ({ runtime_id: identity.sessionID, working_directory: identity.directory })),
    status: vi.fn(async () => "idle" as const),
    history: vi.fn(async () => []),
    promptAsync: vi.fn(async (prompt: { messageID: string; text: string }) => { prompts.push(prompt.text); }),
    close: vi.fn(async () => undefined),
  };
  return { port, prompts };
};

const payload = (value: unknown): Record<string, unknown> =>
  (value as { structuredContent: Record<string, unknown> }).structuredContent;

const cleanupAgent = async (name: string): Promise<void> => {
  await redis.hdel(SESSION_KEYS.registry, name);
  await redis.del(SESSION_KEYS.queue(name), SESSION_KEYS.agent(name), SESSION_KEYS.mailboxMeta(name),
    SESSION_KEYS.heartbeat(name), SESSION_KEYS.agentSessions(name), `gptq:idempotency:${name}`,
    SESSION_KEYS.runtimeBinding(name), SESSION_KEYS.activation(name));
};

afterEach(async () => { await Promise.allSettled(live.splice(0).map((item) => item.close())); });
afterAll(async () => { await redis.quit(); });

describe("OpenCode wake prompt naming", () => {
  it("names the gptqueue_* tools OpenCode actually exposes", async () => {
    const target = { sessionID: unique("oc-prompt"), directory: "/tmp/gptqueue-oc-degrade" };
    const sender = { sessionID: unique("oc-sender"), directory: "/tmp/gptqueue-oc-degrade" };
    const native = portFor(target);
    try {
      const backend = await createOpenCodeBackend(target, { redisUrl: REDIS_URL, runtimePort: native.port, dispatcherOptions: { intervalMs: 20 } });
      const peer = await createOpenCodeBackend(sender, { redisUrl: REDIS_URL, runtimePort: portFor(sender).port, dispatcherOptions: { intervalMs: 20 } });
      live.push(backend, peer);
      expect(payload(await peer.callTool("send_message", { to: opencodeAgentName(target.sessionID), content: "wake", type: "task" })).status).toBe("sent");
      await vi.waitFor(() => expect(native.prompts).toHaveLength(1), { timeout: 5_000 });
      const prompt = native.prompts[0]!;
      for (const name of ["claim_tasks", "send_message", "acknowledge_tasks", "renew_claim", "receive_message"]) {
        expect(prompt).toContain(`gptqueue_${name}`);
        expect(prompt).not.toMatch(new RegExp(`(?<!gptqueue_)\\b${name}\\b`));
      }
    } finally {
      await cleanupAgent(opencodeAgentName(target.sessionID)); await cleanupAgent(opencodeAgentName(sender.sessionID));
    }
  });
});

describe("OpenCode second process on one session", () => {
  it("keeps tools working in standby and takes over activation when the owner goes", async () => {
    const identity = { sessionID: unique("oc-dup"), directory: "/tmp/gptqueue-oc-degrade" };
    const name = opencodeAgentName(identity.sessionID);
    let first: OpenCodeSessionBackend | undefined;
    try {
      first = await createOpenCodeBackend(identity, { redisUrl: REDIS_URL, runtimePort: portFor(identity).port, dispatcherOptions: { intervalMs: 20 } });
      const second = await createOpenCodeBackend(identity, {
        redisUrl: REDIS_URL, runtimePort: portFor(identity).port, dispatcherOptions: { intervalMs: 20 }, activationRetryMs: 50,
      });
      live.push(second);
      expect(payload(await first.callTool("get_runtime_status", {}))).toMatchObject({ activation_ready: true });
      const standby = payload(await second.callTool("get_runtime_status", {}));
      expect(standby).toMatchObject({ activation_ready: false, activation_state: "standby" });
      expect(standby.activation_error_code).toBe("RUNTIME_BINDING_HELD");
      expect(String(standby.activation_error)).toMatch(/live dispatcher/);
      // Tools still work from the second process.
      expect(payload(await second.callTool("list_agents", {})).agents).toEqual(expect.arrayContaining([expect.objectContaining({ name })]));
      await first.close(); first = undefined;
      await vi.waitFor(async () => expect(payload(await second.callTool("get_runtime_status", {}))).toMatchObject({ activation_ready: true }), { timeout: 5_000 });
    } finally {
      await first?.close();
      await cleanupAgent(name);
    }
  });
});

describe("OpenCode activation after lease loss", () => {
  it("restarts the dispatcher on an open runtime that activates pending work", async () => {
    const identity = { sessionID: unique("oc-lease"), directory: "/tmp/gptqueue-oc-degrade" };
    const sender = { sessionID: unique("oc-lease-sender"), directory: "/tmp/gptqueue-oc-degrade" };
    const name = opencodeAgentName(identity.sessionID);
    const native = portFor(identity);
    try {
      const backend = await createOpenCodeBackend(identity, {
        redisUrl: REDIS_URL, runtimePort: native.port, dispatcherOptions: { intervalMs: 20 }, activationRetryMs: 50,
      });
      const peer = await createOpenCodeBackend(sender, { redisUrl: REDIS_URL, runtimePort: portFor(sender).port, dispatcherOptions: { intervalMs: 20 } });
      live.push(backend, peer);
      expect(payload(await backend.callTool("get_runtime_status", {}))).toMatchObject({ activation_ready: true });
      // A foreign owner takes the lease briefly: the dispatcher is fenced and stops.
      await redis.set(SESSION_KEYS.runtimeBinding(name), JSON.stringify({ token: "foreign" }), "PX", 400);
      await vi.waitFor(async () => expect(payload(await backend.callTool("get_runtime_status", {})))
        .toMatchObject({ activation_ready: false, activation_error_code: "RUNTIME_BINDING_LOST" }), { timeout: 5_000 });
      await vi.waitFor(async () => expect(payload(await backend.callTool("get_runtime_status", {})))
        .toMatchObject({ activation_ready: true }), { timeout: 5_000 });
      expect(native.port.close).not.toHaveBeenCalled();
      expect(payload(await peer.callTool("send_message", { to: name, content: "after lease loss", type: "task" })).status).toBe("sent");
      await vi.waitFor(() => expect(native.prompts).toHaveLength(1), { timeout: 5_000 });
    } finally {
      await cleanupAgent(name); await cleanupAgent(opencodeAgentName(sender.sessionID));
    }
  });
});

describe("OpenCode plugin session discovery", () => {
  const nativeClient = (sessions: ReadonlyArray<Record<string, unknown>>): OpenCodeNativeClient => ({
    session: {
      get: vi.fn(async (options: { path: { id: string } }) => {
        const found = sessions.find((session) => session.id === options.path.id);
        if (!found) throw new Error("NotFoundError");
        return found;
      }),
      status: vi.fn(async () => ({})),
      messages: vi.fn(async () => []),
      promptAsync: vi.fn(async () => undefined),
      list: vi.fn(async () => sessions),
    },
  });

  it("binds sessions that existed before the plugin loaded, most recent first within the limit", async () => {
    const older = { id: unique("oc-old"), directory: "/tmp/gptqueue-oc-a", time: { created: 1, updated: 1 } };
    const newer = { id: unique("oc-new"), directory: "/tmp/gptqueue-oc-b", time: { created: 2, updated: 5 } };
    const oldest = { id: unique("oc-oldest"), directory: "/tmp/gptqueue-oc-a", time: { created: 0, updated: 0 } };
    const ports: OpenCodeSessionIdentity[] = [];
    let done!: (result: OpenCodeBootstrapResult) => void;
    const bootstrapped = new Promise<OpenCodeBootstrapResult>((resolve) => { done = resolve; });
    const client = nativeClient([older, oldest, newer]);
    const plugin = createOpenCodePlugin({ client, directory: "/tmp/gptqueue-oc-a" }, {
      redisUrl: REDIS_URL, bootstrapLimit: 2, dispatcherOptions: { intervalMs: 60_000 }, onBootstrapped: done,
      runtimePort: async (_client, identity) => { ports.push(identity); return portFor(identity).port; },
    });
    live.push({ close: () => plugin.dispose() });
    try {
      const result = await bootstrapped;
      expect(result).toMatchObject({ bound: [newer.id, older.id], failed: [], skipped: 1 });
      expect(client.session.list).toHaveBeenCalledWith(expect.objectContaining({ query: { directory: "/tmp/gptqueue-oc-a" } }));
      expect(ports).toEqual(expect.arrayContaining([{ sessionID: newer.id, directory: newer.directory }, { sessionID: older.id, directory: older.directory }]));
      expect(await redis.hexists(SESSION_KEYS.registry, opencodeAgentName(newer.id))).toBe(1);
      // A later hook for an owned session reuses its backend (single flight, per-session owner map).
      await plugin["chat.message"]({ sessionID: newer.id });
      expect(ports.filter((identity) => identity.sessionID === newer.id)).toHaveLength(1);
    } finally {
      await plugin.dispose();
      for (const session of [older, newer, oldest]) await cleanupAgent(opencodeAgentName(session.id));
    }
  });

  it("creates a lazily-bound session in its own directory, not the plugin's", async () => {
    const session = { id: unique("oc-dir"), directory: "/tmp/gptqueue-oc-session-dir" };
    const ports: OpenCodeSessionIdentity[] = [];
    const client = nativeClient([session]);
    delete (client.session as { list?: unknown }).list;
    const plugin = createOpenCodePlugin({ client, directory: "/tmp/gptqueue-oc-plugin-dir" }, {
      redisUrl: REDIS_URL, dispatcherOptions: { intervalMs: 60_000 },
      runtimePort: async (_client, identity) => { ports.push(identity); return portFor(identity).port; },
    });
    try {
      await plugin["chat.message"]({ sessionID: session.id });
      expect(ports).toEqual([{ sessionID: session.id, directory: session.directory }]);
    } finally {
      await plugin.dispose();
      await cleanupAgent(opencodeAgentName(session.id));
    }
  });

  it("defers a transient session.get failure instead of binding the plugin directory, then binds the session's own directory", async () => {
    const session = { id: unique("oc-transient"), directory: "/tmp/gptqueue-oc-own-dir" };
    const ports: OpenCodeSessionIdentity[] = [];
    const client = nativeClient([session]);
    delete (client.session as { list?: unknown }).list;
    (client.session.get as ReturnType<typeof vi.fn>).mockRejectedValueOnce(new Error("ECONNRESET"));
    const plugin = createOpenCodePlugin({ client, directory: "/tmp/gptqueue-oc-plugin-dir" }, {
      redisUrl: REDIS_URL, dispatcherOptions: { intervalMs: 60_000 }, deferredRetryMs: 20,
      runtimePort: async (_client, identity) => { ports.push(identity); return portFor(identity).port; },
    });
    try {
      await plugin["chat.message"]({ sessionID: session.id });
      expect(ports).toEqual([]);
      expect(plugin.deferredSessions()).toEqual([session.id]);
      await vi.waitFor(() => expect(ports).toEqual([{ sessionID: session.id, directory: session.directory }]), { timeout: 5_000 });
      expect(plugin.deferredSessions()).toEqual([]);
      expect(ports.some((identity) => identity.directory === "/tmp/gptqueue-oc-plugin-dir")).toBe(false);
    } finally {
      await plugin.dispose();
      await cleanupAgent(opencodeAgentName(session.id));
    }
  });

  it("falls back to the hook directory only when the native session is definitively not found", async () => {
    const sessionID = unique("oc-missing");
    const ports: OpenCodeSessionIdentity[] = [];
    const client = nativeClient([]);
    delete (client.session as { list?: unknown }).list;
    (client.session.get as ReturnType<typeof vi.fn>).mockRejectedValue(Object.assign(new Error("missing"), { name: "NotFoundError" }));
    const plugin = createOpenCodePlugin({ client, directory: "/tmp/gptqueue-oc-plugin-dir" }, {
      redisUrl: REDIS_URL, dispatcherOptions: { intervalMs: 60_000 },
      runtimePort: async (_client, identity) => { ports.push(identity); return portFor(identity).port; },
    });
    try {
      await plugin["chat.message"]({ sessionID }).catch(() => undefined);
      expect(ports).toEqual([{ sessionID, directory: "/tmp/gptqueue-oc-plugin-dir" }]);
      expect(plugin.deferredSessions()).toEqual([]);
    } finally {
      await plugin.dispose();
      await cleanupAgent(opencodeAgentName(sessionID));
    }
  });
});
