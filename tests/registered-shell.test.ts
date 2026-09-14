import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Redis } from "ioredis";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { startRegisteredShell, shellAgentName, parseRegisteredShellArgs } from "../src/registered-shell/server.js";

const URL15 = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const active: Array<any> = [];
const json = (result: any) => JSON.parse(result.content[0].text);
afterEach(async () => { await Promise.all(active.splice(0).map(async ({ shell, client, peer, redis }) => { await shell.close(); await client.close(); await peer.close(); await redis.hdel(SESSION_KEYS.registry, shell.agentName); await redis.del(SESSION_KEYS.queue(shell.agentName), SESSION_KEYS.agent(shell.agentName), SESSION_KEYS.mailboxMeta(shell.agentName), SESSION_KEYS.heartbeat(shell.agentName), SESSION_KEYS.agentSessions(shell.agentName)); await redis.quit(); })); });

const connected = async (callerSignal?: AbortSignal) => {
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  const shell = await startRegisteredShell({ client: "codex", redisUrl: URL15, cwd: "/tmp/foreign-project" }, callerSignal, () => serverTransport);
  const redis = new Redis(URL15); expect(await redis.hexists(SESSION_KEYS.registry, shell.agentName)).toBe(1); const client = new Client({ name: "registered-shell-test", version: "1" });
  await client.connect(clientTransport); active.push({ shell, client, peer: clientTransport, redis }); return { shell, client, redis, serverTransport };
};

describe("registered shell boundaries", () => {
  it("requires explicit client and redis URL", () => {
    expect(parseRegisteredShellArgs(["--client", "codex", "--redis-url", "redis://127.0.0.1:6379/15"]).client).toBe("codex");
    expect(() => parseRegisteredShellArgs(["--client", "codex"])).toThrow(/redis-url/u);
    expect(() => parseRegisteredShellArgs(["--client", "nope", "--redis-url", "redis://127.0.0.1:6379/15"])).toThrow(/codex or pi/u);
    expect(() => parseRegisteredShellArgs(["--client", "codex", "--redis-url", "https://user:secret@example.test/15"])).toThrow(/Invalid Redis URL/u);
  });

  it("generates full unique identities from arbitrary cwd and ignores inherited identity", () => {
    vi.stubEnv("GPTQ_AGENT_NAME", "foreign");
    const first = shellAgentName("codex", "/tmp/foreign-project");
    const second = shellAgentName("codex", "/tmp/foreign-project");
    expect(first).toMatch(/^gptqueue-shell-codex-foreign-project-[0-9a-f-]{36}$/u);
    expect(second).not.toBe(first);
    expect(first).not.toBe(process.env.GPTQ_AGENT_NAME);
    vi.unstubAllEnvs();
  });

  it("registers before initialize and exposes only bound tools", async () => {
    const { shell, client, redis } = await connected();
    expect(await redis.hexists(SESSION_KEYS.registry, shell.agentName)).toBe(1);
    expect((await client.listTools()).tools.map((tool) => tool.name)).toEqual(["send_message", "receive_message", "list_agents", "get_queue_status", "claim_tasks", "acknowledge_tasks", "renew_claim", "bind_runtime", "get_runtime_status", "find_agents", "get_agent_details", "get_delivery_status", "set_agent_profile"]);
    expect(JSON.stringify(await client.listTools())).not.toContain("session_id");
  });

  it("creates distinct identities for concurrent shells", async () => {
    const first = await connected(); const second = await connected();
    expect(first.shell.agentName).not.toBe(second.shell.agentName);
  });

  it("preserves mailbox after close and closes transport lease", async () => {
    const { shell, client, redis, serverTransport } = await connected();
    const sent = json(await client.callTool({ name: "send_message", arguments: { to: shell.agentName, content: "kept" } }));
    expect(sent.message_id).toBeTruthy(); await serverTransport.close(); await shell.close();
    expect(await redis.scard(SESSION_KEYS.agentSessions(shell.agentName))).toBe(0);
    const probe = new (await import("../src/mcp-server/redis-client.js")).RedisClient(null, URL15);
    expect(await probe.getQueueDepth(shell.agentName)).toBe(1); await probe.shutdown();
  });

  it("aborts a pending receive without losing a later message", async () => {
    const { shell, client, redis } = await connected();
    const before = new Set((await redis.client("LIST") as string).split("\n").filter(Boolean).map((line: string) => line.match(/(?:^| )id=(\d+)/u)?.[1]).filter((id): id is string => Boolean(id)));
    const abort = new AbortController();
    const pending = client.callTool({ name: "receive_message", arguments: { timeout: 30 } }, undefined, { signal: abort.signal }).catch((error) => error);
    let blocking = new Set<string>();
    await vi.waitFor(async () => {
      const clients = (await redis.client("LIST") as string).split("\n").filter(Boolean);
      blocking = new Set(clients
        .filter((line) => line.includes(" db=15 ") && line.includes(" cmd=blpop "))
        .map((line) => line.match(/(?:^| )id=(\d+)/u)?.[1])
        .filter((id): id is string => typeof id === "string" && !before.has(id)));
      expect(blocking.size).toBeGreaterThan(0);
    }, { timeout: 2_000, interval: 10 });
    abort.abort();
    expect(await pending).toBeInstanceOf(Error);
    await vi.waitFor(async () => {
      const clients = (await redis.client("LIST") as string).split("\n").filter(Boolean);
      const activeIds = new Set(clients.map((line: string) => line.match(/(?:^| )id=(\d+)/u)?.[1]).filter((id): id is string => Boolean(id)));
      expect([...blocking].some((id) => activeIds.has(id))).toBe(false);
    }, { timeout: 2_000, interval: 10 });
    const sent = json(await client.callTool({ name: "send_message", arguments: { to: shell.agentName, content: "after-abort" } }));
    const received = json(await client.callTool({ name: "receive_message", arguments: { timeout: 1 } }));
    expect(received.id).toBe(sent.message_id);
  });

  it("closes a pending receive on caller abort and preserves a later peer message", async () => {
    const caller = new AbortController();
    const { shell, redis, client } = await connected(caller.signal);
    const peer = await connected();
    const pending = client.callTool({ name: "receive_message", arguments: { timeout: 30 } }).catch((error) => error);
    await delay(50);
    caller.abort();
    await shell.closed;
    expect(await pending).toBeInstanceOf(Error);
    expect(await redis.scard(SESSION_KEYS.agentSessions(shell.agentName))).toBe(0);
    const sent = json(await peer.client.callTool({ name: "send_message", arguments: { to: shell.agentName, content: "late" } }));
    await delay(100);
    expect(await redis.llen(SESSION_KEYS.queue(shell.agentName))).toBe(1);
    expect(JSON.parse((await redis.lindex(SESSION_KEYS.queue(shell.agentName), 0))!).id).toBe(sent.message_id);
  });

  it("bounds startup against an unresponsive Redis endpoint", async () => {
    const sockets = new Set<import("node:net").Socket>();
    const blackhole = createServer((socket) => { sockets.add(socket); socket.resume(); socket.on("close", () => sockets.delete(socket)); });
    await new Promise<void>((resolve) => blackhole.listen(0, "127.0.0.1", () => resolve()));
    const address = blackhole.address();
    if (!address || typeof address === "string") throw new Error("blackhole did not bind");
    const started = Date.now();
    await expect(startRegisteredShell({ client: "codex", redisUrl: `redis://127.0.0.1:${address.port}/15`, startupTimeoutMs: 80, cleanupTimeoutMs: 100 })).rejects.toThrow(/timed out|timeout|connect|Redis/u);
    expect(Date.now() - started).toBeLessThan(1_000);
    try { await vi.waitFor(() => expect(sockets.size).toBe(0), { timeout: 1000 }); }
    finally { for (const socket of sockets) socket.destroy(); await new Promise<void>((resolve) => blackhole.close(() => resolve())); }
  });

  it.each(["reject", "timeout"])("retires the registered session after transport startup %s", async (mode) => {
    const redis = new Redis(URL15);
    const before = new Set(await redis.hkeys(SESSION_KEYS.registry));
    let name: string | undefined; let sessionId: string | undefined;
    const transport: Transport = {
      async start() {
        const added = (await redis.hkeys(SESSION_KEYS.registry)).filter((candidate) => !before.has(candidate));
        expect(added).toHaveLength(1); name = added[0]!;
        sessionId = (await redis.smembers(SESSION_KEYS.agentSessions(name)))[0];
        expect(sessionId).toBeTruthy();
        if (mode === "timeout") return new Promise<void>(() => {});
        throw new Error("transportfailed");
      },
      async close() { transport.onclose?.(); }, async send() {},
    };
    try {
      await expect(startRegisteredShell({ client: "codex", redisUrl: URL15, startupTimeoutMs: 100, cleanupTimeoutMs: 200 }, undefined, () => transport))
        .rejects.toThrow(mode === "timeout" ? /timed out/ : /transportfailed/);
      expect(name).toBeTruthy();
      expect(await redis.scard(SESSION_KEYS.agentSessions(name!))).toBe(0);
      expect(await redis.exists(SESSION_KEYS.lease(sessionId!))).toBe(0);
      expect(await redis.exists(SESSION_KEYS.session(sessionId!))).toBe(0);
    } finally {
      if (name) { await redis.hdel(SESSION_KEYS.registry, name); await redis.del(SESSION_KEYS.queue(name), SESSION_KEYS.agent(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name), SESSION_KEYS.agentSessions(name)); }
      await redis.quit();
    }
  });

  it("retires its session even when transport close never resolves", async () => {
    const redis = new Redis(URL15);
    const hanging: Transport = { start: async () => {}, close: () => new Promise<void>(() => {}), send: async () => {} };
    const shell = await startRegisteredShell({ client: "codex", redisUrl: URL15, startupTimeoutMs: 200, cleanupTimeoutMs: 50 }, undefined, () => hanging);
    try {
      await expect(shell.close()).rejects.toThrow(/cleanup timed out/);
      await shell.closed;
      expect(await redis.scard(SESSION_KEYS.agentSessions(shell.agentName))).toBe(0);
      expect(await redis.exists(SESSION_KEYS.lease(shell.sessionId))).toBe(0);
    } finally {
      await redis.hdel(SESSION_KEYS.registry, shell.agentName);
      await redis.del(SESSION_KEYS.agent(shell.agentName), SESSION_KEYS.queue(shell.agentName), SESSION_KEYS.mailboxMeta(shell.agentName));
      await redis.quit();
    }
  });
});
