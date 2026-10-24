/**
 * Registered-shell sidecar dlq_status / dlq_requeue parity (sv-9 / f-2063).
 * The sidecar tools are caller-bound to the connection's registered mailbox
 * (the same binding rule as its claim tools) and return the general MCP
 * tools' result shapes. Messages are dead-lettered through the real
 * claims-recover.lua, and restored through the real claims-requeue.lua.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Redis } from "ioredis";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, describe, expect, it } from "vitest";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { DLQ_PROVISIONAL } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { startRegisteredShell } from "../src/registered-shell/server.js";
import { registerRuntimeTools, type RuntimeTools } from "../src/registered-shell/runtime-tools.js";

const URL = testRedisUrl();
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const fn of cleanups.splice(0).reverse()) await fn(); });

const data = (result: unknown): Record<string, unknown> => {
  const r = result as { structuredContent?: Record<string, unknown>; content?: Array<{ text: string }> };
  return r.structuredContent ?? JSON.parse(r.content?.[0]?.text ?? "{}");
};

const shellKeys = (agent: string, ids: readonly string[]) => [
  SESSION_KEYS.queue(agent), SESSION_KEYS.agent(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent),
  SESSION_KEYS.agentSessions(agent), SESSION_KEYS.inboxEvents(agent), SESSION_KEYS.inboxTrace(agent),
  DLQ_KEYS.list(agent), DLQ_KEYS.dropped(agent), CLAIM_KEYS.index(agent),
  ...ids.map((id) => CLAIM_KEYS.recoverCount(agent, id)),
];

const connectedShell = async (ids: readonly string[]) => {
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  const shell = await startRegisteredShell({ client: "codex", redisUrl: URL, cwd: "/tmp/dlq-parity" }, undefined, () => serverTransport);
  const client = new Client({ name: "dlq-parity-test", version: "1" });
  await client.connect(clientTransport);
  const redis = new Redis(URL);
  cleanups.push(async () => {
    await shell.close(); await client.close(); await clientTransport.close();
    await redis.hdel(SESSION_KEYS.registry, shell.agentName);
    await redis.del(...shellKeys(shell.agentName, ids));
    await redis.quit();
  });
  return { shell, client, redis };
};

/** Dead-letter `id` on `agent` through the real recovery Lua (one expiry past the cap). */
const deadLetter = async (redis: Redis, agent: string, id: string): Promise<string> => {
  const store = new TaskClaimStore(redis);
  const raw = JSON.stringify({ id, type: "task", from: "peer", to: agent, payload: { content: id, nested: { n: 1, list: ["a", "b"] } },
    correlation_id: `corr-${id}`, timestamp: new Date().toISOString() });
  await redis.rpush(SESSION_KEYS.queue(agent), raw);
  await redis.set(CLAIM_KEYS.recoverCount(agent, id), String(DLQ_PROVISIONAL.RECOVER_CAP));
  const claimed = await store.claim({ actor_id: agent, session_id: "dlq-seed", max_batch: 1, ttl_seconds: 1, now: new Date().toISOString() });
  if (!claimed.ok || !claimed.claim) throw new Error(`seed claim failed: ${JSON.stringify(claimed)}`);
  await delay(1100);
  expect(await store.recoverExpired({ actor_id: agent, now: new Date().toISOString() })).toMatchObject({ ok: true, deadlettered: 1 });
  // The owner receives an ordinary task notice; acknowledge it before the
  // restoration assertions compare the underlying payload byte for byte.
  const notification = await store.claim({ actor_id: agent, session_id: "dlq-seed", max_batch: 1, ttl_seconds: 300, now: new Date().toISOString() });
  if (!notification.ok || !notification.claim) throw new Error("expected DLQ notice claim");
  expect(JSON.parse(notification.claim.tasks[0]!)).toMatchObject({ to: agent, type: "task", payload: { metadata: { kind: "dlq_notice" } } });
  expect(await store.acknowledge({ actor_id: agent, session_id: "dlq-seed", claim_id: notification.claim.claim_id })).toMatchObject({ ok: true });
  return raw;
};

/** A bare connection whose identity is whatever RedisClient currently holds. */
const bareConnection = async (backend: RedisClient) => {
  const server = new McpServer({ name: "dlq-transition", version: "1" });
  registerRuntimeTools(server, backend, { bind: async () => ({}), status: () => ({}) } as RuntimeTools);
  const [st, ct] = InMemoryTransport.createLinkedPair();
  await server.connect(st);
  const client = new Client({ name: "dlq-transition-client", version: "1" });
  await client.connect(ct);
  cleanups.push(async () => { await client.close(); await server.close(); await backend.shutdown(); });
  return client;
};
const dlqIds = async (client: Client) =>
  (data(await client.callTool({ name: "dlq_status", arguments: {} })).entries as Array<{ message_id: string }>).map((e) => e.message_id);

describe("registered-shell dlq_status / dlq_requeue", () => {
  it("are listed with the safety contract and strict, session-free schemas", async () => {
    const { client } = await connectedShell([]);
    const tools = (await client.listTools()).tools;
    const status = tools.find((t) => t.name === "dlq_status");
    const requeue = tools.find((t) => t.name === "dlq_requeue");
    expect(status?.description).toMatch(/^\[safety: readonly\]/u);
    expect(requeue?.description).toMatch(/^\[safety: writable\]/u);
    expect(status?.annotations?.readOnlyHint).toBe(true);
    expect(requeue?.annotations?.readOnlyHint).toBe(false);
    for (const tool of [status, requeue]) {
      expect(tool?.inputSchema).not.toHaveProperty("properties.session_id");
      expect(tool?.inputSchema).not.toHaveProperty("properties.actor_id");
      expect(tool?.inputSchema).not.toHaveProperty("properties.agent");
    }
    // Unknown fields (including a target mailbox) are refused, not stripped.
    expect((await client.callTool({ name: "dlq_status", arguments: { agent: "someone-else" } })).isError).toBe(true);
    expect((await client.callTool({ name: "dlq_requeue", arguments: { message_id: "x", actor_id: "someone-else" } })).isError).toBe(true);
  });

  it("are caller-bound: a shell sees and restores only its own DLQ, and requeue restores the exact message with a fresh budget", async () => {
    const a = await connectedShell(["a-dead"]);
    const b = await connectedShell(["b-dead"]);
    const originalRaw = await deadLetter(a.redis, a.shell.agentName, "a-dead");
    await deadLetter(b.redis, b.shell.agentName, "b-dead");

    const listA = data(await a.client.callTool({ name: "dlq_status", arguments: {} }));
    expect(listA).toMatchObject({ status: "ok", count: 1, dropped: 0 });
    expect((listA.entries as Array<{ message_id: string }>).map((e) => e.message_id)).toEqual(["a-dead"]);

    // Another mailbox's DLQ entry is not restorable from A; nothing moves.
    const foreign = await a.client.callTool({ name: "dlq_requeue", arguments: { message_id: "b-dead" } });
    expect(foreign.isError).toBe(true);
    expect(JSON.stringify(foreign)).toContain("dlq_entry_not_found");
    expect(await b.redis.llen(DLQ_KEYS.list(b.shell.agentName))).toBe(1);
    expect(await a.redis.llen(SESSION_KEYS.queue(a.shell.agentName))).toBe(0);

    const restored = data(await a.client.callTool({ name: "dlq_requeue", arguments: { message_id: "a-dead" } }));
    expect(restored).toMatchObject({ status: "ok", requeued: 1 });
    const queue = await a.redis.lrange(SESSION_KEYS.queue(a.shell.agentName), 0, -1);
    expect(queue.map((raw) => JSON.parse(raw).id)).toEqual(["a-dead"]);
    // The exact message: the restored raw payload is the original, byte for byte.
    expect(queue).toEqual([originalRaw]);
    expect(await a.redis.llen(DLQ_KEYS.list(a.shell.agentName))).toBe(0);
    expect(await a.redis.exists(CLAIM_KEYS.recoverCount(a.shell.agentName, "a-dead"))).toBe(0);
    expect(data(await a.client.callTool({ name: "dlq_status", arguments: {} }))).toMatchObject({ count: 0, entries: [] });

    // B's DLQ is untouched and still visible to B only.
    const listB = data(await b.client.callTool({ name: "dlq_status", arguments: { limit: 10 } }));
    expect((listB.entries as Array<{ message_id: string }>).map((e) => e.message_id)).toEqual(["b-dead"]);
  });

  it("follow the connection's current identity across a session transition", async () => {
    const a = await connectedShell(["ta-dead"]);
    const b = await connectedShell(["tb-dead"]);
    await deadLetter(a.redis, a.shell.agentName, "ta-dead");
    await deadLetter(b.redis, b.shell.agentName, "tb-dead");
    const backend = new RedisClient(null, URL);
    const client = await bareConnection(backend);
    // Bound to A's session: sees only A's DLQ.
    await backend.reconnectSession(a.shell.sessionId);
    expect(await dlqIds(client)).toEqual(["ta-dead"]);
    // Rebound to B's session: the same connection now sees only B's DLQ, and
    // A's entry is no longer restorable through it.
    await backend.reconnectSession(b.shell.sessionId);
    expect(backend.agentName).toBe(b.shell.agentName);
    expect(await dlqIds(client)).toEqual(["tb-dead"]);
    const stale = await client.callTool({ name: "dlq_requeue", arguments: { message_id: "ta-dead" } });
    expect(stale.isError).toBe(true);
    expect(JSON.stringify(stale)).toContain("dlq_entry_not_found");
    expect(await a.redis.llen(DLQ_KEYS.list(a.shell.agentName))).toBe(1);
    expect(await a.redis.llen(SESSION_KEYS.queue(a.shell.agentName))).toBe(0);
  });

  it("refuse a registered connection with no session binding", async () => {
    const a = await connectedShell([]);
    // A name without a session (e.g. a lost or not-yet-bound session) is refused, not trusted.
    const client = await bareConnection(new RedisClient(a.shell.agentName, URL));
    for (const [name, args] of [["dlq_status", {}], ["dlq_requeue", { message_id: "x" }]] as const) {
      const result = await client.callTool({ name, arguments: args });
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result)).toContain("SESSION_UNAVAILABLE");
    }
  });

  it("refuse an unregistered connection", async () => {
    const backend = new RedisClient(null, URL);
    const server = new McpServer({ name: "dlq-unbound", version: "1" });
    registerRuntimeTools(server, backend, { bind: async () => ({}), status: () => ({}) } as RuntimeTools);
    const [st, ct] = InMemoryTransport.createLinkedPair();
    await server.connect(st);
    const client = new Client({ name: "dlq-unbound-client", version: "1" });
    await client.connect(ct);
    cleanups.push(async () => { await client.close(); await server.close(); await backend.shutdown(); });
    for (const [name, args] of [["dlq_status", {}], ["dlq_requeue", { message_id: "x" }]] as const) {
      const result = await client.callTool({ name, arguments: args });
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result)).toContain("AGENT_NOT_REGISTERED");
    }
  });
});
