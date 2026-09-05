import { afterEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { applyContinuity, prepareContinuity, runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { restoreRuntimeMailbox } from "../src/registered-shell/runtime-mailbox.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";
import { flushTestKeys, assertNotLiveDb } from "./helpers/redis-test-utils.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
assertNotLiveDb(redisUrl, "mailbox continuity tests");
const clients: RedisClient[] = [];
const connections: Redis[] = [];
const binding = (id = randomUUID()): RuntimeBinding => ({
  client: "codex", runtime_id: id, epoch: "one", working_directory: "/workspace",
});

afterEach(async () => {
  for (const client of clients.splice(0)) client.forceDisconnect();
  for (const redis of connections.splice(0)) { await flushTestKeys(redis, redisUrl); await redis.quit(); }
});

const setup = async () => {
  const redis = new Redis(redisUrl); connections.push(redis);
  const canonical = `canonical-${randomUUID()}`;
  const provisional = `provisional-${randomUUID()}`;
  const b = binding();
  await redis.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical }), provisional, JSON.stringify({ name: provisional }));
  const key = `gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`;
  await redis.set(key, JSON.stringify({ agent: provisional, working_directory: b.working_directory }));
  return { redis, canonical, provisional, b };
};

describe("mailbox continuity operator API", () => {
  it("prepares and applies a mapping, then is idempotent", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await applyContinuity(redis, plan)).toBe("idempotent");
    expect(JSON.parse((await redis.get(plan.mappingKey))!).agent).toBe(canonical);
  });

  it("refuses stale fingerprints", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await redis.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical, changed: true }));
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("refuses target live-owner conflicts", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    const liveId = randomUUID(); await redis.sadd(SESSION_KEYS.agentSessions(canonical), liveId); await redis.set(SESSION_KEYS.lease(liveId), "alive", "EX", 30);
    await redis.set(SESSION_KEYS.heartbeat(canonical), "alive", "EX", 30);
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("refuses source queued, claimed, or outbound state", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await redis.rpush(SESSION_KEYS.queue(plan.source!), JSON.stringify({ id: "x" }));
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("refuses missing provenance instead of discovering by cwd", async () => {
    const redis = new Redis(redisUrl); connections.push(redis);
    await expect(prepareContinuity(redis, binding(), `target-${randomUUID()}`)).rejects.toThrow(/provenance/i);
  });

  it("restores the exact queued envelope at the canonical mailbox", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    const envelope = { id: randomUUID(), from: "peer", to: canonical, timestamp: new Date().toISOString(), type: "task", payload: { content: "exact" } };
    await redis.rpush(SESSION_KEYS.queue(canonical), JSON.stringify(envelope));
    const sessionId = client.sessionId!;
    expect(await restoreRuntimeMailbox(client, b)).toBe(canonical);
    expect(client.sessionId).toBe(sessionId);
    expect(await redis.hget(SESSION_KEYS.session(sessionId), "agent_name")).toBe(canonical);
    expect(await redis.sismember(SESSION_KEYS.agentSessions(canonical), sessionId)).toBe(1);
    expect(await redis.sismember(SESSION_KEYS.agentSessions(provisional), sessionId)).toBe(0);
    expect(await redis.hget(SESSION_KEYS.registry, provisional)).toBeNull();
    expect(JSON.parse((await redis.lindex(SESSION_KEYS.queue(canonical), 0))!)).toEqual(envelope);
  });

  it("allows a stale target session member when its lease is absent", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    await redis.sadd(SESSION_KEYS.agentSessions(canonical), randomUUID());
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    await expect(restoreRuntimeMailbox(client, b)).resolves.toBe(canonical);
  });

  it("permits only one concurrent adopter", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const first = new RedisClient(null, redisUrl); const second = new RedisClient(null, redisUrl); clients.push(first, second);
    await first.register("both", provisional);
    await second.register("both", `other-${randomUUID()}`);
    const results = await Promise.allSettled([restoreRuntimeMailbox(first, b), restoreRuntimeMailbox(second, b)]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((r) => r.status === "rejected")).toHaveLength(1);
    expect([first.agentName, second.agentName]).toContain(canonical);
  });

  it("refuses an active target lease and preserves source identity", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const sid = randomUUID(); await redis.sadd(SESSION_KEYS.agentSessions(canonical), sid); await redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30);
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(client.agentName).toBe(provisional);
  });

  it("refuses source queue, claim, and outbound activity", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    await redis.rpush(SESSION_KEYS.queue(provisional), "queued");
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(client.agentName).toBe(provisional);
  });
  it.each(["queue", "claim", "outbound"])("refused adoption with %s leaves durable ownership unchanged", async cause => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    if (cause === "queue") await redis.rpush(SESSION_KEYS.queue(provisional), "retained");
    if (cause === "claim") await redis.zadd(`gptq:claims-index:${provisional}`, 1, "claim");
    if (cause === "outbound") await redis.set(`gptq:outbound-activity:${provisional}`, "1");
    const snapshot = async () => ({
      mapping: await redis.get(runtimeMailboxKey(b)), registry: await redis.hgetall(SESSION_KEYS.registry),
      session: await redis.hgetall(SESSION_KEYS.session(client.sessionId!)),
      sourceMembers: await redis.smembers(SESSION_KEYS.agentSessions(provisional)),
      targetMembers: await redis.smembers(SESSION_KEYS.agentSessions(canonical)),
      sourceQueue: await redis.lrange(SESSION_KEYS.queue(provisional), 0, -1),
      targetQueue: await redis.lrange(SESSION_KEYS.queue(canonical), 0, -1),
      claims: await redis.zrange(`gptq:claims-index:${provisional}`, 0, -1),
      sourceHeartbeat: await redis.get(SESSION_KEYS.heartbeat(provisional)),
      targetHeartbeat: await redis.get(SESSION_KEYS.heartbeat(canonical)),
      binding: await redis.get(`gptq:runtime-binding:${canonical}`),
    });
    const before = await snapshot();
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(await snapshot()).toEqual(before);
    expect(client.agentName).toBe(provisional);
  });

  it("supports explicit legacy adoption without moving its pending envelope", async () => {
    const { redis, canonical, b } = await setup();
    await redis.del(runtimeMailboxKey(b));
    const envelope = JSON.stringify({ id: "pending", to: canonical, payload: { content: "preserve" } });
    await redis.rpush(SESSION_KEYS.queue(canonical), envelope);
    const plan = await prepareContinuity(redis, b, canonical, { allowLegacy: true });
    await expect(applyContinuity(redis, { ...plan, namespace: "another database" })).rejects.toThrow(/namespace/);
    await expect(applyContinuity(redis, { ...plan, mappingKey: "arbitrary:key" })).rejects.toThrow(/mapping/);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await redis.lrange(SESSION_KEYS.queue(canonical), 0, -1)).toEqual([envelope]);
    expect(await redis.xlen("gptq:continuity-audit")).toBe(1);
  });

});
