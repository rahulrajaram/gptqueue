/**
 * DL-2 / AC-1: a sidecar that dies before adopting its bound mailbox (no
 * close()) must not strand work a peer sent to its provisional name.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { sendMessage, sendMessageSchema } from "../src/mcp-server/tools/send-message.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS, SYSTEM_SENDER } from "../src/core/keys.js";
import { InboxEvents } from "../src/core/inbox-events.js";
import { RECIPIENT_RETIRED, reservedSenderRegistrations, retireProvisional, sweepProvisional } from "../src/core/provisional-sweeper.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

describe("provisional identity sweeper (DL-2 / AC-1)", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  const client = () => { const c = new RedisClient(null, URL); clients.push(c); return c; };
  beforeEach(async () => { redis = new Redis(URL); await flushTestKeys(redis, URL); });
  afterEach(async () => {
    for (const c of clients.splice(0)) { c.forceDisconnect(); await c.shutdown().catch(() => undefined); }
    await flushTestKeys(redis, URL); await redis.quit();
  });

  /** A sidecar registers provisionally, then dies without close(); its leases run out. */
  const crashed = async (opts: { provisional: boolean }) => {
    const sidecar = client();
    const name = `gptqueue-shell-pi-x-${randomUUID()}`;
    const { session_id } = await sidecar.register("both", name, "sidecar", undefined, { provisional: opts.provisional });
    return { sidecar, name, expire: async () => { sidecar.forceDisconnect(); await redis.del(SESSION_KEYS.lease(session_id), SESSION_KEYS.heartbeat(name)); } };
  };
  const send = async (from: RedisClient, to: string, type: "task" | "result" | "ping" = "task") =>
    sendMessage(from, sendMessageSchema.parse({ to, content: `${type} for ${to}`, type, ...(type === "result" ? { in_reply_to: "earlier" } : {}) }));
  const sentId = (result: Awaited<ReturnType<typeof send>>) => (result.structuredContent as { message_id: string }).message_id;

  it("retires a crashed provisional identity: task to DLQ byte for byte, one RECIPIENT_RETIRED error back, name unknown", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const { name, expire } = await crashed({ provisional: true });
    const taskId = sentId(await send(sender, name, "task"));
    const original = (await redis.lrange(SESSION_KEYS.queue(name), 0, -1))[0]!;
    await expire();

    // Two sweepers race: each message moves exactly once, one error per task.
    const other = new Redis(URL);
    const results = await Promise.all([retireProvisional(redis, name), retireProvisional(other, name), sweepProvisional(redis)]);
    await other.quit();
    const retired = [results[0], results[1], ...results[2]].filter((r) => r.outcome === "retired");
    expect(retired).toHaveLength(1);
    expect(retired[0]).toMatchObject({ moved: 1, errors_sent: 1 });

    expect(await redis.lrange(DLQ_KEYS.list(name), 0, -1)).toEqual([original]);
    expect(await redis.hget(DLQ_KEYS.reasons(name), taskId)).toBe("recipient_retired");
    expect(await redis.exists(SESSION_KEYS.queue(name))).toBe(0);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(0);

    const inbox = (await redis.lrange(SESSION_KEYS.queue(sender.agentName!), 0, -1)).map((raw) => JSON.parse(raw));
    expect(inbox).toHaveLength(1);
    expect(inbox[0]).toMatchObject({ from: SYSTEM_SENDER, to: sender.agentName, type: "error",
      payload: { in_reply_to: taskId, metadata: { code: RECIPIENT_RETIRED, recipient: name } } });
    expect(inbox[0].payload.content).toMatch(/temporary startup identity.*exited before adopting.*bound mailbox/);
    // The existing reply checks accept it, so the sender's dispatcher wakes for it.
    expect((await new InboxEvents(redis).pending(sender.agentName!)).map((m) => m.id)).toEqual([inbox[0].id]);
    expect(await redis.xlen(SESSION_KEYS.inboxEvents(sender.agentName!))).toBeGreaterThan(0);

    // Truthful refusal from now on.
    const late = await send(sender, name, "task");
    expect(late.isError).toBe(true);
    expect(late.structuredContent).toMatchObject({ error: { code: "unknown_recipient" } });
    expect(await redis.exists(SESSION_KEYS.queue(name))).toBe(0);
  });

  it("COR-F2: a send that resolved the name before retirement is refused, not stranded", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const { name, expire } = await crashed({ provisional: true });
    expect(await sender.sessions.resolveRegistered(name)).toBe(true); // resolve ...
    await expire();
    expect((await retireProvisional(redis, name)).outcome).toBe("retired"); // ... then retire ...
    const late = { id: `late-${randomUUID()}`, from: sender.agentName!, to: name, timestamp: new Date().toISOString(), type: "task" as const, payload: { content: "late" } };
    await expect(sender.sendMessage(late)).rejects.toMatchObject({ code: "RECIPIENT_RETIRED" }); // ... then push
    await expect(sender.sendMessageIdempotent({ ...late, id: `late2-${randomUUID()}` }, "k1")).rejects.toMatchObject({ code: "RECIPIENT_RETIRED" });
    expect(await redis.exists(SESSION_KEYS.queue(name))).toBe(0);
    // A later registration of the name supersedes the tombstone.
    const reborn = client(); await reborn.register("both", name);
    expect(await sender.sendMessage({ ...late, id: `again-${randomUUID()}` })).toBe(true);
  });

  it("SEC-F4: with the reserved sender registered by an old server, the sweep neither re-points owners nor speaks as it", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const { name, expire } = await crashed({ provisional: true });
    const taskId = sentId(await send(sender, name, "task"));
    await expire();
    // What a pre-change server writes when it registers the reserved name.
    await redis.hset(SESSION_KEYS.registry, SYSTEM_SENDER, JSON.stringify({ name: SYSTEM_SENDER }));
    expect(await reservedSenderRegistrations(redis)).toEqual({ registry: true, sessions: 0 });
    expect(await retireProvisional(redis, name)).toMatchObject({ outcome: "retired", moved: 1, errors_sent: 0 });
    expect(await redis.get(SESSION_KEYS.outstanding(sender.agentName!, taskId))).toBe(name);
    expect(await redis.llen(SESSION_KEYS.queue(sender.agentName!))).toBe(0);
    expect(await redis.llen(DLQ_KEYS.list(name))).toBe(1);
    const audit = (await redis.xrange(SESSION_KEYS.identityAudit, "-", "+")).map(([, f]) => f.join(" "));
    expect(audit.some((f) => f.includes("reserved_sender_registered") && f.includes(name))).toBe(true);
    await redis.hdel(SESSION_KEYS.registry, SYSTEM_SENDER);
    expect(await reservedSenderRegistrations(redis)).toEqual({ registry: false, sessions: 0 });
  });

  it("dead-letters results and pings to the dead name without replying", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const { name, expire } = await crashed({ provisional: true });
    await send(sender, name, "result");
    await send(sender, name, "ping");
    await expire();
    expect(await retireProvisional(redis, name)).toMatchObject({ outcome: "retired", moved: 2, errors_sent: 0 });
    expect(await redis.llen(DLQ_KEYS.list(name))).toBe(2);
    expect(await redis.llen(SESSION_KEYS.queue(sender.agentName!))).toBe(0);
  });

  it("never sweeps a provisional identity with a live session or a live claim", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const alive = await crashed({ provisional: true });
    await send(sender, alive.name, "task");
    expect(await retireProvisional(redis, alive.name)).toMatchObject({ outcome: "live" });

    const claimed = await crashed({ provisional: true });
    await send(sender, claimed.name, "task");
    await redis.zadd(CLAIM_KEYS.index(claimed.name), Date.now() + 60_000, "live-claim");
    await claimed.expire();
    expect(await retireProvisional(redis, claimed.name)).toMatchObject({ outcome: "claimed" });
    for (const name of [alive.name, claimed.name]) {
      expect(await redis.llen(SESSION_KEYS.queue(name))).toBe(1);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    }
    expect(await redis.llen(SESSION_KEYS.queue(sender.agentName!))).toBe(0);
  });

  it("never sweeps an ordinary agent, even with every session expired", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const ordinary = await crashed({ provisional: false });
    await send(sender, ordinary.name, "task");
    await ordinary.expire();
    await redis.sadd(SESSION_KEYS.provisional, "unrelated-placeholder");
    await sweepProvisional(redis);
    expect(await retireProvisional(redis, ordinary.name)).toMatchObject({ outcome: "unmarked" });
    expect(await redis.llen(SESSION_KEYS.queue(ordinary.name))).toBe(1);
    expect(await redis.hexists(SESSION_KEYS.registry, ordinary.name)).toBe(1);
  });

  it("registering a new provisional identity sweeps dead ones opportunistically", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const dead = await crashed({ provisional: true });
    await send(sender, dead.name, "task");
    await dead.expire();
    await crashed({ provisional: true });
    expect(await redis.hexists(SESSION_KEYS.registry, dead.name)).toBe(0);
    expect(await redis.llen(SESSION_KEYS.queue(sender.agentName!))).toBe(1);
  });

  it("a provisional registration refused with AGENT_NAME_HELD leaves no mark; the real holder survives a sweep", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const holder = client();
    const name = `real-agent-${randomUUID()}`;
    const { session_id } = await holder.register("both", name, "real identity");
    await expect(client().register("both", name, "late sidecar", undefined, { provisional: true }))
      .rejects.toMatchObject({ code: "AGENT_NAME_HELD" });
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(0);

    await send(sender, name, "task");
    holder.forceDisconnect();
    await redis.del(SESSION_KEYS.lease(session_id), SESSION_KEYS.heartbeat(name));
    await sweepProvisional(redis);
    expect(await retireProvisional(redis, name)).toMatchObject({ outcome: "unmarked" });
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    expect(await redis.llen(SESSION_KEYS.queue(name))).toBe(1);
  });

  it("a successful provisional claim is marked in the same step (crash-safe)", async () => {
    const sidecar = client();
    const name = `gptqueue-shell-pi-x-${randomUUID()}`;
    await sidecar.sessions.createSession(name, "both", undefined, undefined, { provisional: true });
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(1);
  });

  it("a provisional name can never be shared", async () => {
    const name = `gptqueue-shell-pi-x-${randomUUID()}`;
    await expect(client().register("both", name, "sidecar", undefined, { provisional: true, shared: true }))
      .rejects.toMatchObject({ code: "AGENT_REGISTRATION_INVALID" });
    const previous = process.env.GPTQ_AGENT_SHARED;
    process.env.GPTQ_AGENT_SHARED = "1";
    try {
      await expect(client().register("both", name, "sidecar", undefined, { provisional: true }))
        .rejects.toMatchObject({ code: "AGENT_REGISTRATION_INVALID" });
    } finally {
      if (previous === undefined) delete process.env.GPTQ_AGENT_SHARED; else process.env.GPTQ_AGENT_SHARED = previous;
    }
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(0);
    expect(await redis.exists(SESSION_KEYS.agentSessions(name))).toBe(0);
  });

  it("a shared claimant onto a live provisional holder is refused; the name is still swept after expiry", async () => {
    const sender = client(); await sender.register("both", `sender-${randomUUID()}`);
    const { name, expire } = await crashed({ provisional: true });
    await expect(client().register("both", name, "joiner", undefined, { shared: true }))
      .rejects.toMatchObject({ code: "AGENT_NAME_HELD" });
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(1);
    await send(sender, name, "task");
    await expire();
    expect(await retireProvisional(redis, name)).toMatchObject({ outcome: "retired", moved: 1, errors_sent: 1 });
  });

  it("reserves the system sender name", async () => {
    await expect(client().register("both", SYSTEM_SENDER)).rejects.toMatchObject({ code: "AGENT_NAME_RESERVED" });
  });
});
