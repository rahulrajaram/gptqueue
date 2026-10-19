/**
 * Waking admission follow-up: FIFO claiming past quiet mail (R10) and
 * overshoot through every lossless path (R3): expired-claim recovery
 * (claims-recover.lua), DLQ requeue (claims-requeue.lua) and provisional
 * forwarding (forwardProvisionalLua). Real Lua on Redis db 15.
 */
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import type { QueueMessage } from "../src/core/types.js";
import { assertNotLiveDb } from "./helpers/redis-test-utils.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
assertNotLiveDb(REDIS_URL, "waking-admission-lossless");
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("waking admission: FIFO claims and lossless overshoot", () => {
  let redis: Redis;
  let claims: TaskClaimStore;
  const tracked = new Set<string>();
  const track = (...keys: string[]) => { for (const key of keys) tracked.add(key); };
  beforeAll(() => { redis = new Redis(REDIS_URL); claims = new TaskClaimStore(redis); });
  afterAll(async () => { if (tracked.size > 0) await redis.del(...tracked); redis.disconnect(); });

  const mailbox = (prefix = "wl") => {
    const to = `${prefix}-${randomUUID().slice(0, 8)}`;
    track(SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to), SESSION_KEYS.forward(to),
      CLAIM_KEYS.index(to), DLQ_KEYS.list(to), DLQ_KEYS.dropped(to));
    return to;
  };
  const envelope = (to: string, type: QueueMessage["type"]): QueueMessage => {
    const id = randomUUID();
    track(SESSION_KEYS.outstanding("peer", id), SESSION_KEYS.taskLedger(id), SESSION_KEYS.taskAnswered(id), CLAIM_KEYS.recoverCount(to, id));
    return { id, from: "peer", to, timestamp: "2030-01-01T00:00:00.000Z", type, payload: { content: `${type} body` } };
  };
  const idsOf = (raws: readonly string[]) => raws.map((raw) => (JSON.parse(raw) as QueueMessage).id);
  const claim = async (actor: string, max_batch: number): Promise<readonly string[]> => {
    const result = await claims.claim({ actor_id: actor, session_id: "s-wl", max_batch, ttl_seconds: 600, now: new Date().toISOString() });
    if (!result.ok) throw new Error(JSON.stringify(result.error));
    return result.claim?.tasks ?? [];
  };

  /** Ten quiet entries admitted by ordinary sends, then a waking task admitted at position 11. */
  const quietThenTask = async (store: MailboxStore) => {
    const to = mailbox("fifo");
    const quiet = Array.from({ length: 10 }, () => envelope(to, "status"));
    for (const message of quiet) expect(await store.sendWithOutcome(message, 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "status"), 1)).toMatchObject({ status: "full", reason: "full" });
    const task = envelope(to, "task");
    expect(await store.sendWithOutcome(task, 1)).toMatchObject({ status: "sent" });
    return { to, order: [...quiet.map((m) => m.id), task.id], task };
  };

  it("(R10) batches of 4 reach the task in a later batch, FIFO, nothing skipped or reordered", async () => {
    const store = new MailboxStore(redis, redis, 10);
    const { to, order, task } = await quietThenTask(store);
    const batches: (readonly string[])[] = [];
    for (let batch = await claim(to, 4); batch.length > 0; batch = await claim(to, 4)) batches.push(idsOf(batch));
    expect(batches.map((b) => b.length)).toEqual([4, 4, 3]);
    expect(batches.flat()).toEqual(order);
    expect(batches[2]![2]).toBe(task.id);
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(0);
  });

  it("(R10) with max_batch 16 the task is the 11th element claimed", async () => {
    const store = new MailboxStore(redis, redis, 10);
    const { to, order, task } = await quietThenTask(store);
    const batch = idsOf(await claim(to, 16));
    expect(batch).toEqual(order);
    expect(batch[10]).toBe(task.id);
  });

  /** Later ordinary admission at max_size 3 over 2 waking + 10 quiet actual entries. */
  const admitsByActualContents = async (to: string) => {
    const store = new MailboxStore(redis, redis, 3);
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(12); // above 2 x max_size
    expect((await store.status(to))[0]).toMatchObject({ depth: 12, waking_count: 2, quiet_count: 10 });
    expect(await store.sendWithOutcome(envelope(to, "status"), 1)).toMatchObject({ status: "full", reason: "full" });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" }); // 2 < 3
    expect((await store.status(to))[0]).toMatchObject({ waking_count: 3, quiet_count: 10 });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    expect(await store.sendWithOutcome(envelope(to, "ping"), 1)).toMatchObject({ status: "full", reason: "full" });
  };
  const raw = (messages: readonly QueueMessage[]) => messages.map((m) => JSON.stringify(m));

  it("(R3) expired-claim recovery appends at the tail past max_size, refusing nothing", async () => {
    // Recovery only appends at the tail, so this path has a tail case only.
    const to = mailbox();
    const tasks = [envelope(to, "task"), envelope(to, "task")];
    await redis.rpush(SESSION_KEYS.queue(to), ...raw(tasks));
    const claimed = await claims.claim({ actor_id: to, session_id: "s-wl", max_batch: 2, ttl_seconds: 1, now: new Date().toISOString() });
    expect(claimed.ok && claimed.claim?.tasks.length).toBe(2);
    await redis.rpush(SESSION_KEYS.queue(to), ...raw(Array.from({ length: 10 }, () => envelope(to, "status"))));
    await delay(1100);
    expect(await claims.recoverExpired({ actor_id: to, now: new Date().toISOString() })).toEqual({ ok: true, recovered: 2, deadlettered: 0 });
    expect(idsOf((await redis.lrange(SESSION_KEYS.queue(to), -2, -1)))).toEqual(tasks.map((m) => m.id));
    await admitsByActualContents(to);
  });

  for (const where of ["head", "tail"] as const) {
    it(`(R3) DLQ requeue overshoots past max_size, refusing nothing (waking at the ${where})`, async () => {
      const to = mailbox();
      const tasks = [envelope(to, "task"), envelope(to, "task")];
      const quiet = Array.from({ length: 10 }, () => envelope(to, "status"));
      const order = where === "head" ? [...tasks, ...quiet] : [...quiet, ...tasks];
      await redis.lpush(DLQ_KEYS.list(to), ...raw(order));
      for (const message of order) expect(await claims.requeue({ actor_id: to, message_id: message.id })).toEqual({ ok: true, requeued: 1 });
      expect(idsOf(await redis.lrange(SESSION_KEYS.queue(to), 0, -1))).toEqual(order.map((m) => m.id));
      await admitsByActualContents(to);
    });

    it(`(R3) provisional forwarding overshoots past max_size, refusing nothing (waking at the ${where})`, async () => {
      const source = mailbox("prov");
      const target = mailbox("bound");
      const c = new RedisClient(null, REDIS_URL);
      try {
        await c.register("both", source);
        const tasks = [envelope(source, "task"), envelope(source, "task")];
        const quiet = Array.from({ length: 10 }, () => envelope(source, "status"));
        const order = where === "head" ? [...tasks, ...quiet] : [...quiet, ...tasks];
        await redis.rpush(SESSION_KEYS.queue(source), ...raw(order));
        const mapping = SESSION_KEYS.runtimeMailbox("pi", `rt-${randomUUID()}`);
        track(mapping);
        await redis.set(mapping, JSON.stringify({ agent: target, working_directory: "/workspace/project" }));
        expect(await c.retireProvisionalMailbox(source, target, mapping)).toBe("retired");
        expect(await redis.llen(SESSION_KEYS.queue(source))).toBe(0);
        const forwarded = (await redis.lrange(SESSION_KEYS.queue(target), 0, -1)).map((r) => JSON.parse(r) as QueueMessage);
        expect(forwarded.map((m) => m.id)).toEqual(order.map((m) => m.id));
        expect(forwarded.every((m) => m.to === target)).toBe(true);
        await admitsByActualContents(target);
      } finally {
        await c.shutdown().catch(() => undefined);
        await redis.hdel(SESSION_KEYS.registry, source);
      }
    });
  }
});
