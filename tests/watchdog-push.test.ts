/**
 * X3 re-plan: rate admission and enqueue are one atomic script on Redis TIME,
 * composed from bounded-push-idempotent.lua so it cannot drift from the normal
 * send path.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { SESSION_KEYS, SYSTEM_SENDER } from "../src/core/keys.js";
import { composeWatchdogPush } from "../src/core/watchdog-push.js";
import { LUA_DIR } from "../src/core/stored-read.js";
import type { QueueMessage } from "../src/core/types.js";
import { assertPrivateTestRedis } from "./helpers/redis-test-utils.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "watchdog-push");
const HOUR = 3_600_000;

describe("watchdog send script (rate admission + enqueue in one script)", () => {
  let redis: Redis;
  const keys: string[] = [];
  beforeAll(() => { redis = new Redis(REDIS_URL); });
  afterAll(async () => { if (keys.length > 0) await redis.del(...keys); redis.disconnect(); });

  const recipient = () => {
    const to = `wd-push-${randomUUID().slice(0, 8)}`;
    keys.push(SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to), SESSION_KEYS.watchdogSent(to));
    return to;
  };
  const task = (to: string, from = SYSTEM_SENDER): QueueMessage => {
    const id = randomUUID();
    keys.push(SESSION_KEYS.outstanding(from, id), SESSION_KEYS.taskLedger(id));
    return { id, from, to, timestamp: "2030-01-01T00:00:00.000Z", type: "task", payload: { content: "[gptqueue watchdog] reminder\nline", metadata: { watchdog: { rule: 1 } } } };
  };
  const send = (store: MailboxStore, message: QueueMessage, fireKey: string, cap = 6) => {
    keys.push(SESSION_KEYS.idempotencyFor(message.from, message.to, `watchdog:${fireKey}`));
    return store.sendWatchdogTask(message, message.from, `watchdog:${fireKey}`, { fireKey, cap }, 1);
  };
  const redisNowMs = async (): Promise<number> => { const [s, us] = await redis.time(); return Number(s) * 1000 + Math.floor(Number(us) / 1000); };

  it("is composed from bounded-push-idempotent.lua (fails loudly if its anchors drift)", () => {
    const base = readFileSync(join(LUA_DIR, "bounded-push-idempotent.lua"), "utf8");
    const composed = composeWatchdogPush(base);
    expect(composed).toContain(base.split("\n").find((l) => l.startsWith("redis.call('RPUSH'"))!);
    expect(composed).toContain("redis.call('TIME')");
    expect(() => composeWatchdogPush(base.replace("return {1, ARGV[3]}", "return {1}"))).toThrow(/anchor/u);
  });

  it("never admits more than cap enqueues per recipient in a rolling hour, counted by Redis TIME", async () => {
    const to = recipient();
    const store = new MailboxStore(redis, redis, 50);
    const results = [];
    for (let i = 0; i < 7; i += 1) results.push((await send(store, task(to), `remind:lead-${i}:t${i}`)).status);
    expect(results).toEqual(["sent", "sent", "sent", "sent", "sent", "sent", "limited"]);
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(6);
    const scores = (await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, -1, "WITHSCORES")).filter((_, i) => i % 2 === 1).map(Number);
    const now = await redisNowMs();
    for (const score of scores) expect(Math.abs(now - score)).toBeLessThan(60_000); // Redis TIME, not a caller clock
    // An entry that has aged past the hour (by Redis TIME) frees exactly one slot.
    const [oldest] = await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, 0);
    await redis.zadd(SESSION_KEYS.watchdogSent(to), now - HOUR - 1, oldest!);
    expect((await send(store, task(to), "remind:lead-7:t7")).status).toBe("sent");
    expect((await send(store, task(to), "remind:lead-8:t8")).status).toBe("limited");
  });

  it("limited changes nothing: no queue entry, ledger, outstanding, idempotency record or rate entry", async () => {
    const to = recipient();
    const store = new MailboxStore(redis, redis, 50);
    for (let i = 0; i < 2; i += 1) await send(store, task(to), `remind:l${i}:t${i}`, 2);
    const before = await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, -1, "WITHSCORES");
    const refused = task(to);
    expect((await send(store, refused, "remind:l9:t9", 2)).status).toBe("limited");
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(2);
    expect(await redis.exists(SESSION_KEYS.taskLedger(refused.id), SESSION_KEYS.outstanding(SYSTEM_SENDER, refused.id),
      SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, to, "watchdog:remind:l9:t9"))).toBe(0);
    expect(await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, -1, "WITHSCORES")).toEqual(before);
    expect(await redis.xlen(SESSION_KEYS.inboxEvents(to))).toBe(2);
  });

  it("a duplicate retry (lost ack) adds no queue entry and no rate entry", async () => {
    const to = recipient();
    const store = new MailboxStore(redis, redis, 50);
    const message = task(to);
    expect((await send(store, message, "remind:l:t")).status).toBe("sent");
    const before = await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, -1, "WITHSCORES");
    expect(await send(store, { ...message, id: randomUUID() }, "remind:l:t")).toEqual({ status: "duplicate", messageId: message.id });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(1);
    expect(await redis.zrange(SESSION_KEYS.watchdogSent(to), 0, -1, "WITHSCORES")).toEqual(before);
  });

  it("full is a definite non-delivery that changes nothing", async () => {
    const to = recipient();
    const store = new MailboxStore(redis, redis, 1);
    // A reminder is a task: only waking mail fills its quota (waking admission).
    await redis.rpush(SESSION_KEYS.queue(to), JSON.stringify(task(to)));
    const message = task(to);
    expect(await send(store, message, "remind:f:t")).toEqual({ status: "full", messageId: message.id, reason: "waking_full" });
    expect(await redis.zcard(SESSION_KEYS.watchdogSent(to))).toBe(0);
    expect(await redis.exists(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, to, "watchdog:remind:f:t"), SESSION_KEYS.taskLedger(message.id))).toBe(0);
  });

  it("concurrent sends at cap - 1 are atomic: exactly one is admitted", async () => {
    const to = recipient();
    const a = new MailboxStore(redis, redis, 50);
    const other = new Redis(REDIS_URL);
    try {
      const b = new MailboxStore(other, other, 50);
      for (let i = 0; i < 5; i += 1) await send(a, task(to), `remind:c${i}:t`);
      const results = await Promise.all([send(a, task(to), "remind:x:t"), send(b, task(to), "remind:y:t")]);
      expect(results.map((r) => r.status).sort()).toEqual(["limited", "sent"]);
      expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(6);
      expect(await redis.zcard(SESSION_KEYS.watchdogSent(to))).toBe(6);
    } finally {
      other.disconnect();
    }
  });

  it("leaves exactly the same records as a normal idempotent task send", async () => {
    const store = new MailboxStore(redis, redis, 50);
    const plainTo = recipient();
    const watchTo = recipient();
    const base = task(plainTo, "wd-equiv-sender");
    const plain = base;
    const watch: QueueMessage = { ...base, id: randomUUID(), to: watchTo };
    keys.push(SESSION_KEYS.outstanding("wd-equiv-sender", watch.id), SESSION_KEYS.taskLedger(watch.id),
      SESSION_KEYS.idempotencyFor("wd-equiv-sender", plainTo, "k"), SESSION_KEYS.idempotencyFor("wd-equiv-sender", watchTo, "k"));
    expect((await store.sendIdempotent(plain, "wd-equiv-sender", "k")).status).toBe("sent");
    expect((await store.sendWatchdogTask(watch, "wd-equiv-sender", "k", { fireKey: "remind:e:t", cap: 6 }, 1)).status).toBe("sent");

    const entry = async (to: string) => JSON.parse((await redis.lrange(SESSION_KEYS.queue(to), 0, -1))[0]!) as QueueMessage;
    expect({ ...(await entry(watchTo)), id: "", to: "" }).toEqual({ ...(await entry(plainTo)), id: "", to: "" });
    const event = async (to: string) => { const [[, flat]] = await redis.xrange(SESSION_KEYS.inboxEvents(to), "-", "+") as [[string, string[]]]; return Object.fromEntries(flat.flatMap((v, i) => (i % 2 === 0 ? [[v, flat[i + 1]]] : []))); };
    expect({ ...(await event(watchTo)), message_id: "", to: "" }).toEqual({ ...(await event(plainTo)), message_id: "", to: "" });
    expect(await redis.get(SESSION_KEYS.outstanding("wd-equiv-sender", watch.id))).toBe(watchTo);
    expect(await redis.get(SESSION_KEYS.outstanding("wd-equiv-sender", plain.id))).toBe(plainTo);
    const ledger = async (id: string) => JSON.parse((await redis.get(SESSION_KEYS.taskLedger(id)))!) as Record<string, string>;
    expect({ ...(await ledger(watch.id)), to: "" }).toEqual({ ...(await ledger(plain.id)), to: "" });
    expect(await redis.get(SESSION_KEYS.idempotencyFor("wd-equiv-sender", watchTo, "k"))).toBe(watch.id);
    expect(await redis.get(SESSION_KEYS.idempotencyFor("wd-equiv-sender", plainTo, "k"))).toBe(plain.id);
    expect(await redis.hget(SESSION_KEYS.mailboxMeta(watchTo), "current_size")).toBe(await redis.hget(SESSION_KEYS.mailboxMeta(plainTo), "current_size"));
    const ttls = await Promise.all([redis.ttl(SESSION_KEYS.taskLedger(watch.id)), redis.ttl(SESSION_KEYS.taskLedger(plain.id))]);
    expect(Math.abs(ttls[0] - ttls[1])).toBeLessThanOrEqual(1);
  });
});
