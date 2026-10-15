/**
 * S8: the task ledger and the answered marker are written by the same
 * bounded-push script call that delivers the message, so delivery and
 * correlation cannot diverge, and an idempotent replay rewrites neither.
 */
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import type { QueueMessage } from "../src/core/types.js";
import { assertNotLiveDb } from "./helpers/redis-test-utils.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
assertNotLiveDb(REDIS_URL, "send-correlation");

describe("send-path correlation records (S8)", () => {
  let redis: Redis;
  const keys: string[] = [];
  beforeAll(() => { redis = new Redis(REDIS_URL); });
  afterAll(async () => { if (keys.length > 0) await redis.del(...keys); redis.disconnect(); });

  /** A MailboxStore whose connection records every command it issues. */
  const recorded = () => {
    const calls: string[] = [];
    const proxy = new Proxy(redis, {
      get(target, prop, receiver) {
        const value = Reflect.get(target, prop, receiver) as unknown;
        if (typeof value !== "function") return value;
        return (...args: unknown[]) => { calls.push(String(prop)); return (value as (...a: unknown[]) => unknown).apply(target, args); };
      },
    });
    return { store: new MailboxStore(proxy, proxy, 10), calls };
  };
  const names = () => {
    const a = `corr-a-${randomUUID().slice(0, 8)}`;
    const b = `corr-b-${randomUUID().slice(0, 8)}`;
    keys.push(...[a, b].flatMap((n) => [SESSION_KEYS.queue(n), SESSION_KEYS.mailboxMeta(n), SESSION_KEYS.inboxEvents(n)]));
    return { a, b };
  };
  const msg = (from: string, to: string, type: QueueMessage["type"], content: string, inReplyTo?: string): QueueMessage => {
    const id = randomUUID();
    keys.push(SESSION_KEYS.taskLedger(id), SESSION_KEYS.outstanding(from, id), SESSION_KEYS.taskAnswered(id));
    return { id, from, to, timestamp: new Date().toISOString(), type, payload: { content, ...(inReplyTo ? { in_reply_to: inReplyTo } : {}) } };
  };

  it("a task send writes the queue entry and the ledger in one script call", async () => {
    const { a, b } = names();
    const { store, calls } = recorded();
    const task = msg(a, b, "task", "first line\nsecond \"quoted\" line");
    expect(await store.send(task)).toBe(true);
    expect(calls).toEqual(["eval"]);
    expect(JSON.parse((await redis.get(SESSION_KEYS.taskLedger(task.id)))!)).toEqual({ from: a, to: b, subject: "first line second \"quoted\" line", sent_at: task.timestamp });
    expect(await redis.ttl(SESSION_KEYS.taskLedger(task.id))).toBeGreaterThan(604800 - 60);
  });

  it("an owner's reply writes the answered marker in the same script call; a non-owner's does not", async () => {
    const { a, b } = names();
    const { store, calls } = recorded();
    const task = msg(a, b, "task", "do it");
    await store.send(task);
    calls.length = 0;
    const stranger = msg("corr-stranger", a, "result", "not mine", task.id);
    await store.send(stranger);
    expect(await redis.exists(SESSION_KEYS.taskAnswered(task.id))).toBe(0);
    await store.send(msg(b, a, "result", "done", task.id));
    expect(calls).toEqual(["eval", "eval"]);
    expect(Date.parse((await redis.get(SESSION_KEYS.taskAnswered(task.id)))!)).not.toBeNaN();
    expect(await redis.exists(SESSION_KEYS.taskLedger(stranger.id))).toBe(0);
  });

  it("an idempotent duplicate leaves both records unchanged", async () => {
    const { a, b } = names();
    const { store } = recorded();
    const key = `idem-${randomUUID()}`;
    keys.push(SESSION_KEYS.idempotencyFor(a, b, key), SESSION_KEYS.idempotencyFor(b, a, key));
    const task = msg(a, b, "task", "original subject");
    expect((await store.sendIdempotent(task, a, key)).status).toBe("sent");
    const ledger = await redis.get(SESSION_KEYS.taskLedger(task.id));
    expect(ledger).not.toBeNull();
    const replay = { ...task, payload: { content: "a different subject" } };
    expect((await store.sendIdempotent(replay, a, key)).status).toBe("duplicate");
    expect(await redis.get(SESSION_KEYS.taskLedger(task.id))).toBe(ledger);

    const reply = msg(b, a, "result", "done", task.id);
    expect((await store.sendIdempotent(reply, b, key)).status).toBe("sent");
    const answered = await redis.get(SESSION_KEYS.taskAnswered(task.id));
    expect(answered).not.toBeNull();
    await redis.set(SESSION_KEYS.taskAnswered(task.id), "sentinel", "KEEPTTL");
    expect((await store.sendIdempotent(reply, b, key)).status).toBe("duplicate");
    expect(await redis.get(SESSION_KEYS.taskAnswered(task.id))).toBe("sentinel");
  });
});
