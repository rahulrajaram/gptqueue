// Core claim/recovery/dedupe semantics (acceptance criteria DL-2, DL-3, DL-7,
// WK-6 core, clock-skew safety), exercised against a real Redis.
import { testRedisUrl } from "./helpers/test-redis.js";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { TaskClaimStore, claimLuaScript } from "../src/core/task-claim-store.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { InboxEvents } from "../src/core/inbox-events.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import type { QueueMessage } from "../src/mcp-server/types.js";
import { dlqStatusResult } from "../src/mcp-server/tools/task-claim-result.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const url = testRedisUrl();
const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

const task = (id: string, to: string, from = "sender"): QueueMessage => ({
  id,
  from,
  to,
  timestamp: new Date().toISOString(),
  type: "task",
  payload: { content: id },
});

describe("claim core semantics", () => {
  let redis: Redis;
  let subscriber: Redis;
  let store: TaskClaimStore;
  let mailbox: MailboxStore;

  beforeEach(async () => {
    redis = new Redis(url);
    subscriber = new Redis(url);
    await flushTestKeys(redis, url);
    store = new TaskClaimStore(redis);
    mailbox = new MailboxStore(redis, subscriber, 10);
  });

  afterEach(async () => {
    await flushTestKeys(redis, url);
    await Promise.all([redis.quit(), subscriber.quit()]);
  });

  // Mutation caught: claims-recover.lua without wake_emit (the old RPUSH-only
  // re-queue) leaves the waiter blocked, so `woke` is null.
  it("a recovered task emits an inbox event that wakes a blocked XREAD and shows the true size", async () => {
    const actor = "recover-wake";
    await redis.rpush(SESSION_KEYS.queue(actor), JSON.stringify(task("t1", actor)));
    const claimed = await store.claim({ actor_id: actor, session_id: "s1", max_batch: 1, ttl_seconds: 1, now: new Date().toISOString() });
    expect(claimed.ok && claimed.claim !== null).toBe(true);

    const events = new InboxEvents(redis);
    const waiter = events.createWaiter();
    try {
      const last = (await redis.xrevrange(SESSION_KEYS.inboxEvents(actor), "+", "-", "COUNT", 1))[0]?.[0] ?? "0-0";
      const woke = waiter.wait(actor, last, new AbortController().signal, 5000);
      await delay(1100); // let the 1 s claim expire on the server clock
      const rec = await store.recoverExpired({ actor_id: actor, now: new Date().toISOString() });
      expect(rec).toEqual({ ok: true, recovered: 1, deadlettered: 0 });
      expect(await woke).not.toBeNull();
    } finally {
      waiter.close();
    }
    const [[, fields] = ['', []]] = await redis.xrevrange(SESSION_KEYS.inboxEvents(actor), "+", "-", "COUNT", 1);
    expect(fields).toEqual(expect.arrayContaining(["message_id", "t1", "type", "task"]));
    expect(await redis.hget(SESSION_KEYS.mailboxMeta(actor), "current_size")).toBe("1");
  });

  // Mutation caught: claims-requeue.lua without wake_emit/HSET.
  it("a DLQ requeue emits an inbox event and updates current_size, even past the send bound", async () => {
    const actor = "requeue-wake";
    for (let i = 0; i < 10; i++) await redis.rpush(SESSION_KEYS.queue(actor), JSON.stringify(task(`q${i}`, actor)));
    await redis.lpush(DLQ_KEYS.list(actor), JSON.stringify(task("dead", actor)));
    expect(await store.requeue({ actor_id: actor, message_id: "dead" })).toEqual({ ok: true, requeued: 1 });
    expect(await redis.xlen(SESSION_KEYS.inboxEvents(actor))).toBe(1);
    expect(await redis.llen(SESSION_KEYS.queue(actor))).toBe(11); // accepted work is never dropped
    expect(await redis.hget(SESSION_KEYS.mailboxMeta(actor), "current_size")).toBe("11");
  });

  it("recovery does not wake for a reply whose outstanding owner is someone else", async () => {
    const actor = "reply-owner";
    const reply = { ...task("r1", actor, "peer"), type: "result" as const, payload: { content: "x", in_reply_to: "req-1" } };
    await redis.set(SESSION_KEYS.outstanding(actor, "req-1"), "not-peer");
    await redis.lpush(DLQ_KEYS.list(actor), JSON.stringify(reply));
    await store.requeue({ actor_id: actor, message_id: "r1" });
    expect(await redis.xlen(SESSION_KEYS.inboxEvents(actor))).toBe(0);
    await redis.set(SESSION_KEYS.outstanding(actor, "req-2"), "peer");
    await redis.lpush(DLQ_KEYS.list(actor), JSON.stringify({ ...reply, id: "r2", payload: { content: "y", in_reply_to: "req-2" } }));
    await store.requeue({ actor_id: actor, message_id: "r2" });
    expect(await redis.xlen(SESSION_KEYS.inboxEvents(actor))).toBe(1);
  });

  // Mutation caught: honoring the caller's `now` (the old Date.parse(now)
  // cutoff) recovers the live claim, so recovered becomes 1.
  it("a skewed client clock cannot expire, stamp or refuse renewal of a live claim", async () => {
    const actor = "skew";
    await redis.rpush(SESSION_KEYS.queue(actor), JSON.stringify(task("s1", actor)));
    const farPast = "2001-01-01T00:00:00.000Z";
    const farFuture = "2099-01-01T00:00:00.000Z";
    const claimed = await store.claim({ actor_id: actor, session_id: "s", max_batch: 1, ttl_seconds: 600, now: farPast });
    if (!claimed.ok || claimed.claim === null) throw new Error("expected a claim");
    const stamped = Date.parse(claimed.claim.claimed_at);
    expect(Math.abs(stamped - Date.now())).toBeLessThan(60_000);

    const rec = await store.recoverExpired({ actor_id: actor, now: farFuture });
    expect(rec).toEqual({ ok: true, recovered: 0, deadlettered: 0 });
    expect(await redis.zcard(CLAIM_KEYS.index(actor))).toBe(1);

    const renewed = await store.renew({ claim_id: claimed.claim.claim_id, actor_id: actor, session_id: "s", ttl_seconds: 600, budget_seconds: 86400, now: farFuture });
    expect(renewed.ok).toBe(true);
  });

  // Mutation caught: a sender-only dedupe key makes the second recipient's
  // send a "duplicate" of the first message, so b2's inbox stays empty.
  it("one idempotency key to two recipients enqueues two messages; a retry to the same recipient dedupes", async () => {
    const first = await mailbox.sendIdempotent(task("m1", "b1", "a"), "a", "k");
    const other = await mailbox.sendIdempotent(task("m2", "b2", "a"), "a", "k");
    const retry = await mailbox.sendIdempotent(task("m3", "b1", "a"), "a", "k");
    expect(first).toEqual({ status: "sent", messageId: "m1" });
    expect(other).toEqual({ status: "sent", messageId: "m2" });
    expect(retry).toEqual({ status: "duplicate", messageId: "m1" });
    expect(await redis.llen(SESSION_KEYS.queue("b1"))).toBe(1);
    expect(await redis.llen(SESSION_KEYS.queue("b2"))).toBe(1);
  });

  // Mutation caught: the old unconditional LTRIM with no INCRBY leaves the
  // counter absent, so dlq_status reports dropped 0.
  it("DLQ overflow increments the drop counter that dlq_status reports", async () => {
    const actor = "dlq-drop";
    const recover = claimLuaScript("claims-recover.lua");
    const caller = new TaskClaimStore(redis, { clock: "caller" });
    const now = "2030-01-01T00:00:00.000Z";
    for (const id of ["d1", "d2", "d3"]) {
      await redis.rpush(SESSION_KEYS.queue(actor), JSON.stringify(task(id, actor)));
      await caller.claim({ actor_id: actor, session_id: "s", max_batch: 1, ttl_seconds: 1, now });
      await redis.eval(recover, 7,
        CLAIM_KEYS.index(actor), CLAIM_KEYS.claims, SESSION_KEYS.queue(actor), DLQ_KEYS.list(actor),
        SESSION_KEYS.inboxEvents(actor), SESSION_KEYS.mailboxMeta(actor), DLQ_KEYS.dropped(actor),
        Date.parse("2030-01-01T00:00:05.000Z"), 0, 2, 604800);
    }
    expect(await redis.lrange(DLQ_KEYS.list(actor), 0, -1)).toHaveLength(2);
    const entries = await store.deadLetterEntries({ actor_id: actor });
    expect(entries.ok && entries.dropped).toBe(1);
    const tool = dlqStatusResult(entries);
    const text = (tool as { content: { text: string }[] }).content[0]!.text;
    expect(JSON.parse(text)).toMatchObject({ status: "ok", count: 2, dropped: 1 });
  });

  // Mutation caught: falling back to `dropped: 0` for an unparsable counter
  // (the previous Number.isSafeInteger(...) ? dropped : 0) reports ok.
  it("a corrupt DLQ drop counter is reported as store_corrupt, never as 0 dropped", async () => {
    const actor = "dlq-corrupt";
    for (const bad of ["abc", "1.5", "-1", "99999999999999999999"]) {
      await redis.set(DLQ_KEYS.dropped(actor), bad);
      const result = await store.deadLetterEntries({ actor_id: actor });
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.error.code).toBe("store_corrupt");
      const text = (dlqStatusResult(result) as { content: { text: string }[] }).content[0]!.text;
      expect(JSON.parse(text)).toMatchObject({ status: "error", error: { code: "store_corrupt" } });
    }
  });
});
