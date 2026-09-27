import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { WakeLeaseStore } from "../src/core/wake-lease.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { ActivationStore } from "../src/registered-shell/activation-store.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS, WAKE_LEASE_KEYS } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

/** Stored values that are valid JSON but not a complete record read as absent/corrupt, never throw. */
describe("stored record admission", () => {
  let redis: Redis;
  const keys: string[] = [];
  beforeAll(() => { redis = new Redis(TEST_REDIS_URL); });
  afterAll(async () => { if (keys.length) await redis.del(...keys); await redis.quit(); });

  it("treats an incomplete wake lease as absent", async () => {
    const actor = `lease-${randomUUID()}`;
    keys.push(WAKE_LEASE_KEYS.lease(actor));
    await redis.set(WAKE_LEASE_KEYS.lease(actor), JSON.stringify({ lease_id: "only-an-id" }));
    expect(await new WakeLeaseStore(redis).get(actor)).toBeNull();
  });

  it("treats a corrupt activation record as absent instead of wedging the dispatcher", async () => {
    const agent = `activation-${randomUUID()}`;
    keys.push(SESSION_KEYS.activation(agent));
    const store = new ActivationStore(redis, agent);
    for (const bad of ["{not json", JSON.stringify({ operation_id: "op", state: "exploded", message_ids: [], attempt: 1, created_at: "t" })]) {
      await redis.set(SESSION_KEYS.activation(agent), bad);
      await expect(store.current()).resolves.toBeNull();
    }
  });

  it("an unbounded receive does not stall other receives on the same store", async () => {
    const [idle, busy] = [`idle-${randomUUID()}`, `busy-${randomUUID()}`];
    keys.push(SESSION_KEYS.queue(idle), SESSION_KEYS.queue(busy), SESSION_KEYS.mailboxMeta(idle), SESSION_KEYS.mailboxMeta(busy));
    const mailbox = new MailboxStore(redis, redis.duplicate());
    const waiting = new AbortController();
    const unbounded = mailbox.receive(idle, 0, waiting.signal).catch(() => null); // blocks until a message or abort
    await new Promise((r) => setTimeout(r, 100));
    await redis.rpush(SESSION_KEYS.queue(busy), JSON.stringify({ id: "b1" }));
    const started = Date.now();
    await expect(mailbox.receive(busy, 5)).resolves.toMatchObject({ id: "b1" });
    expect(Date.now() - started).toBeLessThan(2_000);
    await redis.rpush(SESSION_KEYS.queue(idle), JSON.stringify({ id: "i1" }));
    await expect(unbounded).resolves.toMatchObject({ id: "i1" });
  });

  it("receive records the post-pop queue depth in mailbox metadata", async () => {
    const agent = `depth-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent));
    await redis.rpush(SESSION_KEYS.queue(agent), JSON.stringify({ id: "a" }), JSON.stringify({ id: "b" }));
    await new MailboxStore(redis, redis).receive(agent, 1);
    expect(await redis.hget(SESSION_KEYS.mailboxMeta(agent), "current_size")).toBe("1");
  });

  it("recovers tasks from an expired claim record that lacks actor_id", async () => {
    const agent = `recover-${randomUUID()}`;
    const claimId = `claim-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), CLAIM_KEYS.index(agent), DLQ_KEYS.list(agent));
    const task = JSON.stringify({ id: "t1", type: "task" });
    await redis.hset(CLAIM_KEYS.claims, claimId, JSON.stringify({ claim_id: claimId, tasks: [task] }));
    await redis.zadd(CLAIM_KEYS.index(agent), 1, claimId);
    const result = await new TaskClaimStore(redis).recoverExpired({ actor_id: agent, now: new Date().toISOString() });
    expect(result).toMatchObject({ ok: true, recovered: 1, deadlettered: 0 });
    expect(await redis.lrange(SESSION_KEYS.queue(agent), 0, -1)).toEqual([task]);
  });

  it("parks an unparseable received payload in the DLQ instead of throwing", async () => {
    const agent = `recv-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), DLQ_KEYS.list(agent), SESSION_KEYS.mailboxMeta(agent));
    await redis.rpush(SESSION_KEYS.queue(agent), "{not json");
    const mailbox = new MailboxStore(redis, redis);
    await expect(mailbox.receive(agent, 1)).resolves.toBeNull();
    expect(await redis.lrange(DLQ_KEYS.list(agent), 0, -1)).toEqual(["{not json"]);
  });
});
