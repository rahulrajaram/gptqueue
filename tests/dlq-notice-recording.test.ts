/** Baseline-compatible behavioral oracles: real recovery Lua, no notifier API. */
import { Redis } from "ioredis";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { claimLuaScript } from "../src/core/task-claim-store.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS, SYSTEM_SENDER } from "../src/core/keys.js";
import { testRedisUrl } from "./helpers/test-redis.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const url = testRedisUrl();
const redis = new Redis(url);
const actor = "notice-recording";
const pending = `gptq:dlq-notice-pending:${actor}`;
const overflow = `gptq:dlq-notice-overflow:${actor}`;
const window = `gptq:dlq-notice-window:${actor}`;
const script = claimLuaScript("claims-recover.lua");
const payload = (id: string, from = "sender") => JSON.stringify({
  id, from, to: actor, timestamp: "2030-01-01T00:00:00.000Z", type: "task", payload: { content: "work" },
});
const seed = async (tasks: string[]) => {
  await redis.hset(CLAIM_KEYS.claims, "expired", JSON.stringify({
    claim_id: "expired", actor_id: actor, session_id: "s", tasks,
    claimed_at: "2030-01-01T00:00:00.000Z", expires_at: "2030-01-01T00:00:01.000Z",
  }));
  await redis.zadd(CLAIM_KEYS.index(actor), 1, "expired");
};
const recover = () => redis.eval(script, 7,
  CLAIM_KEYS.index(actor), CLAIM_KEYS.claims, SESSION_KEYS.queue(actor), DLQ_KEYS.list(actor),
  SESSION_KEYS.inboxEvents(actor), SESSION_KEYS.mailboxMeta(actor), DLQ_KEYS.dropped(actor),
  1000, 0, 1000, 604800);
const snapshot = async () => {
  const keys = (await redis.keys("gptq:*")).sort();
  return Promise.all(keys.map(async (key) => [key, (await redis.dumpBuffer(key))?.toString("base64")]));
};
beforeEach(() => flushTestKeys(redis, url));
afterAll(() => redis.quit());

describe("atomic DLQ notice recording", () => {
  it("records an id in the same recovery that dead-letters it, with no TTL", async () => {
    await seed([payload("work-1")]);
    expect(await recover()).toEqual([0, 1, 0]);
    expect(await redis.smembers(pending)).toEqual(["work-1"]);
    expect(await redis.ttl(pending)).toBe(-1);
    expect(await redis.lrange(DLQ_KEYS.list(actor), 0, -1)).toEqual([payload("work-1")]);
    expect(await redis.hlen(CLAIM_KEYS.claims)).toBe(0);
  });

  it("dead-letters a system envelope without recursively recording it", async () => {
    await seed([payload("system-task", SYSTEM_SENDER)]);
    expect(await recover()).toEqual([0, 1, 0]);
    expect(await redis.exists(pending, overflow)).toBe(0);
  });

  it("caps the set at 256, increments durable overflow, and coalesces duplicate ids", async () => {
    await seed(Array.from({ length: 260 }, (_, i) => payload(`m-${i}`)));
    expect(await recover()).toEqual([0, 260, 0]);
    expect(await redis.scard(pending)).toBe(256);
    expect(await redis.get(overflow)).toBe("4");
    expect(await redis.ttl(overflow)).toBe(-1);
    await seed([payload("m-0")]);
    await recover();
    expect(await redis.get(overflow)).toBe("4");
  });

  for (const [label, corrupt] of [
    ["pending type", () => redis.set(pending, "bad")],
    ["overflow type", () => redis.sadd(overflow, "bad")],
    ["overflow numeric", () => redis.set(overflow, "NaN")],
    ["overflow capacity", () => redis.set(overflow, "9007199254740991")],
    ["window type", () => redis.sadd(window, "bad")],
    ["window numeric", () => redis.set(window, "bad")],
  ] as const) {
    it(`refuses ${label} before ANY write`, async () => {
      await seed([payload("work-1")]);
      await corrupt();
      const before = await snapshot();
      expect((await recover() as [number, string])[0]).toBe(-1);
      expect(await snapshot()).toEqual(before);
    });
  }
});
