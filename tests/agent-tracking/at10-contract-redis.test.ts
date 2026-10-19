/** AT10: the same seat contract suite against the Redis store GPTQueue uses. */
import { testRedisUrl } from "../helpers/test-redis.js";
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { createRedisSeatStore } from "../../src/agent-tracking/index.js";
import { assertPrivateTestRedis } from "../helpers/redis-test-utils.js";
import { seatContract } from "./seat-contract.js";

const TEST_REDIS_URL = testRedisUrl();
assertPrivateTestRedis(TEST_REDIS_URL, "at10-contract-redis");
const RUN = `gptq:seat-test:${randomUUID()}:`;
let redis: Redis;
let n = 0;

const deletePrefix = async (prefix: string): Promise<void> => {
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 500);
    cursor = next;
    if (keys.length > 0) await redis.del(...keys);
  } while (cursor !== "0");
};

beforeAll(() => { redis = new Redis(TEST_REDIS_URL); });
afterAll(async () => { await deletePrefix(RUN); redis.disconnect(); });

// Each rig gets its own prefix so tests stay independent without flushing.
seatContract("redis store", () => createRedisSeatStore(redis, { prefix: `${RUN}${++n}:` }));

describe("redis seat store specifics", () => {
  it("reports a corrupt stored tenure as store_corrupt instead of guessing", async () => {
    const prefix = `${RUN}corrupt:`;
    const store = createRedisSeatStore(redis, { prefix });
    await redis.set(`${prefix}tenure:s1`, "{not json");
    const read = await store.read("s1");
    expect(read.ok).toBe(false);
    if (!read.ok) expect(read.error.code).toBe("store_corrupt");
  });

  it("reports a structurally invalid tenure as store_corrupt", async () => {
    const prefix = `${RUN}shape:`;
    const store = createRedisSeatStore(redis, { prefix });
    await redis.set(`${prefix}tenure:s1`, JSON.stringify({ rev: 1, tenure: { seat_id: "s1", state: "maybe", epoch: 1 } }));
    const read = await store.read("s1");
    expect(read.ok === false && read.error.code).toBe("store_corrupt");
  });

  it("rejects a held tenure at epoch 0, which no transition can produce", async () => {
    const prefix = `${RUN}epoch0:`;
    const store = createRedisSeatStore(redis, { prefix });
    const holder = { runtime: "claude", session_id: "s", host: "h", machine_id: "m", boot_id: "b", pidns: null, pid: 1, pid_start: "1" };
    await redis.set(`${prefix}tenure:s1`, JSON.stringify({
      rev: 1, tenure: { seat_id: "s1", state: "held", epoch: 0, holder, bound_at: "2030-01-01T00:00:00.000Z", lease_expires_at: "2030-01-01T06:00:00.000Z" },
    }));
    const read = await store.read("s1");
    expect(read.ok === false && read.error.code).toBe("store_corrupt");
  });

  it("reads a nil reply as absent whether the client returns null or undefined", async () => {
    const undefinedNil = createRedisSeatStore({ eval: async () => undefined }, { prefix: `${RUN}nil:` });
    expect(await undefinedNil.read("s1")).toEqual({ ok: true, stored: null });
  });

  it("S9: a wrong-typed history key fails the commit as store_corrupt before any write", async () => {
    const prefix = `${RUN}histtype:`;
    const store = createRedisSeatStore(redis, { prefix });
    await redis.set(`${prefix}history:s1`, "not a list");
    const result = await store.commit("s1", 0, { seat_id: "s1", state: "vacant", epoch: 0 }, []);
    expect(result.ok === false && result.error.code).toBe("store_corrupt");
    expect(await redis.exists(`${prefix}tenure:s1`)).toBe(0);
  });

  it("writes only under its own prefix", async () => {
    const prefix = `${RUN}scoped:`;
    const store = createRedisSeatStore(redis, { prefix });
    await store.commit("s1", 0, { seat_id: "s1", state: "vacant", epoch: 0 }, []);
    const keys = await redis.keys(`${RUN}scoped:*`);
    expect(keys.every((k) => k.startsWith(prefix))).toBe(true);
    expect(keys).toContain(`${prefix}tenure:s1`);
  });
});
