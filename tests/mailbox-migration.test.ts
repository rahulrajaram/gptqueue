/**
 * F2 regression suite: mailbox migration must be atomic and recoverable.
 *
 * The legacy rename path closed the old session first and then moved
 * messages with a per-message LPOP/RPUSH loop — a failure or crash between
 * the two permanently lost that message. These tests pin the new contract:
 *   - MailboxStore.migrateMessages is one all-or-nothing transfer that
 *     preserves FIFO order and appends to the destination tail;
 *   - RedisClient.register keeps the old session/registry state until the
 *     new session exists AND the transfer has completed, rolling the new
 *     session back on transfer failure so the old name is untouched.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SessionStore } from "../src/core/session-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

/** Raw seeded message payloads with stable, assertable identities. */
const msg = (i: number) => JSON.stringify({ id: `m${i}`, type: "task" });
const contents = async (redis: Redis, agent: string): Promise<string[]> =>
  (await redis.lrange(SESSION_KEYS.queue(agent), 0, -1)) as string[];

describe("MailboxStore.migrateMessages (atomic transfer, F2)", () => {
  let redis: Redis;
  let store: MailboxStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new MailboxStore(redis, redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("moves every message in order, appending to the destination tail", async () => {
    await redis.rpush(SESSION_KEYS.queue("src"), msg(1), msg(2), msg(3));
    await redis.rpush(SESSION_KEYS.queue("dst"), JSON.stringify({ id: "d1", type: "task" }));

    const count = await store.migrateMessages("src", "dst");

    expect(count).toBe(3);
    expect(await contents(redis, "src")).toEqual([]);
    expect(await contents(redis, "dst")).toEqual([
      JSON.stringify({ id: "d1", type: "task" }),
      msg(1),
      msg(2),
      msg(3),
    ]);
  });

  it("returns 0 and touches nothing when the source is empty", async () => {
    await redis.rpush(SESSION_KEYS.queue("dst"), msg(9));
    const count = await store.migrateMessages("empty-src", "dst");
    expect(count).toBe(0);
    expect(await contents(redis, "dst")).toEqual([msg(9)]);
  });

  it("moves a large mailbox in exact order (chunked RPUSH path)", async () => {
    const N = 2000; // exercises the script's bounded-chunk loop
    const seeded: string[] = [];
    for (let i = 1; i <= N; i++) seeded.push(msg(i));
    await redis.rpush(SESSION_KEYS.queue("big"), ...seeded);

    const count = await store.migrateMessages("big", "big-dst");

    expect(count).toBe(N);
    expect(await contents(redis, "big")).toEqual([]);
    expect(await contents(redis, "big-dst")).toEqual(seeded);
  });

  it("leaves the source exactly intact when the transfer call fails", async () => {
    await redis.rpush(SESSION_KEYS.queue("src"), msg(1), msg(2));
    await redis.rpush(SESSION_KEYS.queue("dst"), msg(3));

    // Force the eval carrying the migration script to fail, as a connection
    // loss would. The all-or-nothing contract: nothing has moved.
    const internals = store as unknown as { redis: Redis };
    const realRedis = internals.redis;
    internals.redis = {
      eval: async () => {
        throw new Error("connection lost during eval");
      },
    } as unknown as Redis;

    await expect(store.migrateMessages("src", "dst")).rejects.toThrow(
      /connection lost/
    );

    internals.redis = realRedis;
    expect(await contents(redis, "src")).toEqual([msg(1), msg(2)]);
    expect(await contents(redis, "dst")).toEqual([msg(3)]);
  });
});

describe("RedisClient.register rename path (F2: no message loss)", () => {
  let redis: Redis;
  let client: RedisClient;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    client = new RedisClient(null, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await client.shutdown();
    await redis.quit();
  });

  const seedOldMailbox = async (n: number): Promise<void> => {
    const seeded: string[] = [];
    for (let i = 1; i <= n; i++) seeded.push(msg(i));
    await redis.rpush(SESSION_KEYS.queue("old-name"), ...seeded);
  };

  it("happy rename: messages land under the new name in order and the old identity is retired", async () => {
    const first = await client.register("both", "old-name", "first");
    await seedOldMailbox(3);

    const second = await client.register("both", "new-name", "second");

    expect(second.name).toBe("new-name");
    expect(client.agentName).toBe("new-name");
    expect(client.sessionId).toBe(second.session_id);
    expect(await contents(redis, "old-name")).toEqual([]);
    expect(await contents(redis, "new-name")).toEqual([msg(1), msg(2), msg(3)]);
    // Old session retired; new session live.
    expect(await client.sessions.getSession(first.session_id)).toBeNull();
    expect(await client.sessions.getSession(second.session_id)).not.toBeNull();
    expect(await redis.hexists(SESSION_KEYS.registry, "old-name")).toBe(0);
    expect(await redis.hexists(SESSION_KEYS.registry, "new-name")).toBe(1);
    expect(await redis.exists(SESSION_KEYS.heartbeat("old-name"))).toBe(0);
  });

  it("createSession failure changes nothing: old session, mailbox, and registry intact", async () => {
    const first = await client.register("both", "old-name", "first");
    await seedOldMailbox(2);

    const sessions = client.sessions as unknown as SessionStore & {
      createSession: SessionStore["createSession"];
    };
    const realCreate = sessions.createSession.bind(sessions);
    sessions.createSession = (() =>
      Promise.reject(new Error("session creation failed"))) as typeof realCreate;

    await expect(
      client.register("both", "new-name", "second")
    ).rejects.toThrow(/session creation failed/);

    sessions.createSession = realCreate;

    // Nothing changed: the client is still the old registration.
    expect(client.agentName).toBe("old-name");
    expect(client.sessionId).toBe(first.session_id);
    expect(await contents(redis, "old-name")).toEqual([msg(1), msg(2)]);
    expect(await contents(redis, "new-name")).toEqual([]);
    expect(await client.sessions.getSession(first.session_id)).not.toBeNull();
    expect(await redis.hexists(SESSION_KEYS.registry, "old-name")).toBe(1);
    expect(await redis.hexists(SESSION_KEYS.registry, "new-name")).toBe(0);
  });

  it("migration failure rolls the new session back and leaves every message under the old name", async () => {
    const first = await client.register("both", "old-name", "first");
    await seedOldMailbox(3);

    // Capture the session the register attempt creates before migration.
    const sessions = client.sessions as unknown as SessionStore & {
      createSession: SessionStore["createSession"];
    };
    const realCreate = sessions.createSession.bind(sessions);
    let createdSessionId: string | null = null;
    sessions.createSession = (async (...args: Parameters<SessionStore["createSession"]>) => {
      const record = await realCreate(...args);
      createdSessionId = record.session_id;
      return record;
    }) as typeof realCreate;

    const internals = client as unknown as { mailbox: MailboxStore };
    const realMigrate = internals.mailbox.migrateMessages.bind(internals.mailbox);
    internals.mailbox.migrateMessages = (async () => {
      throw new Error("migration failed");
    }) as typeof realMigrate;

    await expect(
      client.register("both", "new-name", "second")
    ).rejects.toThrow(/migration failed/);

    sessions.createSession = realCreate;
    internals.mailbox.migrateMessages = realMigrate;

    // Every message is still exactly under the old name, in order.
    expect(await contents(redis, "old-name")).toEqual([msg(1), msg(2), msg(3)]);
    expect(await contents(redis, "new-name")).toEqual([]);
    // The old identity is untouched.
    expect(client.agentName).toBe("old-name");
    expect(client.sessionId).toBe(first.session_id);
    expect(await client.sessions.getSession(first.session_id)).not.toBeNull();
    expect(await redis.hexists(SESSION_KEYS.registry, "old-name")).toBe(1);
    // The just-created replacement session was rolled back.
    expect(createdSessionId).not.toBeNull();
    expect(
      await client.sessions.getSession(createdSessionId as string)
    ).toBeNull();
    expect(await redis.smembers(SESSION_KEYS.agentSessions("new-name"))).toEqual(
      []
    );
    expect(await redis.hexists(SESSION_KEYS.registry, "new-name")).toBe(0);

    // Recovery: a retry of the rename completes cleanly.
    const retried = await client.register("both", "new-name", "second");
    expect(retried.name).toBe("new-name");
    expect(await contents(redis, "new-name")).toEqual([msg(1), msg(2), msg(3)]);
  });
});
