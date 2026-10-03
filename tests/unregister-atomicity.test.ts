/**
 * RF5 regression suite: retiring a name must be one atomic step.
 *
 * `RedisClient.unregister` used to close the session, delete the registry
 * entry, mailbox and heartbeat, and then purge claims as separate awaited
 * commands; the experimental wrapper's exclusive unregister ran its Lua
 * retirement and the claim purge separately too. A new owner that
 * registered the name in between and claimed a task lost that claim (and,
 * on the MCP path, its registration and mailbox). Each test lets a new owner
 * register and claim after EVERY command the retiring connection issues, and
 * requires the new owner's state to survive.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { acquireWrapperIdentityClaim } from "../src/experimental-wrapper/identity-claim.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

type SendCommand = (command: unknown, stream?: unknown) => Promise<unknown>;

/**
 * Run `inject` after the `afterCommand`-th command sent on `conn` replies and
 * before the awaiting caller can issue its next command.
 */
const interleaveAfter = (conn: Redis, afterCommand: number, inject: () => Promise<void>) => {
  const target = conn as unknown as { sendCommand: SendCommand };
  const real = target.sendCommand.bind(conn);
  const state = { seen: 0, fired: false };
  target.sendCommand = (command, stream) => {
    const reply = real(command, stream);
    state.seen += 1;
    if (state.seen !== afterCommand) return reply;
    return reply.then(async (value) => {
      state.fired = true;
      await inject();
      return value;
    });
  };
  return state;
};

interface NewOwner {
  readonly client: RedisClient;
  readonly sessionId: string;
  readonly claimId: string;
}

describe("unregister retires a name atomically (RF5)", () => {
  let redis: Redis;
  const live: RedisClient[] = [];

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    for (const client of live.splice(0)) await client.shutdown();
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  const tracked = (): RedisClient => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    live.push(client);
    return client;
  };

  /** A second process registers `name`, receives a task, and claims it. */
  const registerNewOwner = async (name: string): Promise<NewOwner> => {
    const client = tracked();
    const { session_id } = await client.register("both", name, "new owner");
    await redis.rpush(
      SESSION_KEYS.queue(name),
      JSON.stringify({ id: "new-owner-task", from: "x", to: name, type: "task", timestamp: new Date().toISOString(), payload: { content: "do" } })
    );
    const claimed = await client.taskClaim.claim({ actor_id: name, session_id, max_batch: 1, ttl_seconds: 300, now: new Date().toISOString() });
    if (!claimed.ok || claimed.claim === null) throw new Error("new owner expected a claim");
    return { client, sessionId: session_id, claimId: claimed.claim.claim_id };
  };

  const expectClaimSurvives = async (name: string, owner: NewOwner) => {
    expect(await redis.hget(CLAIM_KEYS.claims, owner.claimId)).not.toBeNull();
    expect(await redis.zscore(CLAIM_KEYS.index(name), owner.claimId)).not.toBeNull();
  };

  it("MCP unregister never purges the state of an owner that registers the name mid-unregister", async () => {
    const name = "rf5-mcp";
    let interleavings = 0;
    for (let afterCommand = 1; afterCommand <= 50; afterCommand++) {
      await flushTestKeys(redis, TEST_REDIS_URL);
      const retiring = tracked();
      await retiring.register("both", name, "retiring owner");

      let owner: NewOwner | null = null;
      const state = interleaveAfter(retiring.adapterConnection, afterCommand, async () => {
        owner = await registerNewOwner(name);
      });
      await retiring.unregister();
      const fired = state.fired;
      if (!fired) owner = await registerNewOwner(name);
      const newOwner = owner as NewOwner | null;
      if (newOwner === null) throw new Error("new owner was not registered");

      await expectClaimSurvives(name, newOwner);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
      expect(await redis.exists(SESSION_KEYS.session(newOwner.sessionId))).toBe(1);
      expect(await redis.sismember(SESSION_KEYS.agentSessions(name), newOwner.sessionId)).toBe(1);
      expect(await redis.exists(SESSION_KEYS.mailboxMeta(name))).toBe(1);

      for (const client of live.splice(0)) await client.shutdown();
      if (!fired) break;
      interleavings += 1;
    }
    // Every command the unregister issues was followed by an interleaving.
    expect(interleavings).toBeGreaterThanOrEqual(1);
  });

  // FIX8: the purge no longer parses the discarded inbox; counters of its
  // queued messages are left to expire with their TTL, like acked ones.
  it("MCP unregister deletes exactly the retiring name's counters for claimed and dead-lettered messages, leaving queued ones to their TTL", async () => {
    const name = "rf5-counters";
    const extended = `${name}:x`;
    const retiring = tracked();
    const { session_id } = await retiring.register("both", name, "retiring owner");
    const envelope = (id: string) => JSON.stringify({ id, from: "x", to: name, type: "task", timestamp: new Date().toISOString(), payload: { content: "do" } });
    await redis.rpush(SESSION_KEYS.queue(name), envelope("claimed"), envelope("queued"));
    const claimed = await retiring.taskClaim.claim({ actor_id: name, session_id, max_batch: 1, ttl_seconds: 300, now: new Date().toISOString() });
    expect(claimed.ok && claimed.claim !== null).toBe(true);
    await redis.rpush(`gptq:dlq:${name}`, envelope("dead"));
    for (const id of ["claimed", "queued", "dead"]) {
      await redis.set(CLAIM_KEYS.recoverCount(name, id), "1", "EX", 604800);
      await redis.set(CLAIM_KEYS.recoverCount(extended, id), "1");
    }

    await retiring.unregister();

    for (const id of ["claimed", "dead"]) {
      expect(await redis.exists(CLAIM_KEYS.recoverCount(name, id))).toBe(0);
    }
    expect(await redis.ttl(CLAIM_KEYS.recoverCount(name, "queued"))).toBeGreaterThan(0);
    for (const id of ["claimed", "queued", "dead"]) {
      expect(await redis.get(CLAIM_KEYS.recoverCount(extended, id))).toBe("1");
    }
    expect(await redis.exists(SESSION_KEYS.queue(name), CLAIM_KEYS.index(name), `gptq:dlq:${name}`)).toBe(0);
  });

  it("wrapper exclusive unregister never purges the claims of an owner that registers the name mid-unregister", async () => {
    const name = "rf5-wrapper";
    let interleavings = 0;
    for (let afterCommand = 1; afterCommand <= 50; afterCommand++) {
      await flushTestKeys(redis, TEST_REDIS_URL);

      // The claim's private connection is the only one sending commands
      // while it is acquired; capture it to interleave on it later.
      const proto = Redis.prototype as unknown as { sendCommand: SendCommand };
      const realSend = proto.sendCommand;
      const senders: Redis[] = [];
      proto.sendCommand = function (this: Redis, command: unknown, stream?: unknown) {
        senders.push(this);
        return realSend.call(this, command, stream);
      } as SendCommand;
      let claim: Awaited<ReturnType<typeof acquireWrapperIdentityClaim>>;
      try {
        claim = await acquireWrapperIdentityClaim(TEST_REDIS_URL, name, false);
      } finally {
        proto.sendCommand = realSend;
      }
      const claimConn = senders[0];
      if (claimConn === undefined || senders.some((sender) => sender !== claimConn)) {
        throw new Error("claim connection not captured unambiguously");
      }

      const retiring = tracked();
      const { session_id } = await retiring.register("both", name, "wrapped runtime");
      // As the wrapper does: stop this process's timers before the atomic delete.
      retiring.forceDisconnect();

      let owner: NewOwner | null = null;
      const state = interleaveAfter(claimConn, afterCommand, async () => {
        owner = await registerNewOwner(name);
      });
      try {
        await expect(claim.unregisterExclusiveSession(session_id)).resolves.toBe("unregistered");
      } finally {
        await claim.release();
      }
      const fired = state.fired;
      if (!fired) owner = await registerNewOwner(name);
      const newOwner = owner as NewOwner | null;
      if (newOwner === null) throw new Error("new owner was not registered");

      await expectClaimSurvives(name, newOwner);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);

      for (const client of live.splice(0)) await client.shutdown();
      if (!fired) break;
      interleavings += 1;
    }
    expect(interleavings).toBeGreaterThanOrEqual(1);
  });

  // FIX3: the purge read the DLQ and inbox with LRANGE after the session,
  // registry and claims were already gone, so a wrong-typed list aborted the
  // script half way. Destructive cleanup now tolerates any key type.
  for (const path of ["MCP", "wrapper"] as const) {
    for (const corrupt of ["DLQ", "inbox"] as const) {
      it(`${path} unregister completes over a wrong-typed ${corrupt} and leaves nothing behind (FIX3)`, async () => {
        const name = `fix3-${path}-${corrupt}`.toLowerCase();
        const claim = path === "wrapper" ? await acquireWrapperIdentityClaim(TEST_REDIS_URL, name, false) : null;
        try {
          const retiring = tracked();
          const { session_id } = await retiring.register("both", name, "retiring owner");
          await redis.rpush(SESSION_KEYS.queue(name), JSON.stringify({ id: "claimed", from: "x", to: name, type: "task", timestamp: new Date().toISOString(), payload: { content: "do" } }));
          const claimed = await retiring.taskClaim.claim({ actor_id: name, session_id, max_batch: 1, ttl_seconds: 300, now: new Date().toISOString() });
          if (!claimed.ok || claimed.claim === null) throw new Error("expected a claim");
          await redis.set(CLAIM_KEYS.recoverCount(name, "claimed"), "1");
          await redis.xadd(SESSION_KEYS.inboxEvents(name), "*", "message_id", "claimed");
          await redis.xadd(SESSION_KEYS.inboxTrace(name), "*", "stage", "x");
          await redis.set(corrupt === "DLQ" ? DLQ_KEYS.list(name) : SESSION_KEYS.queue(name), "not-a-list");

          if (claim === null) {
            await retiring.unregister();
          } else {
            retiring.forceDisconnect();
            await expect(claim.unregisterExclusiveSession(session_id)).resolves.toBe("unregistered");
          }

          expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
          expect(await redis.hget(CLAIM_KEYS.claims, claimed.claim.claim_id)).toBeNull();
          const left = [
            SESSION_KEYS.session(session_id), SESSION_KEYS.lease(session_id), SESSION_KEYS.agentSessions(name),
            SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name),
            CLAIM_KEYS.index(name), DLQ_KEYS.list(name), SESSION_KEYS.inboxEvents(name), SESSION_KEYS.inboxTrace(name),
            CLAIM_KEYS.recoverCount(name, "claimed"),
          ];
          const remaining = [];
          for (const key of left) if ((await redis.exists(key)) === 1) remaining.push(key);
          expect(remaining).toEqual([]);
        } finally {
          await claim?.release();
        }
      });
    }
  }
});
