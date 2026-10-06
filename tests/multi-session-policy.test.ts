import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { spawnSync } from "node:child_process";
import { SessionStore, pidNamespaceId } from "../src/core/session-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { GptQueueError } from "../src/mcp-server/errors.js";
import { sessionTag } from "../src/core/session-tag.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";


/** A pid that existed and has exited on this host. */
function deadPid(): number {
  const child = spawnSync(process.execPath, ["-e", "process.stdout.write(String(process.pid))"]);
  return Number(child.stdout.toString());
}

async function heldError(promise: Promise<unknown>): Promise<GptQueueError> {
  const error = await promise.then(() => null, (e: unknown) => e);
  expect(error).toBeInstanceOf(GptQueueError);
  return error as GptQueueError;
}

/**
 * Multi-session policy (NXT-020, narrowed by D2): sharing a name is opt-in
 * (`shared: true` on both registrants). Under the opt-in:
 * - Any session for an agent can send messages on that agent's behalf
 * - All sessions share a single mailbox queue (last-writer-wins for send)
 * - Any session can consume from the shared mailbox
 * - Agent is online if ANY session has a live lease
 */
describe("Multi-session policy (NXT-020)", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup, TEST_REDIS_URL);
    await cleanup.quit();
  });

  it("two sessions for the same agent can both send messages", async () => {
    const sessionA = new RedisClient(null, TEST_REDIS_URL);
    const sessionB = new RedisClient(null, TEST_REDIS_URL);
    const receiver = new RedisClient(null, TEST_REDIS_URL);

    await sessionA.register("both", "multi-sender", "session A", undefined, { shared: true });
    await sessionB.register("both", "multi-sender", "session B", undefined, { shared: true });
    await receiver.register("consumer", "multi-receiver", "receives");

    // Both sessions can send on behalf of multi-sender
    await sessionA.sendMessage({
      id: "ms-1",
      from: "multi-sender",
      to: "multi-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "from session A" },
    });

    await sessionB.sendMessage({
      id: "ms-2",
      from: "multi-sender",
      to: "multi-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "from session B" },
    });

    const depth = await receiver.getQueueDepth();
    expect(depth).toBe(2);

    // Receiver gets both messages
    const msg1 = await receiver.receiveMessage(2);
    const msg2 = await receiver.receiveMessage(2);
    expect(msg1).not.toBeNull();
    expect(msg2).not.toBeNull();

    const contents = [msg1!.payload.content, msg2!.payload.content].sort();
    expect(contents).toEqual(["from session A", "from session B"]);

    await sessionA.shutdown();
    await sessionB.shutdown();
    await receiver.shutdown();
  });

  it("closing one session keeps agent online via the other session", async () => {
    const sessionA = new RedisClient(null, TEST_REDIS_URL);
    const sessionB = new RedisClient(null, TEST_REDIS_URL);

    await sessionA.register("both", "dual-session", "session A", undefined, { shared: true });
    await sessionB.register("both", "dual-session", "session B", undefined, { shared: true });

    // Both sessions active -- agent is online
    let agents = await sessionA.listAgents();
    let found = agents.find((a) => a.name === "dual-session");
    expect(found?.online).toBe(true);

    // Close session A
    await sessionA.closeCurrentSession();

    // Agent should still be online via session B
    agents = await sessionB.listAgents();
    found = agents.find((a) => a.name === "dual-session");
    expect(found?.online).toBe(true);

    await sessionA.shutdown();
    await sessionB.shutdown();
  });
});

describe("Exclusive agent names (D2, ID-1, ID-5)", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  const client = () => {
    const c = new RedisClient(null, TEST_REDIS_URL);
    clients.push(c);
    return c;
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    for (const c of clients.splice(0)) await c.shutdown().catch(() => {});
    delete process.env.GPTQ_AGENT_SHARED;
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("refuses a second live claimant with AGENT_NAME_HELD and the holder's session tag", async () => {
    const holder = await client().register("both", "excl-agent", "first");
    const error = await heldError(client().register("both", "excl-agent", "second"));
    expect(error.code).toBe("AGENT_NAME_HELD");
    // A live holder registered by current code carries pid/hostname/pidns, so its liveness is
    // verifiable, and its lease has most of the 30 s TTL left.
    expect(error.details).toEqual({
      name: "excl-agent",
      holder_session_tag: sessionTag(holder.session_id),
      holder_liveness_verifiable: true,
      holder_lease_ttl_ms: expect.any(Number),
    });
    const ttl = (error.details as { holder_lease_ttl_ms: number }).holder_lease_ttl_ms;
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(30_000);
    expect(await redis.smembers(SESSION_KEYS.agentSessions("excl-agent"))).toEqual([holder.session_id]);
  });

  it("never discloses a credential usable by reconnectSession in a refused register (SEC-F1)", async () => {
    const holder = await client().register("both", "excl-secret", "first");
    const error = await heldError(client().register("both", "excl-secret", "second"));
    const surface = JSON.stringify({ message: error.message, details: error.details });
    expect(surface).not.toContain(holder.session_id);
    // Every string the refusal carries, and every token in the message, must fail as a session id.
    const strings = new Set<string>([
      ...Object.values(error.details ?? {}).filter((v): v is string => typeof v === "string"),
      ...error.message.split(/[\s()'.,]+/).filter(Boolean),
    ]);
    const probe = client();
    for (const s of strings) {
      await expect(probe.reconnectSession(s)).rejects.toThrow();
    }
  });

  it("records pid and hostname on the session", async () => {
    const { session_id } = await client().register("both", "excl-meta", "meta");
    const [pid, host, shared] = await redis.hmget(SESSION_KEYS.session(session_id), "pid", "hostname", "shared");
    expect(Number(pid)).toBe(process.pid);
    expect(host).toBeTruthy();
    expect(shared).toBe("0");
  });

  it("takes over a dead-pid holder atomically: of two racing claimants exactly one wins", async () => {
    const store = new SessionStore(redis);
    const old = await store.createSession("excl-dead", "both");
    await redis.hset(SESSION_KEYS.session(old.session_id), "pid", String(deadPid()));
    await redis.rpush(SESSION_KEYS.queue("excl-dead"), JSON.stringify({ id: "kept" }));

    const results = await Promise.allSettled([
      client().register("both", "excl-dead", "a"),
      client().register("both", "excl-dead", "b"),
    ]);
    const won = results.filter((r) => r.status === "fulfilled");
    const lost = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    expect(won).toHaveLength(1);
    expect(lost).toHaveLength(1);
    expect((lost[0]!.reason as GptQueueError).code).toBe("AGENT_NAME_HELD");

    const winner = (won[0] as PromiseFulfilledResult<{ session_id: string }>).value.session_id;
    expect(await redis.smembers(SESSION_KEYS.agentSessions("excl-dead"))).toEqual([winner]);
    expect(await redis.exists(SESSION_KEYS.session(old.session_id))).toBe(0);
    expect(await redis.lrange(SESSION_KEYS.queue("excl-dead"), 0, -1)).toEqual([JSON.stringify({ id: "kept" })]);
    const audit = await redis.xrange(SESSION_KEYS.identityAudit, "-", "+");
    expect(audit).toHaveLength(1);
    const fields = audit[0]![1];
    expect(await redis.hget(SESSION_KEYS.session(winner), "pidns")).toBe(pidNamespaceId() ?? "");
    expect(fields).toEqual(expect.arrayContaining(["name_takeover", old.session_id, winner, "holder_pid_dead"]));
  });

  it("takes over a holder whose lease expired", async () => {
    const store = new SessionStore(redis);
    const old = await store.createSession("excl-expired", "both");
    await redis.del(SESSION_KEYS.lease(old.session_id));
    const { session_id } = await client().register("both", "excl-expired", "new");
    expect(await redis.smembers(SESSION_KEYS.agentSessions("excl-expired"))).toEqual([session_id]);
    const audit = await redis.xrange(SESSION_KEYS.identityAudit, "-", "+");
    expect(audit).toHaveLength(1);
    expect(audit[0]![1]).toEqual(
      expect.arrayContaining(["name_takeover", old.session_id, session_id, "lease_expired"])
    );
  });

  it("treats a dead-looking pid in another PID namespace on the same hostname as live", async () => {
    const store = new SessionStore(redis);
    const old = await store.createSession("excl-pidns", "both");
    await redis.hset(SESSION_KEYS.session(old.session_id), "pid", String(deadPid()), "pidns", "pid:[1]");
    const error = await heldError(client().register("both", "excl-pidns", "claimant"));
    expect(error.code).toBe("AGENT_NAME_HELD");
    expect(await redis.smembers(SESSION_KEYS.agentSessions("excl-pidns"))).toEqual([old.session_id]);
  });

  it("refuses a shared/non-shared mismatch in both directions", async () => {
    await client().register("both", "excl-shared", "s", undefined, { shared: true });
    expect((await heldError(client().register("both", "excl-shared", "n"))).code).toBe("AGENT_NAME_HELD");

    await client().register("both", "excl-plain", "n");
    expect(
      (await heldError(client().register("both", "excl-plain", "s", undefined, { shared: true }))).code
    ).toBe("AGENT_NAME_HELD");
  });

  it("GPTQ_AGENT_SHARED=1 opts a registrant into sharing by default", async () => {
    process.env.GPTQ_AGENT_SHARED = "1";
    await client().register("both", "excl-env", "a");
    await client().register("both", "excl-env", "b");
    expect(await redis.scard(SESSION_KEYS.agentSessions("excl-env"))).toBe(2);
  });

  it("still allows reconnect by session_id and same-client re-registration", async () => {
    const first = client();
    const { session_id } = await first.register("both", "excl-reconnect", "a");
    const resumed = client();
    expect(await resumed.reconnectSession(session_id)).toBe("excl-reconnect");
    await expect(first.register("both", "excl-reconnect", "again")).resolves.toMatchObject({ name: "excl-reconnect" });
  });

  it("refuses a rename onto a live held name and keeps the old identity", async () => {
    await client().register("both", "excl-target", "holder");
    const renamer = client();
    await renamer.register("both", "excl-source", "renamer");
    expect((await heldError(renamer.register("both", "excl-target", "rename"))).code).toBe("AGENT_NAME_HELD");
    expect(renamer.requireRegistered()).toBe("excl-source");
  });
});
