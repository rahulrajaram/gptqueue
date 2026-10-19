import { testRedisUrl } from "./helpers/test-redis.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { randomUUID, createHash } from "node:crypto";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { applyContinuity, prepareContinuity, runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { OCCUPANCY_SIGNALS } from "../src/core/occupancy-guard.js";
import { restoreRuntimeMailbox, RuntimeMailboxAdoptionRefused } from "../src/registered-shell/runtime-mailbox.js";
import { AdoptionRefusedError } from "../src/core/adoption-refusal.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";
import { flushTestKeys, assertPrivateTestRedis } from "./helpers/redis-test-utils.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";

const redisUrl = testRedisUrl();
assertPrivateTestRedis(redisUrl, "mailbox continuity tests");
const clients: RedisClient[] = [];
const connections: Redis[] = [];
const binding = (id = randomUUID()): RuntimeBinding => ({
  client: "codex", runtime_id: id, epoch: "one", working_directory: "/workspace",
});

afterEach(async () => {
  vi.restoreAllMocks();
  for (const client of clients.splice(0)) client.forceDisconnect();
  for (const redis of connections.splice(0)) { await flushTestKeys(redis, redisUrl); await redis.quit(); }
});

const setup = async () => {
  const redis = new Redis(redisUrl); connections.push(redis);
  const canonical = `canonical-${randomUUID()}`;
  const provisional = `provisional-${randomUUID()}`;
  const b = binding();
  await redis.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical }), provisional, JSON.stringify({ name: provisional }));
  const key = `gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`;
  await redis.set(key, JSON.stringify({ agent: provisional, working_directory: b.working_directory }));
  return { redis, canonical, provisional, b };
};

describe("mailbox continuity operator API", () => {
  it("prepares and applies a mapping, then is idempotent", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await applyContinuity(redis, plan)).toBe("idempotent");
    expect(JSON.parse((await redis.get(plan.mappingKey))!).agent).toBe(canonical);
  });

  it("refuses stale fingerprints", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await redis.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical, changed: true }));
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("refuses target live-owner conflicts", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    const liveId = randomUUID(); await redis.sadd(SESSION_KEYS.agentSessions(canonical), liveId); await redis.set(SESSION_KEYS.lease(liveId), "alive", "EX", 30);
    await redis.set(SESSION_KEYS.heartbeat(canonical), "alive", "EX", 30);
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("refuses source queued, claimed, or outbound state", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await redis.rpush(SESSION_KEYS.queue(plan.source!), JSON.stringify({ id: "x" }));
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
  });

  it("allows expired target claims without deleting their recoverable payload", async () => {
    const { redis, canonical, b } = await setup();
    const claimId = randomUUID();
    const payload = JSON.stringify({ id: randomUUID(), to: canonical, payload: { content: "unfinished" } });
    const expiry = Date.now() - 60_000;
    const claim = JSON.stringify({ claim_id: claimId, actor_id: canonical, session_id: randomUUID(),
      claimed_at: new Date(expiry - 1000).toISOString(), expires_at: new Date(expiry).toISOString(), tasks: [payload] });
    await redis.hset(CLAIM_KEYS.claims, claimId, claim);
    await redis.zadd(CLAIM_KEYS.index(canonical), expiry, claimId);
    const plan = await prepareContinuity(redis, b, canonical);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await redis.hget(CLAIM_KEYS.claims, claimId)).toBe(claim);
    expect(await redis.zrange(CLAIM_KEYS.index(canonical), 0, -1)).toEqual([claimId]);
  });

  it("refuses to abandon expired claims at a replaced source mailbox", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.zadd(CLAIM_KEYS.index(provisional), Date.now() - 60_000, randomUUID());
    const plan = await prepareContinuity(redis, b, canonical);
    await expect(applyContinuity(redis, plan)).rejects.toThrow();
    expect(JSON.parse((await redis.get(plan.mappingKey))!).agent).toBe(provisional);
  });

  it("refuses missing provenance instead of discovering by cwd", async () => {
    const redis = new Redis(redisUrl); connections.push(redis);
    await expect(prepareContinuity(redis, binding(), `target-${randomUUID()}`)).rejects.toThrow(/provenance/i);
  });

  it("restores the exact queued envelope at the canonical mailbox", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    const envelope = { id: randomUUID(), from: "peer", to: canonical, timestamp: new Date().toISOString(), type: "task", payload: { content: "exact" } };
    await redis.rpush(SESSION_KEYS.queue(canonical), JSON.stringify(envelope));
    const sessionId = client.sessionId!;
    expect(await restoreRuntimeMailbox(client, b)).toBe(canonical);
    expect(client.sessionId).toBe(sessionId);
    expect(await redis.hget(SESSION_KEYS.session(sessionId), "agent_name")).toBe(canonical);
    expect(await redis.sismember(SESSION_KEYS.agentSessions(canonical), sessionId)).toBe(1);
    expect(await redis.sismember(SESSION_KEYS.agentSessions(provisional), sessionId)).toBe(0);
    expect(await redis.hget(SESSION_KEYS.registry, provisional)).toBeNull();
    expect(JSON.parse((await redis.lindex(SESSION_KEYS.queue(canonical), 0))!)).toEqual(envelope);
  });

  it("allows a stale target session member when its lease is absent", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    await redis.sadd(SESSION_KEYS.agentSessions(canonical), randomUUID());
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    await expect(restoreRuntimeMailbox(client, b)).resolves.toBe(canonical);
  });

  it("permits only one concurrent adopter", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const first = new RedisClient(null, redisUrl); const second = new RedisClient(null, redisUrl); clients.push(first, second);
    await first.register("both", provisional);
    await second.register("both", `other-${randomUUID()}`);
    const results = await Promise.allSettled([restoreRuntimeMailbox(first, b), restoreRuntimeMailbox(second, b)]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(results.filter((r) => r.status === "rejected")).toHaveLength(1);
    expect([first.agentName, second.agentName]).toContain(canonical);
  });

  it("refuses an active target lease and preserves source identity", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(`gptq:runtime-mailbox:${(await import("node:crypto")).createHash("sha256").update(JSON.stringify([b.client, b.runtime_id])).digest("hex")}`, JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const sid = randomUUID(); await redis.sadd(SESSION_KEYS.agentSessions(canonical), sid); await redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30);
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(client.agentName).toBe(provisional);
  });

  it("forwards a queued provisional source into the bound mailbox, re-addressed, in the adoption step (DL-2)", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    const existing = { id: "already-bound", from: "peer", to: canonical, timestamp: "t0", type: "task", payload: { content: "first" } };
    const sent = { id: "sent-to-provisional", from: "peer", to: provisional, timestamp: "t1", type: "task", payload: { content: "x", n: 0 } };
    // An integer beyond 2^53, written into the raw JSON exactly: a re-encoding move would change it.
    const raw = (value: object) => JSON.stringify(value).replace('"n":0', '"n":12345678901234567');
    await redis.rpush(SESSION_KEYS.queue(canonical), JSON.stringify(existing));
    await redis.rpush(SESSION_KEYS.queue(provisional), raw(sent));
    await expect(restoreRuntimeMailbox(client, b)).resolves.toBe(canonical);
    expect(await redis.exists(SESSION_KEYS.queue(provisional))).toBe(0);
    // FIFO after existing work; only the address changes, byte for byte.
    expect(await redis.lrange(SESSION_KEYS.queue(canonical), 0, -1)).toEqual([JSON.stringify(existing), raw({ ...sent, to: canonical })]);
  });
  it.each(["claim", "outbound"])("refused adoption with %s leaves durable ownership unchanged", async cause => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client); await client.register("both", provisional);
    if (cause === "queue") await redis.rpush(SESSION_KEYS.queue(provisional), "retained");
    if (cause === "claim") await redis.zadd(`gptq:claims-index:${provisional}`, 1, "claim");
    if (cause === "outbound") await redis.set(`gptq:outbound-activity:${provisional}`, "1");
    const snapshot = async () => ({
      mapping: await redis.get(runtimeMailboxKey(b)), registry: await redis.hgetall(SESSION_KEYS.registry),
      session: await redis.hgetall(SESSION_KEYS.session(client.sessionId!)),
      sourceMembers: await redis.smembers(SESSION_KEYS.agentSessions(provisional)),
      targetMembers: await redis.smembers(SESSION_KEYS.agentSessions(canonical)),
      sourceQueue: await redis.lrange(SESSION_KEYS.queue(provisional), 0, -1),
      targetQueue: await redis.lrange(SESSION_KEYS.queue(canonical), 0, -1),
      claims: await redis.zrange(`gptq:claims-index:${provisional}`, 0, -1),
      sourceHeartbeat: await redis.get(SESSION_KEYS.heartbeat(provisional)),
      targetHeartbeat: await redis.get(SESSION_KEYS.heartbeat(canonical)),
      binding: await redis.get(`gptq:runtime-binding:${canonical}`),
    });
    const before = await snapshot();
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(await snapshot()).toEqual(before);
    expect(client.agentName).toBe(provisional);
  });

  it("supports explicit legacy adoption without moving its pending envelope", async () => {
    const { redis, canonical, b } = await setup();
    await redis.del(runtimeMailboxKey(b));
    const envelope = JSON.stringify({ id: "pending", to: canonical, payload: { content: "preserve" } });
    await redis.rpush(SESSION_KEYS.queue(canonical), envelope);
    const plan = await prepareContinuity(redis, b, canonical, { allowLegacy: true });
    await expect(applyContinuity(redis, { ...plan, namespace: "another database" })).rejects.toThrow(/namespace/);
    await expect(applyContinuity(redis, { ...plan, mappingKey: "arbitrary:key" })).rejects.toThrow(/mapping/);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await redis.lrange(SESSION_KEYS.queue(canonical), 0, -1)).toEqual([envelope]);
    expect(await redis.xlen("gptq:continuity-audit")).toBe(1);
  });

  // F5: the two adoption state machines (applyContinuity's occupied() and
  // adoptIdentity's target-occupancy guard) must enforce the SAME occupancy
  // signal set on the target. Every signal below must be refused by BOTH
  // paths — the claims-index row is the divergence the review verified
  // (adoptIdentity previously accepted a claimed target).
  const wrapperKeyFor = (agent: string) =>
    `gptq:experimental-wrapper-claim:${createHash("sha256").update(agent).digest("hex")}`;

  // D5: the occupancy matrix is DERIVED from the shared guard table
  // (core/occupancy-guard.ts). Adding a signal to the table without an
  // injector here fails the completeness check; adding an injector without
  // a table entry is flagged below the matrix.
  const SIGNAL_INJECTORS: Record<string, (redis: Redis, target: string) => Promise<unknown>> = {
    runtime_binding: (redis, target) => redis.set(`gptq:runtime-binding:${target}`, "bound"),
    heartbeat: (redis, target) => redis.set(SESSION_KEYS.heartbeat(target), "alive", "EX", 30),
    wrapper_claim: (redis, target) => redis.set(wrapperKeyFor(target), "claimed"),
    live_session_lease: async (redis, target) => {
      const sid = randomUUID();
      await redis.sadd(SESSION_KEYS.agentSessions(target), sid);
      await redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30);
    },
    claims_index: (redis, target) => redis.zadd(`gptq:claims-index:${target}`, Date.now() + 60_000, "active-claim"),
  };

  it("the injector map covers every signal in the shared guard table (D5)", () => {
    for (const signal of OCCUPANCY_SIGNALS) {
      expect(SIGNAL_INJECTORS[signal.name], `missing injector for guard-table signal '${signal.name}'`).toBeDefined();
    }
  });

  const OCCUPANCY_INJECTORS = OCCUPANCY_SIGNALS.map((signal) => ({
    name: signal.name,
    inject: SIGNAL_INJECTORS[signal.name]!,
  }));

  const rejection = (work: Promise<unknown>): Promise<unknown> =>
    work.then(() => { throw new Error("expected a rejection"); }, (error: unknown) => error);

  // P1: both paths name the same first blocking signal from the shared table.
  it.each(OCCUPANCY_INJECTORS)("operator path refuses a target with %s", async ({ name, inject }) => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await inject(redis, canonical);
    const error = await rejection(applyContinuity(redis, plan));
    expect(error).toBeInstanceOf(AdoptionRefusedError);
    expect((error as AdoptionRefusedError).refusal).toMatchObject({ code: -4, reason: "target_occupied", signal: name });
    expect(JSON.parse((await redis.get(plan.mappingKey))!).agent).toBe(plan.source);
  });

  it.each(OCCUPANCY_INJECTORS)("runtime path refuses a target with %s", async ({ name, inject }) => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    await inject(redis, canonical);
    const error = await rejection(restoreRuntimeMailbox(client, b));
    expect(error).toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    expect(error).toMatchObject({ code: -5, reason: "target_occupied", signal: name, target: canonical });
    expect(client.agentName).toBe(provisional);
  });

  // P1: the operator path still refuses a busy source on the same conditions, now naming the first one.
  const SOURCE_STATES: Array<[string, (redis: Redis, source: string) => Promise<unknown>]> = [
    ["heartbeat", (redis, source) => redis.set(SESSION_KEYS.heartbeat(source), "alive", "EX", 30)],
    ["claims_index", (redis, source) => redis.zadd(CLAIM_KEYS.index(source), Date.now() - 60_000, "expired-claim")],
    ["queued_messages", (redis, source) => redis.rpush(SESSION_KEYS.queue(source), JSON.stringify({ id: "x" }))],
    ["outbound_activity", (redis, source) => redis.set(SESSION_KEYS.outboundActivity(source), "1", "EX", 60)],
  ];
  it.each(SOURCE_STATES)("operator path refuses a busy source, naming %s", async (signal, inject) => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    await inject(redis, plan.source!);
    const error = await rejection(applyContinuity(redis, plan));
    expect(error).toBeInstanceOf(AdoptionRefusedError);
    expect((error as AdoptionRefusedError).refusal).toMatchObject({ code: -5, reason: "source_busy", signal });
    expect((error as Error).message).toMatch(/^Continuity apply refused \(-5\): source_busy/);
    expect(JSON.parse((await redis.get(plan.mappingKey))!).agent).toBe(plan.source);
  });

  // Merge-gate F6: a stale operator plan is classified the same whether the
  // preflight reads or the atomic script see the change first.
  const STALE_PLAN: Array<[string, (redis: Redis, s: { canonical: string; provisional: string; key: string }) => Promise<unknown>, object]> = [
    ["mapping", (redis, s) => redis.set(s.key, JSON.stringify({ agent: `other-${randomUUID()}`, working_directory: "/workspace" })),
      { code: -1, reason: "mapping_changed", signal: null }],
    ["target registration", (redis, s) => redis.hset(SESSION_KEYS.registry, s.canonical, JSON.stringify({ name: s.canonical, changed: true })),
      { code: -2, reason: "registration_changed", signal: "target_registration" }],
    ["source registration", (redis, s) => redis.hset(SESSION_KEYS.registry, s.provisional, JSON.stringify({ name: s.provisional, changed: true })),
      { code: -3, reason: "registration_changed", signal: "source_registration" }],
  ];
  it.each(STALE_PLAN)("classifies a changed %s identically in the preflight and in the script", async (_name, change, expected) => {
    for (const path of ["preflight", "script"] as const) {
      const { redis, canonical, provisional, b } = await setup();
      const plan = await prepareContinuity(redis, b, canonical);
      const s = { canonical, provisional, key: plan.mappingKey };
      if (path === "preflight") await change(redis, s);
      else {
        // The change lands after the preflight reads, just before the atomic script runs.
        const original = redis.eval.bind(redis) as (...args: unknown[]) => Promise<unknown>;
        vi.spyOn(redis, "eval").mockImplementationOnce((async (...args: unknown[]) => { await change(redis, s); return original(...args); }) as never);
      }
      const error = await rejection(applyContinuity(redis, plan));
      expect(error, path).toBeInstanceOf(AdoptionRefusedError);
      expect((error as AdoptionRefusedError).refusal, path).toMatchObject(expected);
      vi.restoreAllMocks();
    }
  });

  it("keeps malformed plans and Redis failures separate from refusals", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    const malformed = await rejection(applyContinuity(redis, { ...plan, mappingKey: "arbitrary:key" }));
    expect(malformed).toBeInstanceOf(Error);
    expect(malformed).not.toBeInstanceOf(AdoptionRefusedError);
    vi.spyOn(redis, "eval").mockRejectedValueOnce(new Error("Connection is closed."));
    const failed = await rejection(applyContinuity(redis, plan));
    expect(failed).toMatchObject({ message: "Connection is closed." });
    expect(failed).not.toBeInstanceOf(AdoptionRefusedError);
  });

  // F6: idempotent replay must revalidate the target fingerprint. A target
  // registry entry rewritten (reclaimed) between applies must be refused,
  // not reported as an idempotent success.
  it("rejects idempotent replay after the target registry entry is reclaimed (F6)", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    expect(await applyContinuity(redis, plan)).toBe("applied");

    // The target entry is deleted and recreated between applies: the mapping
    // still reads as desired, but the recorded target fingerprint is stale.
    await redis.hdel(SESSION_KEYS.registry, canonical);
    await redis.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical, reclaimed: true }));

    await expect(applyContinuity(redis, plan)).rejects.toThrow(/fingerprints|changed/);
  });

  it("idempotent replay still succeeds when nothing changed (F6 control)", async () => {
    const { redis, canonical, b } = await setup();
    const plan = await prepareContinuity(redis, b, canonical);
    expect(await applyContinuity(redis, plan)).toBe("applied");
    expect(await applyContinuity(redis, plan)).toBe("idempotent");
  });

  // F8: working-directory comparisons accept lexical `.`/`..` aliases, so a
  // binding validated with an aliased path still restores a mapping stored
  // with the canonical form (and vice versa).
  it("accepts lexically aliased working directories across restore (F8)", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: "/workspace" }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    await expect(
      restoreRuntimeMailbox(client, { ...b, working_directory: "/workspace/./" })
    ).resolves.toBe(canonical);
  });

  // D6: runtime-path identity adoption must be audited on the same bounded
  // stream the operator path uses — it is exactly the path with no human in
  // the loop. Refusals must write nothing.
  it("audits successful runtime adoption with source, target, and runtime_id (D6)", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    const before = await redis.xlen("gptq:continuity-audit");
    expect(before).toBe(0);

    expect(await restoreRuntimeMailbox(client, b)).toBe(canonical);

    expect(await redis.xlen("gptq:continuity-audit")).toBe(before + 1);
    const [last] = await redis.xrevrange("gptq:continuity-audit", "+", "-", "COUNT", 1);
    const fields = last?.[1] ?? [];
    const entry: Record<string, string> = {};
    for (let i = 0; i + 1 < fields.length; i += 2) entry[String(fields[i])] = String(fields[i + 1]);
    expect(entry.event).toBe("runtime_continuity");
    expect(entry.source).toBe(provisional);
    expect(entry.target).toBe(canonical);
    expect(entry.runtime_id).toBe(b.runtime_id);
    expect(entry.session_id).toBe(client.sessionId);
  });

  it("a refused adoption writes no audit event (D6)", async () => {
    const { redis, canonical, provisional, b } = await setup();
    await redis.set(runtimeMailboxKey(b), JSON.stringify({ agent: canonical, working_directory: b.working_directory }));
    const client = new RedisClient(null, redisUrl); clients.push(client);
    await client.register("both", provisional);
    await redis.set(`gptq:outbound-activity:${provisional}`, "1");
    await expect(restoreRuntimeMailbox(client, b)).rejects.toThrow();
    expect(await redis.xlen("gptq:continuity-audit")).toBe(0);
  });

});
