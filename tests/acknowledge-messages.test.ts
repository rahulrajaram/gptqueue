/**
 * acknowledge_messages: subset acknowledgement of a claim (f-1996 v1). Real
 * Lua against Redis db 15, in the style of task-claim-store.test.ts.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { testRedisUrl } from "./helpers/test-redis.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";
import { AgentDiagnostics } from "../src/core/agent-diagnostics.js";
import { acknowledgeMessagesSchema } from "../src/mcp-server/tools/acknowledge-messages.js";

const URL = testRedisUrl();
const T0 = "2030-01-01T00:00:00.000Z";
const AFTER_EXPIRY = "2030-01-01T00:10:00.000Z";
const actor = "ackmsg-actor";
const session = "session-runtime";
const env = (id: string) => JSON.stringify({ id, type: "task", from: "peer", to: actor, payload: { n: id }, timestamp: T0 });
const ids = ["m1", "m2", "m3", "m4", "m5", "m6"];

/** Every gptq:* key with its type and full value, for no-write assertions. */
const snapshot = async (redis: Redis): Promise<Record<string, unknown>> => {
  const keys = (await redis.keys("gptq:*")).sort();
  const out: Record<string, unknown> = {};
  for (const key of keys) {
    const type = await redis.type(key);
    const ttl = await redis.pttl(key);
    const value = type === "hash" ? await redis.hgetall(key)
      : type === "zset" ? await redis.zrange(key, 0, -1, "WITHSCORES")
      : type === "list" ? await redis.lrange(key, 0, -1)
      : type === "string" ? await redis.get(key)
      : type === "stream" ? await redis.xrange(key, "-", "+") : null;
    out[key] = { type, value, persistent: ttl === -1 };
  }
  return out;
};

describe("TaskClaimStore.acknowledgeMessages (claims-ack-messages.lua)", () => {
  let redis: Redis;
  let store: TaskClaimStore;

  beforeEach(async () => {
    redis = new Redis(URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, URL);
    store = new TaskClaimStore(redis, { clock: "caller" });
  });
  afterEach(async () => {
    await flushTestKeys(redis, URL);
    await redis.quit();
  });

  const claimAll = async (envelopes: string[] = ids.map(env)) => {
    await redis.rpush(SESSION_KEYS.queue(actor), ...envelopes);
    const r = await store.claim({ actor_id: actor, session_id: session, max_batch: 16, ttl_seconds: 300, now: T0 });
    if (!r.ok || !r.claim) throw new Error("claim failed");
    return r.claim;
  };
  const ack = (claim_id: string, message_ids: string[], session_id = session) =>
    store.acknowledgeMessages({ claim_id, actor_id: actor, session_id, message_ids });
  const counter = (id: string) => CLAIM_KEYS.recoverCount(actor, id);

  it("acknowledges 4 of 6: remainder keeps raw order, expiry and score; counters go only for the 4", async () => {
    const claim = await claimAll();
    for (const id of ids) await redis.set(counter(id), "2");
    const scoreBefore = await redis.zscore(CLAIM_KEYS.index(actor), claim.claim_id);

    const r = await ack(claim.claim_id, ["m5", "m1", "m3", "m6"]);
    expect(r).toMatchObject({ ok: true, acknowledged: 4, remaining: 2, claim_deleted: false });

    const stored = JSON.parse((await redis.hget(CLAIM_KEYS.claims, claim.claim_id))!);
    expect(stored.tasks).toEqual([env("m2"), env("m4")]);
    expect(stored.claimed_at).toBe(claim.claimed_at);
    expect(stored.expires_at).toBe(claim.expires_at);
    expect(stored.session_id).toBe(session);
    expect(await redis.zscore(CLAIM_KEYS.index(actor), claim.claim_id)).toBe(scoreBefore);
    for (const id of ["m1", "m3", "m5", "m6"]) expect(await redis.exists(counter(id))).toBe(0);
    for (const id of ["m2", "m4"]) expect(await redis.get(counter(id))).toBe("2");
    const got = await store.get(claim.claim_id);
    expect(got.ok && got.claim?.tasks).toEqual([env("m2"), env("m4")]);
  });

  it("acknowledging the rest deletes the claim from the hash and the index", async () => {
    const claim = await claimAll();
    await ack(claim.claim_id, ["m1", "m2", "m3", "m4"]);
    const r = await ack(claim.claim_id, ["m6", "m5"]);
    expect(r).toMatchObject({ ok: true, acknowledged: 2, remaining: 0, claim_deleted: true });
    expect(await redis.hexists(CLAIM_KEYS.claims, claim.claim_id)).toBe(0);
    expect(await redis.zscore(CLAIM_KEYS.index(actor), claim.claim_id)).toBeNull();
    // A later whole-claim ack sees nothing, exactly as after acknowledge_tasks.
    const after = await store.acknowledge({ claim_id: claim.claim_id, actor_id: actor, session_id: session });
    expect(!after.ok && after.error.code).toBe("unknown_claim");
  });

  /** Seed a raw claim record and its index member directly. */
  const seed = async (claimId: string, raw: string, score: number) => {
    await redis.hset(CLAIM_KEYS.claims, claimId, raw);
    await redis.zadd(CLAIM_KEYS.index(actor), score, claimId);
  };
  const redisNowMs = async () => { const [s, us] = await redis.time(); return Number(s) * 1000 + Math.floor(Number(us) / 1000); };

  it("late ack of an ALREADY expired, unrecovered claim succeeds for subset and whole-claim ack", async () => {
    // claims-ack.lua:47-52 checks ownership and presence only, never expiry;
    // acknowledge_messages keeps that rule (claims-ack-messages.lua:58).
    const past = (await redisNowMs()) - 60_000;
    const iso = (ms: number) => new Date(ms).toISOString();
    await seed("late-c", JSON.stringify({ claim_id: "late-c", actor_id: actor, session_id: session,
      claimed_at: iso(past - 300_000), expires_at: iso(past), tasks: [env("m1"), env("m2"), env("m3")] }), past);
    const now = await redisNowMs();
    expect(Number(await redis.zscore(CLAIM_KEYS.index(actor), "late-c"))).toBeLessThan(now);
    expect(Date.parse(JSON.parse((await redis.hget(CLAIM_KEYS.claims, "late-c"))!).expires_at)).toBeLessThan(now);

    const partial = await ack("late-c", ["m2"]);
    expect(partial).toMatchObject({ ok: true, acknowledged: 1, remaining: 2, claim_deleted: false });
    expect(Number(await redis.zscore(CLAIM_KEYS.index(actor), "late-c"))).toBe(past);
    const whole = await store.acknowledge({ claim_id: "late-c", actor_id: actor, session_id: session });
    expect(whole).toMatchObject({ ok: true, acknowledged: 2 });
    expect(await redis.hexists(CLAIM_KEYS.claims, "late-c")).toBe(0);
    expect(await redis.zscore(CLAIM_KEYS.index(actor), "late-c")).toBeNull();
  });

  it("a remainder that cannot be re-encoded is store_corrupt with nothing written, counters included", async () => {
    // 1e999 decodes (to inf) but cjson refuses to encode it.
    const raw = `{"claim_id":"enc-c","actor_id":"${actor}","session_id":"${session}","claimed_at":"${T0}",`
      + `"expires_at":"2030-01-01T00:05:00.000Z","x":1e999,"tasks":${JSON.stringify([env("m1"), env("m2")])}}`;
    await seed("enc-c", raw, Date.parse("2030-01-01T00:05:00.000Z"));
    await redis.set(counter("m1"), "3");
    await redis.set(counter("m2"), "3");
    const before = await snapshot(redis);
    // A script error must not skip the no-write check, so capture it first.
    const r = await ack("enc-c", ["m1"]).catch((error: unknown) => ({ ok: false as const, thrown: error }));
    expect(await snapshot(redis)).toEqual(before);
    expect(r).toMatchObject({ ok: false, error: { code: "store_corrupt" } });
    expect(await redis.hget(CLAIM_KEYS.claims, "enc-c")).toBe(raw);
    expect(await redis.get(counter("m1"))).toBe("3");
  });

  it("every refusal writes nothing", async () => {
    const claim = await claimAll([...ids.map(env), "legacy-not-json"]);
    for (const id of ids) await redis.set(counter(id), "1");
    const dup = await claimAll([env("d1"), env("d1")]);
    const cases: Array<[string, () => ReturnType<typeof ack>, string]> = [
      ["empty", () => ack(claim.claim_id, []), "invalid_message_selector"],
      ["duplicate selector", () => ack(claim.claim_id, ["m1", "m2", "m1"]), "invalid_message_selector"],
      ["unknown id (legacy envelope present)", () => ack(claim.claim_id, ["m1", "nope"]), "unreadable_envelope"],
      ["ambiguous claim", () => ack(dup.claim_id, ["d1"]), "ambiguous_claim"],
      ["unknown claim", () => ack("no-such-claim", ["m1"]), "unknown_claim"],
      ["wrong session", () => ack(claim.claim_id, ["m1"], "session-other"), "not_claim_owner"],
      ["too many", () => ack(claim.claim_id, Array.from({ length: 17 }, (_, i) => `x${i}`)), "invalid_message_selector"],
    ];
    for (const [label, run, code] of cases) {
      const before = await snapshot(redis);
      const r = await run();
      expect(r.ok, label).toBe(false);
      if (!r.ok) expect(r.error.code, label).toBe(code);
      expect(await snapshot(redis), label).toEqual(before);
    }
  });

  it("an unknown id in a fully readable claim is unknown_message, with no writes", async () => {
    const claim = await claimAll();
    const before = await snapshot(redis);
    const r = await ack(claim.claim_id, ["m1", "zzz"]);
    expect(!r.ok && r.error.code).toBe("unknown_message");
    expect(await snapshot(redis)).toEqual(before);
  });

  it("a legacy envelope stays in the remainder when readable ids are acknowledged", async () => {
    const claim = await claimAll([env("m1"), "legacy-not-json", env("m2")]);
    const r = await ack(claim.claim_id, ["m2"]);
    expect(r).toMatchObject({ ok: true, acknowledged: 1, remaining: 2 });
    const stored = JSON.parse((await redis.hget(CLAIM_KEYS.claims, claim.claim_id))!);
    expect(stored.tasks).toEqual([env("m1"), "legacy-not-json"]);
  });

  it("store corruption is refused with no writes", async () => {
    const claim = await claimAll();
    await redis.zrem(CLAIM_KEYS.index(actor), claim.claim_id);
    let before = await snapshot(redis);
    let r = await ack(claim.claim_id, ["m1"]);
    expect(!r.ok && r.error.code).toBe("store_corrupt");
    expect(await snapshot(redis)).toEqual(before);

    await redis.del(CLAIM_KEYS.index(actor));
    await redis.set(CLAIM_KEYS.index(actor), "wrong-type");
    before = await snapshot(redis);
    r = await ack(claim.claim_id, ["m1"]);
    expect(!r.ok && r.error.code).toBe("store_corrupt");
    expect(await snapshot(redis)).toEqual(before);
  });

  it("wrong session gets acknowledge_tasks' refusal code and message shape", async () => {
    const claim = await claimAll();
    const partial = await ack(claim.claim_id, ["m1"], "session-other");
    const full = await store.acknowledge({ claim_id: claim.claim_id, actor_id: actor, session_id: "session-other" });
    expect(!partial.ok && partial.error).toEqual(!full.ok && full.error);
    expect(JSON.stringify(partial)).not.toContain(session);
  });

  it("after a partial ack, expiry and recovery requeue only the remainder and count only it", async () => {
    const claim = await claimAll();
    for (const id of ids) await redis.set(counter(id), "1");
    await ack(claim.claim_id, ["m1", "m2", "m3", "m4"]);
    const rec = await store.recoverExpired({ actor_id: actor, now: AFTER_EXPIRY });
    expect(rec).toMatchObject({ ok: true, recovered: 2, deadlettered: 0 });
    expect(await redis.lrange(SESSION_KEYS.queue(actor), 0, -1)).toEqual([env("m5"), env("m6")]);
    expect(await redis.get(counter("m5"))).toBe("2");
    expect(await redis.get(counter("m6"))).toBe("2");
    for (const id of ["m1", "m2", "m3", "m4"]) expect(await redis.exists(counter(id))).toBe(0);
    expect(await redis.hexists(CLAIM_KEYS.claims, claim.claim_id)).toBe(0);
  });

  it("recovery between two partial acks leaves a consistent state in either order", async () => {
    // Order A: ack, recover, ack -> second ack finds no claim; nothing lost or doubled.
    const a = await claimAll();
    await ack(a.claim_id, ["m1", "m2"]);
    await store.recoverExpired({ actor_id: actor, now: AFTER_EXPIRY });
    const second = await ack(a.claim_id, ["m3"]);
    expect(!second.ok && second.error.code).toBe("unknown_claim");
    expect(await redis.lrange(SESSION_KEYS.queue(actor), 0, -1)).toEqual(["m3", "m4", "m5", "m6"].map(env));
    expect(await redis.zcard(CLAIM_KEYS.index(actor))).toBe(0);
    expect(await redis.hlen(CLAIM_KEYS.claims)).toBe(0);
    await redis.del(SESSION_KEYS.queue(actor));

    // Order B: both acks land before recovery -> only the true remainder returns.
    const b = await claimAll();
    await ack(b.claim_id, ["m1", "m2"]);
    await ack(b.claim_id, ["m3"]);
    await store.recoverExpired({ actor_id: actor, now: AFTER_EXPIRY });
    expect(await redis.lrange(SESSION_KEYS.queue(actor), 0, -1)).toEqual(["m4", "m5", "m6"].map(env));
    expect(await redis.zcard(CLAIM_KEYS.index(actor))).toBe(0);

    // Concurrent: both scripts are atomic, so racing them yields one of A/B.
    await redis.del(SESSION_KEYS.queue(actor));
    const c = await claimAll();
    await ack(c.claim_id, ["m1", "m2"]);
    await Promise.all([ack(c.claim_id, ["m3"]), store.recoverExpired({ actor_id: actor, now: AFTER_EXPIRY })]);
    const queue = await redis.lrange(SESSION_KEYS.queue(actor), 0, -1);
    expect([["m3", "m4", "m5", "m6"].map(env), ["m4", "m5", "m6"].map(env)]).toContainEqual(queue);
    expect(await redis.zcard(CLAIM_KEYS.index(actor))).toBe(0);
    expect(await redis.hlen(CLAIM_KEYS.claims)).toBe(0);
  });
});

describe("acknowledge_messages tool", () => {
  it("the schema is strict: unknown fields are refused, not stripped", () => {
    expect(acknowledgeMessagesSchema.safeParse({ claim_id: "c", message_ids: ["a"], extra: 1 }).success).toBe(false);
    expect(acknowledgeMessagesSchema.safeParse({ claim_id: "c", message_ids: [] }).success).toBe(false);
    expect(acknowledgeMessagesSchema.safeParse({ claim_id: "c", message_ids: ["a"] }).success).toBe(true);
  });

  it("over MCP: refuses unknown fields, acks a subset, and traces and diagnostics show only those ids", async () => {
    const redis = new Redis(URL);
    await flushTestKeys(redis, URL);
    const backend = new RedisClient(null, URL);
    const server = new McpServer({ name: "ackmsg-test", version: "1" });
    registerTools(server, backend);
    const [st, ct] = InMemoryTransport.createLinkedPair();
    await server.connect(st);
    const client = new Client({ name: "ackmsg-client", version: "1" });
    await client.connect(ct);
    const data = (r: unknown) => (r as { structuredContent?: Record<string, unknown> }).structuredContent ?? {};
    try {
      const name = "ackmsg-mcp";
      const reg = data(await client.callTool({ name: "register_agent", arguments: { name, role: "both", description: "subset ack test" } }));
      const session_id = String(reg.session_id);
      await redis.rpush(SESSION_KEYS.queue(name), ...["k1", "k2", "k3"].map((id) => JSON.stringify({ id, type: "task", from: "peer", to: name, payload: {}, timestamp: T0 })));
      const claimed = data(await client.callTool({ name: "claim_tasks", arguments: { session_id, max_batch: 3 } }));
      if (!claimed.claim) throw new Error(`claim failed: ${JSON.stringify(claimed)} reg=${JSON.stringify(reg)}`);
      const claim_id = String((claimed.claim as Record<string, unknown>).claim_id);

      const before = await snapshot(redis);
      const bad = await client.callTool({ name: "acknowledge_messages", arguments: { session_id, claim_id, message_ids: ["k1"], mode: "full" } });
      expect(bad.isError).toBe(true);
      expect(await snapshot(redis)).toEqual(before);

      const ok = data(await client.callTool({ name: "acknowledge_messages", arguments: { session_id, claim_id, message_ids: ["k2"] } }));
      expect(ok).toMatchObject({ status: "ok", acknowledged: 1, remaining: 2, claim_deleted: false, message_ids: ["k2"] });

      const traces = (await redis.xrange(SESSION_KEYS.inboxTrace(name), "-", "+")).map(([, f]) => Object.fromEntries(
        Array.from({ length: f.length / 2 }, (_, i) => [f[i * 2], f[i * 2 + 1]])));
      expect(traces.filter((t) => t.stage === "task_acknowledged")).toEqual([]);
      expect(traces.filter((t) => t.stage === "message_acknowledged").map((t) => [t.claim_id, t.message_id])).toEqual([[claim_id, "k2"]]);

      const diagnostics = new AgentDiagnostics(redis);
      expect((await diagnostics.delivery(name, "k2")).status).toBe("acknowledged");
      expect((await diagnostics.delivery(name, "k1")).status).toBe("claimed");
      expect((await diagnostics.delivery(name, "k3")).status).toBe("claimed");
      // A pending remainder is not marked acknowledged even once it leaves the claim.
      await redis.hdel(CLAIM_KEYS.claims, claim_id);
      await redis.zrem(CLAIM_KEYS.index(name), claim_id);
      expect((await diagnostics.delivery(name, "k1")).status).toBe("unknown_history");
      expect((await diagnostics.delivery(name, "k2")).status).toBe("acknowledged");
    } finally {
      await client.close();
      await backend.shutdown().catch(() => undefined);
      await flushTestKeys(redis, URL);
      await redis.quit();
    }
  });
});
