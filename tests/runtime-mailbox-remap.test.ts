/**
 * Operator remap of a runtime mailbox mapping (core/runtime-mailbox-remap.ts):
 * the plan's guards, and the apply script's atomic re-check of every guard it
 * can see inside Redis after the plan was read.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { applyRuntimeMailboxRemap, planRuntimeMailboxRemap, remapPreview } from "../src/core/runtime-mailbox-remap.js";
import { flushTestKeys, assertNotLiveDb } from "./helpers/redis-test-utils.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
assertNotLiveDb(redisUrl, "runtime mailbox remap tests");
const connections: Redis[] = [];
// Start from an empty gptq:* namespace: other suites may leave audit entries behind.
beforeEach(async () => {
  const redis = new Redis(redisUrl);
  try { await flushTestKeys(redis, redisUrl); } finally { await redis.quit(); }
});
afterEach(async () => { vi.restoreAllMocks(); for (const redis of connections.splice(0)) { await flushTestKeys(redis, redisUrl); await redis.quit(); } });

const alive = () => true;
const setup = async () => {
  const redis = new Redis(redisUrl); connections.push(redis);
  const runtimeId = randomUUID();
  const from = `remap-from-${randomUUID()}`;
  const to = `remap-to-${randomUUID()}`;
  const key = SESSION_KEYS.runtimeMailbox("codex", runtimeId);
  const mapping = JSON.stringify({ agent: from, working_directory: "/workspace/project" });
  await redis.set(key, mapping);
  await redis.hset(SESSION_KEYS.registry, from, JSON.stringify({ name: from, pid: 999_999_999 }), to, JSON.stringify({ name: to, pid: 4242 }));
  await redis.set(SESSION_KEYS.heartbeat(to), "alive", "EX", 60);
  await redis.sadd(SESSION_KEYS.provisional, to);
  const request = { client: "codex" as const, runtimeId, from, to };
  return { redis, runtimeId, from, to, key, mapping, request };
};
const unchanged = async (s: Awaited<ReturnType<typeof setup>>) => {
  expect(await s.redis.get(s.key)).toBe(s.mapping);
  expect(await s.redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(1);
  expect(await s.redis.xlen(SESSION_KEYS.continuityAudit)).toBe(0);
};

describe("runtime mailbox remap", () => {
  it("applies atomically: CAS, provisional mark cleared, operator_remap audit, no work moved", async () => {
    const s = await setup();
    await s.redis.rpush(SESSION_KEYS.queue(s.from), "retained");
    await s.redis.zadd(CLAIM_KEYS.index(s.from), Date.now() + 60_000, "claim");
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    expect(plan.ok).toBe(true);
    expect(remapPreview(plan)).not.toHaveProperty("cas");
    const outcome = await applyRuntimeMailboxRemap(s.redis, plan);
    expect(outcome.status).toBe("applied");
    expect(JSON.parse((await s.redis.get(s.key))!)).toEqual({ agent: s.to, working_directory: "/workspace/project" });
    expect(await s.redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(0);
    const entries = await s.redis.xrange(SESSION_KEYS.continuityAudit, "-", "+");
    expect(entries).toHaveLength(1);
    const [id, fields] = entries[0]!;
    expect(outcome).toEqual({ status: "applied", audit_id: id });
    expect(fields).toEqual(["event", "operator_remap", "target", s.to, "source", s.from, "runtime_id", s.runtimeId, "client", "codex"]);
    expect(await s.redis.lrange(SESSION_KEYS.queue(s.from), 0, -1)).toEqual(["retained"]);
    expect(await s.redis.zcard(CLAIM_KEYS.index(s.from))).toBe(1);
  });

  it("accepts a live session lease as proof of TO's live registration", async () => {
    const s = await setup();
    await s.redis.del(SESSION_KEYS.heartbeat(s.to));
    const sid = randomUUID();
    await s.redis.sadd(SESSION_KEYS.agentSessions(s.to), sid);
    await s.redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30);
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    expect(plan.guards.find((g) => g.guard === "to_live_registration")).toMatchObject({ ok: true });
    expect((await applyRuntimeMailboxRemap(s.redis, plan)).status).toBe("applied");
  });

  it("refuses without writing when a plan guard failed (pid checked in this process)", async () => {
    const s = await setup();
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: () => false });
    expect(plan.ok).toBe(false);
    expect(await applyRuntimeMailboxRemap(s.redis, plan)).toEqual({ status: "refused",
      refusals: [{ reason: "to_pid_alive", detail: `${s.to} pid 4242 is not running (checked by this process)` }] });
    await unchanged(s);
  });

  const races: Array<[string, (s: Awaited<ReturnType<typeof setup>>) => Promise<unknown>]> = [
    ["mapping_changed", (s) => s.redis.set(s.key, JSON.stringify({ agent: `other-${randomUUID()}`, working_directory: "/workspace/project" }))],
    ["to_registration_changed", (s) => s.redis.hset(SESSION_KEYS.registry, s.to, JSON.stringify({ name: s.to, pid: 4343 }))],
    ["to_not_live", (s) => s.redis.del(SESSION_KEYS.heartbeat(s.to))],
    ["from_not_dormant", (s) => s.redis.set(SESSION_KEYS.runtimeBinding(s.from), "bound")],
    ["from_not_dormant", (s) => s.redis.set(SESSION_KEYS.heartbeat(s.from), "alive", "EX", 30)],
  ];
  it.each(races)("re-checks inside the atomic apply: %s after the plan", async (reason, race) => {
    const s = await setup();
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    expect(plan.ok).toBe(true);
    await race(s);
    const raced = await s.redis.get(s.key);
    const outcome = await applyRuntimeMailboxRemap(s.redis, plan);
    expect(outcome).toMatchObject({ status: "refused", refusals: [{ reason }] });
    // Nothing written: the mapping is exactly what the race left.
    expect(await s.redis.get(s.key)).toBe(raced);
    expect(await s.redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(1);
    expect(await s.redis.xlen(SESSION_KEYS.continuityAudit)).toBe(0);
  });

  it("discloses outstanding reply correlations touching either mailbox, even with no queue or claims (F2)", async () => {
    const s = await setup();
    await s.redis.set(SESSION_KEYS.outstanding(s.from, "t-await"), "peer", "EX", 600); // FROM awaits peer's reply
    await s.redis.set(SESSION_KEYS.outstanding("peer", "t-owe"), s.from, "EX", 600); // FROM owes peer a reply
    await s.redis.set(SESSION_KEYS.outstanding("peer", "t-to"), s.to, "EX", 600); // TO owes peer a reply
    await s.redis.set(SESSION_KEYS.outstanding("peer", "t-other"), `someone-${randomUUID()}`, "EX", 600);
    await s.redis.set(SESSION_KEYS.outstanding("peer", "t-no-ttl"), s.from); // unreadable as a correlation
    await s.redis.set(SESSION_KEYS.taskLedger("t-owe"),
      JSON.stringify({ from: "peer", to: s.from, subject: "review the patch", sent_at: "2026-10-19T00:00:00.000Z" }), "EX", 600);
    await s.redis.set(SESSION_KEYS.taskAnswered("t-await"), "2026-10-19T00:01:00.000Z", "EX", 600);
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    expect(plan.correlations).toMatchObject({ complete: true, outstanding_keys: 5, unparsed: 1,
      from: { awaiting_reply: 1, owes_reply: 1 }, to: { awaiting_reply: 0, owes_reply: 1 } });
    expect(plan.correlations.from.sample).toEqual(expect.arrayContaining([
      expect.objectContaining({ sender: s.from, task_id: "t-await", recipient: "peer", answered: true, subject: null }),
      expect.objectContaining({ sender: "peer", task_id: "t-owe", recipient: s.from, answered: false, subject: "review the patch" }),
    ]));
    expect(plan.correlations.to.sample).toEqual([expect.objectContaining({ task_id: "t-to", recipient: s.to })]);
    expect(plan.notes[0]).toMatch(/0 queued message\(s\), 0 unexpired claim\(s\), and 1 awaited and 1 owed reply correlation\(s\) stay at/);
    expect(plan.guards.find((g) => g.guard === "correlation_disclosure_complete")).toMatchObject({ ok: true });
  });

  it("an incomplete correlation SCAN refuses apply unless the operator accepts it (F2)", async () => {
    const s = await setup();
    // A keyspace the capped SCAN cannot finish in one step.
    for (let i = 0; i < 64; i++) await s.redis.set(`gptq:test-filler:${i}`, "x");
    const capped = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive, correlationScanLimit: 1 });
    expect(capped.correlations).toMatchObject({ complete: false, scan_limit: 1 });
    expect(capped.notes[0]).toMatch(/and at least 0 awaited and at least 0 owed/);
    expect(await applyRuntimeMailboxRemap(s.redis, capped)).toMatchObject({ status: "refused",
      refusals: [{ reason: "correlation_disclosure_complete", detail: expect.stringMatching(/--accept-incomplete-disclosure yes/) }] });
    await unchanged(s);
    const accepted = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive, correlationScanLimit: 1, acceptIncompleteDisclosure: true });
    expect(accepted.correlations.complete).toBe(false);
    expect(accepted.guards.find((g) => g.guard === "correlation_disclosure_complete")).toMatchObject({ ok: true, detail: expect.stringMatching(/accepted/) });
    expect((await applyRuntimeMailboxRemap(s.redis, accepted)).status).toBe("applied");
  });

  const wrongTypes: Array<[string, (s: Awaited<ReturnType<typeof setup>>) => Promise<unknown>]> = [
    ["continuity audit", (s) => s.redis.set(SESSION_KEYS.continuityAudit, "not-a-stream")],
    ["provisional", async (s) => { await s.redis.del(SESSION_KEYS.provisional); await s.redis.set(SESSION_KEYS.provisional, "not-a-set"); }],
    ["to session lease", async (s) => {
      const sid = randomUUID();
      await s.redis.sadd(SESSION_KEYS.agentSessions(s.to), sid);
      await s.redis.hset(SESSION_KEYS.lease(sid), "not", "a-string");
    }],
  ];
  it.each(wrongTypes)("refuses a wrong-type %s key found after the plan, before any write (F4)", async (role, corrupt) => {
    const s = await setup();
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    expect(plan.ok).toBe(true);
    await corrupt(s);
    expect(await applyRuntimeMailboxRemap(s.redis, plan)).toMatchObject({ status: "refused",
      refusals: [{ reason: "wrong_key_type", detail: expect.stringContaining(`the ${role} key has type`) }] });
    expect(await s.redis.get(s.key)).toBe(s.mapping);
    if (role !== "provisional") expect(await s.redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(1);
    else expect(await s.redis.get(SESSION_KEYS.provisional)).toBe("not-a-set");
    if (role === "continuity audit") expect(await s.redis.get(SESSION_KEYS.continuityAudit)).toBe("not-a-stream");
  });

  it("reports a Redis failure during apply as an unknown outcome, never a refusal (F4)", async () => {
    const s = await setup();
    const plan = await planRuntimeMailboxRemap(s.redis, s.request, { isPidAlive: alive });
    vi.spyOn(s.redis, "eval").mockRejectedValueOnce(new Error("Connection is closed."));
    await expect(applyRuntimeMailboxRemap(s.redis, plan)).rejects.toThrow(/^Remap outcome unknown \(Connection is closed\.\)/);
  });
});
