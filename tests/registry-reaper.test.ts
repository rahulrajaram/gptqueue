/**
 * Operator reaper for empty offline per-startup sidecar names. Each case names
 * the mutation of src/core/registry-reaper.ts it catches.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { ACTOR_KEYS, CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS, SYSTEM_SENDER, WAKE_LEASE_KEYS } from "../src/core/keys.js";
import { REAP_AUDIT_REASON, reapOfflineIdentities, type SkipReason } from "../src/core/registry-reaper.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const URL = testRedisUrl();
const DAY = 24 * 60 * 60 * 1000;
const old = new Date(Date.now() - 3 * DAY).toISOString();

describe("registry reaper (reap-offline)", () => {
  let redis: Redis;
  beforeEach(async () => { redis = new Redis(URL); await flushTestKeys(redis, URL); });
  afterEach(async () => { await flushTestKeys(redis, URL); await redis.quit(); });

  /** A dead sidecar registration: registry entry, one expired session (no lease), mailbox meta. */
  const dead = async (name = `gptqueue-shell-pi-x-${randomUUID()}`, registeredAt: string | null = old) => {
    const sid = randomUUID();
    await redis.hset(SESSION_KEYS.registry, name, JSON.stringify({ name, role: "both", ...(registeredAt ? { registered_at: registeredAt } : {}) }));
    await redis.sadd(SESSION_KEYS.agentSessions(name), sid);
    await redis.hset(SESSION_KEYS.session(sid), "agent_name", name, "created_at", old, "last_seen", old);
    await redis.hset(SESSION_KEYS.mailboxMeta(name), "max_size", "10", "current_size", "0");
    await redis.sadd(SESSION_KEYS.provisional, name);
    await redis.xadd(SESSION_KEYS.inboxTrace(name), "*", "event", "x");
    return { name, sid };
  };
  const skipOf = (report: Awaited<ReturnType<typeof reapOfflineIdentities>>, name: string) =>
    report.skipped.find(s => s.name === name)?.reason;

  it("reaps an empty old offline sidecar: registry, sessions, meta, mark and dangling forward gone; audit written; trace kept", async () => {
    // Mutations caught: skipping HDEL/DEL in the script; dropping the audit XADD; deleting inbox-trace.
    const { name, sid } = await dead();
    await redis.set(SESSION_KEYS.forward(name), "to:nobody-live", "EX", 100);
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(report.candidates.map(c => c.name)).toEqual([name]);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
    expect(await redis.exists(SESSION_KEYS.agentSessions(name), SESSION_KEYS.session(sid), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.forward(name))).toBe(0);
    expect(await redis.sismember(SESSION_KEYS.provisional, name)).toBe(0);
    expect(await redis.exists(SESSION_KEYS.inboxTrace(name))).toBe(1);
    const audit = await redis.xrange(SESSION_KEYS.identityAudit, "-", "+");
    expect(audit.filter(([, f]) => f.includes(name) && f.includes(REAP_AUDIT_REASON))).toHaveLength(1);
  });

  it("keeps a forward tombstone that points at a live registered name", async () => {
    // Mutation caught: deleting every forward tombstone unconditionally.
    const { name } = await dead();
    await redis.hset(SESSION_KEYS.registry, "bound-mailbox", JSON.stringify({ name: "bound-mailbox" }));
    await redis.set(SESSION_KEYS.forward(name), "to:bound-mailbox");
    await reapOfflineIdentities(redis, { dryRun: false });
    expect(await redis.get(SESSION_KEYS.forward(name))).toBe("to:bound-mailbox");
  });

  const exclusions: ReadonlyArray<[SkipReason, (n: string, sid: string) => Promise<unknown>, string]> = [
    ["live_lease", (_n, sid) => redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30), "lease loop removed"],
    ["heartbeat", n => redis.set(SESSION_KEYS.heartbeat(n), "1", "EX", 30), "heartbeat check removed"],
    ["queue_not_empty", n => redis.rpush(SESSION_KEYS.queue(n), "{}"), "LLEN queue check removed"],
    ["claims_outstanding", n => redis.zadd(CLAIM_KEYS.index(n), Date.now() + 1000, "c1"), "ZCARD check removed"],
    ["dlq_not_empty", n => redis.lpush(DLQ_KEYS.list(n), "{}"), "DLQ check removed"],
    ["runtime_mapping_reference", n => redis.set(SESSION_KEYS.runtimeMailbox("codex", "r1"), JSON.stringify({ agent: n, working_directory: "/w" })), "mapping scan removed"],
    ["outstanding_request", n => redis.set(SESSION_KEYS.outstanding(n, "req-1"), n, "EX", 100), "outstanding scan removed"],
    ["durable_actor", n => redis.hset(ACTOR_KEYS.profiles, n, "{}"), "actor-directory check removed"],
    ["wrapper_claim", n => redis.set(SESSION_KEYS.wrapperClaim(n), "x"), "wrapper claim check removed"],
  ];
  for (const [reason, arrange, mutation] of exclusions) {
    it(`skips a name with ${reason} and leaves it registered (catches: ${mutation})`, async () => {
      const { name, sid } = await dead();
      await arrange(name, sid);
      const report = await reapOfflineIdentities(redis, { dryRun: false });
      expect(skipOf(report, name)).toBe(reason);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    });
  }

  it("skips a name younger than minAge by registered_at or by a newer session stamp", async () => {
    // Mutations caught: inverted age comparison; ignoring session stamps.
    const young = await dead(undefined, new Date().toISOString());
    const freshSession = await dead();
    await redis.hset(SESSION_KEYS.session(freshSession.sid), "last_seen", new Date().toISOString());
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, young.name)).toBe("too_young");
    expect(skipOf(report, freshSession.name)).toBe("too_young");
  });

  it("never reaps claude-* or SYSTEM_SENDER even under a widened prefix", async () => {
    // Mutation caught: removing the static name exclusions.
    await dead(`claude-${randomUUID()}`);
    await dead(SYSTEM_SENDER);
    const report = await reapOfflineIdentities(redis, { dryRun: false, namePrefix: "", nameShape: /^/u });
    expect(report.candidates).toEqual([]);
    expect(report.skipped.map(s => s.reason).sort()).toEqual(["claude_name", "reserved_name"]);
  });

  it("ignores names outside the prefix entirely", async () => {
    // Mutation caught: dropping the prefix filter.
    const { name } = await dead(`other-${randomUUID()}`);
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(report.examined).toBe(0);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
  });

  it("dry run lists the candidate and changes nothing", async () => {
    // Mutation caught: ignoring the dry-run flag in the script.
    const { name } = await dead();
    const before = (await redis.keys("gptq:*")).sort();
    const report = await reapOfflineIdentities(redis, { dryRun: true });
    expect(report.candidates.map(c => c.name)).toEqual([name]);
    expect(report.counts.would_reap).toBe(1);
    expect((await redis.keys("gptq:*")).sort()).toEqual(before);
  });

  it("refuses at delete time when a message arrives after the scan, or the registration changes", async () => {
    // Mutations caught: dropping the in-script queue re-check; dropping the registry-value equality check.
    const a = await dead(), b = await dead();
    const real = redis.eval.bind(redis);
    let injected = false;
    (redis as unknown as { eval: typeof redis.eval }).eval = (async (...args: Parameters<typeof redis.eval>) => {
      if (!injected) {
        injected = true;
        await redis.rpush(SESSION_KEYS.queue(a.name), "{\"id\":\"late\"}");
        await redis.hset(SESSION_KEYS.registry, b.name, JSON.stringify({ name: b.name, registered_at: new Date().toISOString() }));
      }
      return real(...args);
    }) as typeof redis.eval;
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, a.name)).toBe("queue_not_empty");
    expect(skipOf(report, b.name)).toBe("changed_before_delete");
    expect(await redis.lrange(SESSION_KEYS.queue(a.name), 0, -1)).toEqual(["{\"id\":\"late\"}"]);
    expect(await redis.hexists(SESSION_KEYS.registry, a.name)).toBe(1);
  });

  it("is idempotent and honours the limit", async () => {
    // Mutations caught: limit ignored; a second run re-reporting reaped names.
    await dead(); await dead(); await dead();
    const first = await reapOfflineIdentities(redis, { dryRun: false, limit: 2 });
    expect(first.candidates).toHaveLength(2);
    expect(first.counts.limit_reached).toBe(1);
    const second = await reapOfflineIdentities(redis, { dryRun: false });
    expect(second.candidates).toHaveLength(1);
    const third = await reapOfflineIdentities(redis, { dryRun: false });
    expect(third.candidates).toHaveLength(0);
    expect(third.examined).toBe(0);
  });

  /** Run `mutate` once, between the TS scan and the single atomic Lua step. */
  const beforeStep = (mutate: () => Promise<unknown>) => {
    const real = redis.eval.bind(redis);
    let done = false;
    (redis as unknown as { eval: typeof redis.eval }).eval = (async (...args: Parameters<typeof redis.eval>) => {
      if (!done) { done = true; await mutate(); }
      return real(...args);
    }) as typeof redis.eval;
  };

  const newlyBlocking: ReadonlyArray<[SkipReason, (n: string) => Promise<unknown>]> = [
    ["dlq_dropped", n => redis.set(DLQ_KEYS.dropped(n), "3")],
    ["dlq_reason", n => redis.hset(DLQ_KEYS.reasons(n), "m1", "recipient_retired")],
    ["recover_count", n => redis.set(CLAIM_KEYS.recoverCount(n, "m:1"), "2")],
    ["wake_lease", n => redis.set(WAKE_LEASE_KEYS.lease(n), "{}")],
    ["activation", n => redis.set(SESSION_KEYS.activation(n), "{}")],
    ["agent_profile", n => redis.set(SESSION_KEYS.agentProfile(n), "{}")],
    ["outbound_activity", n => redis.set(SESSION_KEYS.outboundActivity(n), "1")],
    ["idempotency", n => redis.set(SESSION_KEYS.idempotencyFor(n, "peer", "k"), "id")],
    ["runtime_binding", n => redis.set(SESSION_KEYS.runtimeBinding(n), "{}")],
    ["legacy_agent_hash", n => redis.hset(SESSION_KEYS.agent(n), "x", "1")],
    ["registration_in_flight", n => redis.hset(SESSION_KEYS.registryPending, n, "{}")],
  ];
  for (const [reason, arrange] of newlyBlocking) {
    it(`blocks on name-owned ${reason} state, including when it appears just before the atomic step`, async () => {
      // Mutation caught: dropping the ${reason} entry from the in-Lua blocking table.
      const { name } = await dead();
      beforeStep(() => arrange(name));
      const report = await reapOfflineIdentities(redis, { dryRun: false });
      expect(skipOf(report, name)).toBe(reason);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    });
  }

  it("race: a leased session appears in the set, or as a record outside the set, between scan and step", async () => {
    // Mutations caught: trusting the set alone; lease-checking only snapshot sids without the outside-snapshot abort.
    const inSet = await dead(), outside = await dead(), outsideNoLease = await dead();
    beforeStep(async () => {
      const a = randomUUID(), b = randomUUID(), c = randomUUID();
      await redis.sadd(SESSION_KEYS.agentSessions(inSet.name), a);
      await redis.hset(SESSION_KEYS.session(a), "agent_name", inSet.name, "created_at", old);
      await redis.set(SESSION_KEYS.lease(a), "1", "EX", 30);
      await redis.hset(SESSION_KEYS.session(b), "agent_name", outside.name, "created_at", old);
      await redis.set(SESSION_KEYS.lease(b), "1", "EX", 30);
      await redis.hset(SESSION_KEYS.session(c), "agent_name", outsideNoLease.name, "created_at", old);
    });
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    for (const { name } of [inSet, outside, outsideNoLease]) {
      expect(skipOf(report, name)).toBe("session_outside_snapshot");
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    }
  });

  it("a live lease on a session recorded only outside the set (older writer) blocks", async () => {
    // Mutation caught: dropping the record-scan union, leaving the set as the only session source.
    const { name } = await dead();
    const sid = randomUUID();
    await redis.hset(SESSION_KEYS.session(sid), "agent_name", name, "created_at", old, "last_seen", old);
    await redis.set(SESSION_KEYS.lease(sid), "1", "EX", 30);
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, name)).toBe("live_lease");
  });

  it("race: a runtime-mailbox mapping is created or re-pointed, or an outstanding key appears, between scan and step", async () => {
    // Mutations caught: removing the in-Lua keyspace scan (mapping/outstanding checked only before the step).
    const created = await dead(), repointed = await dead(), owing = await dead(), garbled = await dead();
    const mapping = SESSION_KEYS.runtimeMailbox("codex", "r-repoint");
    await redis.set(mapping, JSON.stringify({ agent: "someone-else", working_directory: "/w" }));
    beforeStep(async () => {
      await redis.set(SESSION_KEYS.runtimeMailbox("pi", "r-new"), JSON.stringify({ agent: created.name, working_directory: "/w" }));
      await redis.set(mapping, JSON.stringify({ agent: repointed.name, working_directory: "/w" }));
      await redis.set(SESSION_KEYS.outstanding(owing.name, "req:with:colons"), "peer", "EX", 100);
      await redis.set(SESSION_KEYS.runtimeMailbox("pi", "r-bad"), `not json ${garbled.name}`);
    });
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, created.name)).toBe("runtime_mapping_reference");
    expect(skipOf(report, repointed.name)).toBe("runtime_mapping_reference");
    expect(skipOf(report, owing.name)).toBe("outstanding_request");
    expect(skipOf(report, garbled.name)).toBe("runtime_mapping_reference");
  });

  it("missing or unparseable registered_at, or a session record without stamps, is age_unknown", async () => {
    // Mutation caught: treating absent age evidence as old enough.
    const missing = await dead(undefined, null);
    const garbage = await dead(undefined, "not-a-date");
    const bare = await dead();
    await redis.hdel(SESSION_KEYS.session(bare.sid), "created_at", "last_seen");
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    for (const { name } of [missing, garbage, bare]) expect(skipOf(report, name)).toBe("age_unknown");
    expect(report.candidates).toEqual([]);
  });

  it("default sidecar shape excludes non-uuid and ':'-suffixed names under the prefix", async () => {
    // Mutation caught: dropping the shape filter (prefix-only matching).
    const good = await dead();
    await dead(`gptqueue-shell-pi-x-not-a-uuid`);
    await dead(`${good.name}:evil`);
    const report = await reapOfflineIdentities(redis, { dryRun: true });
    expect(report.examined).toBe(1);
    expect(report.candidates.map(c => c.name)).toEqual([good.name]);
  });

  it("adversarial names neither broaden matching nor misattribute; a name prefixing another + ':' fails closed", async () => {
    // Mutations caught: glob-built SCAN patterns; last-colon owner parsing.
    const names = ["a*", "a?b", "a[1]", "x:y", "a", "a:b", "ab", "a\\"];
    for (const n of names) await dead(n);
    await redis.set(SESSION_KEYS.outstanding("a:b", "r"), "peer");       // owner is "a" or "a:b": both blocked
    await redis.rpush(SESSION_KEYS.queue("ab"), "{}");                    // must not leak to "a*" / "a?b"
    await redis.set(CLAIM_KEYS.recoverCount("abc", "m"), "1");            // unrelated name, blocks nobody
    const report = await reapOfflineIdentities(redis, { dryRun: false, namePrefix: "", nameShape: /^/u });
    expect(report.candidates.map(c => c.name).sort()).toEqual(["a*", "a?b", "a[1]", "a\\", "x:y"].sort());
    expect(skipOf(report, "a")).toBe("outstanding_request");
    expect(skipOf(report, "a:b")).toBe("outstanding_request");
    expect(skipOf(report, "ab")).toBe("queue_not_empty");
    for (const n of ["a", "a:b", "ab"]) expect(await redis.hexists(SESSION_KEYS.registry, n)).toBe(1);
  });

  it("a wrong-typed forward key skips only that name with unexpected_key_type; nothing of it is deleted", async () => {
    // Mutation caught: dropping the phase-A TYPE check (GET would abort mid-script after deletes).
    const bad = await dead(), good = await dead();
    await redis.hset(SESSION_KEYS.forward(bad.name), "x", "1");
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, bad.name)).toBe("unexpected_key_type");
    expect(report.candidates.map(c => c.name)).toEqual([good.name]);
    expect(await redis.exists(SESSION_KEYS.session(bad.sid), SESSION_KEYS.agentSessions(bad.name), SESSION_KEYS.mailboxMeta(bad.name))).toBe(3);
    expect(await redis.hexists(SESSION_KEYS.registry, bad.name)).toBe(1);
  });

  it("a wrong-typed audit key refuses the whole batch with nothing written", async () => {
    // Mutation caught: dropping the batch-wide TYPE check (XADD would fail after the first deletes).
    const a = await dead(), b = await dead();
    await redis.set(SESSION_KEYS.identityAudit, "not-a-stream");
    const before = (await redis.keys("gptq:*")).sort();
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(report.candidates).toEqual([]);
    expect(skipOf(report, a.name)).toBe("unexpected_key_type");
    expect(skipOf(report, b.name)).toBe("unexpected_key_type");
    expect((await redis.keys("gptq:*")).sort()).toEqual(before);
  });

  it("race: a snapshot session reassigned to another agent is never deleted; the name is refused", async () => {
    // Mutation caught: deleting snapshot sids without re-checking agent_name in the step.
    const victim = await dead(), other = await dead();
    beforeStep(async () => {
      await redis.hset(SESSION_KEYS.session(victim.sid), "agent_name", other.name);
      await redis.set(SESSION_KEYS.lease(victim.sid), "other", "EX", 1000);
    });
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, victim.name)).toBe("session_owner_mismatch");
    expect(await redis.hget(SESSION_KEYS.session(victim.sid), "agent_name")).toBe(other.name);
    expect(await redis.exists(SESSION_KEYS.lease(victim.sid))).toBe(1);
  });

  it("a set member whose record belongs to another agent refuses the name and leaves the other agent intact", async () => {
    // Mutation caught: trusting set membership as ownership.
    const { name } = await dead();
    const foreign = randomUUID(), owner = `live-${randomUUID()}`;
    await redis.sadd(SESSION_KEYS.agentSessions(name), foreign);
    await redis.hset(SESSION_KEYS.session(foreign), "agent_name", owner, "created_at", old, "last_seen", old);
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    expect(skipOf(report, name)).toBe("session_owner_mismatch");
    expect(await redis.hget(SESSION_KEYS.session(foreign), "agent_name")).toBe(owner);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
  });

  it("a gptq:claims record naming the actor or one of its sessions blocks, even with no claims-index", async () => {
    // Mutation caught: dropping the HSCAN of the global claims hash.
    const byActor = await dead(), bySession = await dead(), garbled = await dead();
    await redis.hset(CLAIM_KEYS.claims, "c1", JSON.stringify({ actor_id: byActor.name, session_id: "s" }));
    await redis.hset(CLAIM_KEYS.claims, "c2", JSON.stringify({ actor_id: "x", session_id: bySession.sid }));
    await redis.hset(CLAIM_KEYS.claims, "c3", `{broken ${garbled.name}`);
    const report = await reapOfflineIdentities(redis, { dryRun: false });
    for (const { name } of [byActor, bySession, garbled]) expect(skipOf(report, name)).toBe("claim_record");
  });

  it("limit bounds the input batch and reports how many remain", async () => {
    // Mutation caught: applying the limit after the step (unbounded Lua batch).
    await dead(); await dead(); await dead();
    const report = await reapOfflineIdentities(redis, { dryRun: true, limit: 1 });
    expect(report.candidates).toHaveLength(1);
    expect(report.remaining).toBe(2);
    expect(report.counts.limit_reached).toBe(2);
  });

  it("an unreadable reference (hash-typed runtime-mailbox, owner-less session record) refuses the whole batch", async () => {
    // Mutation caught: silently skipping references the scan cannot read or attribute.
    for (const arrange of [
      () => redis.hset(SESSION_KEYS.runtimeMailbox("pi", "r-hash"), "agent", "x"),
      () => redis.hset(SESSION_KEYS.session(randomUUID()), "created_at", old),
    ]) {
      await flushTestKeys(redis, URL);
      const a = await dead(), b = await dead();
      await arrange();
      const before = await Promise.all((await redis.keys("gptq:*")).sort().map(async k => [k, await redis.dump(k)]));
      const report = await reapOfflineIdentities(redis, { dryRun: false });
      expect(report.candidates).toEqual([]);
      for (const { name } of [a, b]) expect(skipOf(report, name)).toBe("unexpected_key_type");
      const after = await Promise.all((await redis.keys("gptq:*")).sort().map(async k => [k, await redis.dump(k)]));
      expect(after).toEqual(before);
    }
  });

  it("growth breaker: over maxKeys the step neither scans nor writes; keyspace byte-identical", async () => {
    // Mutation caught: running the DBSIZE breaker after the scan/writes, or not at all.
    const a = await dead(), b = await dead();
    const snap = async () => Promise.all((await redis.keys("*")).sort().map(async k => [k, await redis.dump(k)]));
    const before = await snap();
    const keys = await redis.dbsize();
    const report = await reapOfflineIdentities(redis, { dryRun: false, maxKeys: keys - 1 });
    expect(report.keyspace_too_large).toBe(true);
    expect(report.keyspace_keys).toBe(keys);
    expect(report.candidates).toEqual([]);
    for (const { name } of [a, b]) expect(skipOf(report, name)).toBe("keyspace_too_large");
    expect(await snap()).toEqual(before);
  });

  it("growth breaker at exactly maxKeys does not trip", async () => {
    // Mutation caught: an off-by-one (>=) breaker.
    const { name } = await dead();
    const report = await reapOfflineIdentities(redis, { dryRun: false, maxKeys: await redis.dbsize() });
    expect(report.keyspace_too_large).toBe(false);
    expect(report.candidates.map(c => c.name)).toEqual([name]);
  });
});
