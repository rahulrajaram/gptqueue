/**
 * P1 adoption-refusal visibility: every runtime refusal is classified end to
 * end (code, reason, first blocking signal, recheck hint), infrastructure
 * failures stay unclassified, and the controller, bootstrap and lifecycle
 * log report a pending adoption truthfully.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import {
  AdoptionRefusedError, operatorActionRequired, parseGuardReply, RUNTIME_ADOPTION_REFUSALS,
  type AdoptionRefusal, type AdoptionRefusalReason,
} from "../src/core/adoption-refusal.js";
import { restoreRuntimeMailbox, RuntimeMailboxAdoptionError, RuntimeMailboxAdoptionRefused } from "../src/registered-shell/runtime-mailbox.js";
import { createRuntimeController, refusedContinuity } from "../src/registered-shell/runtime-controller.js";
import { bindingLifecycleEntry, bootstrapCodexBinding, withBindingState, type CodexBindingState } from "../src/registered-shell/codex-bootstrap.js";
import { createLifecycleLog } from "../src/registered-shell/lifecycle-log.js";
import type { RuntimeAdapter, RuntimeBinding } from "../src/registered-shell/runtime.js";
import { flushTestKeys, assertNotLiveDb } from "./helpers/redis-test-utils.js";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
assertNotLiveDb(redisUrl, "adoption refusal tests");
const clients: RedisClient[] = [];
const connections: Redis[] = [];
const closers: Array<() => Promise<unknown>> = [];

// Start from an empty gptq:* namespace: other suites may leave audit entries behind.
beforeEach(async () => {
  const redis = new Redis(redisUrl);
  try { await flushTestKeys(redis, redisUrl); } finally { await redis.quit(); }
});

afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const client of clients.splice(0)) client.forceDisconnect();
  for (const redis of connections.splice(0)) { await flushTestKeys(redis, redisUrl); await redis.quit(); }
});

/** A provisional client whose runtime mapping names a registered, otherwise idle target. */
const setup = async (kind: "codex" | "pi" = "codex") => {
  const redis = new Redis(redisUrl); connections.push(redis);
  const target = `target-${randomUUID()}`;
  const source = `source-${randomUUID()}`;
  const binding: RuntimeBinding = { client: kind, runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace/project" };
  const key = runtimeMailboxKey(binding);
  const mapping = JSON.stringify({ agent: target, working_directory: binding.working_directory });
  await redis.hset(SESSION_KEYS.registry, target, JSON.stringify({ name: target }));
  await redis.set(key, mapping);
  const client = new RedisClient(null, redisUrl); clients.push(client);
  await client.register("both", source);
  return { redis, client, source, target, binding, key, mapping };
};

const rejection = (work: Promise<unknown>): Promise<unknown> =>
  work.then(() => { throw new Error("expected a rejection"); }, (error: unknown) => error);

/** adoptIdentity's refusal, asserting that it wrote nothing. */
const refusal = async (s: Awaited<ReturnType<typeof setup>>, expected = s.mapping): Promise<AdoptionRefusal> => {
  const error = await rejection(s.client.adoptIdentity(s.source, s.target, s.key, expected, s.binding.runtime_id));
  expect(error).toBeInstanceOf(AdoptionRefusedError);
  expect(s.client.agentName).toBe(s.source);
  expect(await s.redis.get(s.key)).toBe(s.mapping);
  expect(await s.redis.xlen(SESSION_KEYS.continuityAudit)).toBe(0);
  return (error as AdoptionRefusedError).refusal;
};

const redisNowMs = async (redis: Redis) => {
  const [seconds, micros] = await redis.time();
  return Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
};

describe("refusal taxonomy (core/adoption-refusal.ts)", () => {
  it("parses success, bare codes and tuples, and maps every runtime code to its reason", () => {
    expect(parseGuardReply(1, RUNTIME_ADOPTION_REFUSALS)).toBeNull();
    expect([...RUNTIME_ADOPTION_REFUSALS]).toEqual([
      [-1, "mapping_changed"], [-2, "registration_missing"], [-3, "source_session_invalid"],
      [-4, "source_ownership_conflict"], [-5, "target_occupied"], [-6, "source_busy"],
    ]);
    expect(parseGuardReply(-3, RUNTIME_ADOPTION_REFUSALS)).toEqual({ code: -3, reason: "source_session_invalid", signal: null, recheckAfterMs: null });
    expect(parseGuardReply([-5, "claims_index", 1_800_000_000_000], RUNTIME_ADOPTION_REFUSALS))
      .toEqual({ code: -5, reason: "target_occupied", signal: "claims_index", recheckAfterMs: 1_800_000_000_000 });
    expect(parseGuardReply([-6, "wrapper_claim", null], RUNTIME_ADOPTION_REFUSALS))
      .toEqual({ code: -6, reason: "source_busy", signal: "wrapper_claim", recheckAfterMs: null });
  });

  it("treats an unexpected reply as an infrastructure error, never a refusal", () => {
    for (const reply of [0, -7, "OK", null, [-5, "not_a_signal"], ["-5", "heartbeat"]]) {
      const error = (() => { try { parseGuardReply(reply, RUNTIME_ADOPTION_REFUSALS); } catch (e) { return e; } return undefined; })();
      expect(error, JSON.stringify(reply)).toBeInstanceOf(Error);
      expect(error).not.toBeInstanceOf(AdoptionRefusedError);
    }
  });

  it("requires an operator only for refusals that cannot clear on their own", () => {
    const expected: Record<AdoptionRefusalReason, boolean> = {
      mapping_changed: true, registration_missing: true, source_session_invalid: true, source_ownership_conflict: true,
      registration_changed: true, target_occupied: false, source_busy: false,
    };
    for (const [reason, required] of Object.entries(expected)) expect(operatorActionRequired(reason as AdoptionRefusalReason), reason).toBe(required);
  });
});

describe("adoptIdentity refusals, end to end", () => {
  it("-1 mapping_changed", async () => {
    const s = await setup();
    expect(await refusal(s, "stale-mapping")).toEqual({ code: -1, reason: "mapping_changed", signal: null, recheckAfterMs: null });
  });

  it("-2 registration_missing", async () => {
    const s = await setup();
    await s.redis.hdel(SESSION_KEYS.registry, s.target);
    expect(await refusal(s)).toEqual({ code: -2, reason: "registration_missing", signal: null, recheckAfterMs: null });
  });

  it("-3 source_session_invalid", async () => {
    const s = await setup();
    await s.redis.hset(SESSION_KEYS.session(s.client.sessionId!), "agent_name", `someone-${randomUUID()}`);
    expect(await refusal(s)).toEqual({ code: -3, reason: "source_session_invalid", signal: null, recheckAfterMs: null });
  });

  it("-4 source_ownership_conflict", async () => {
    const s = await setup();
    await s.redis.sadd(SESSION_KEYS.agentSessions(s.source), randomUUID());
    expect(await refusal(s)).toEqual({ code: -4, reason: "source_ownership_conflict", signal: null, recheckAfterMs: null });
  });

  const targetSignals: Array<[string, (redis: Redis, target: string) => Promise<unknown>]> = [
    ["runtime_binding", (redis, target) => redis.set(SESSION_KEYS.runtimeBinding(target), "bound")],
    ["heartbeat", (redis, target) => redis.set(SESSION_KEYS.heartbeat(target), "alive", "EX", 30)],
    ["wrapper_claim", (redis, target) => redis.set(SESSION_KEYS.wrapperClaim(target), "claimed")],
    ["live_session_lease", async (redis, target) => {
      const sid = randomUUID();
      await redis.sadd(SESSION_KEYS.agentSessions(target), sid);
      await redis.set(SESSION_KEYS.lease(sid), "alive", "EX", 30);
    }],
  ];
  it.each(targetSignals)("-5 target_occupied names the signal %s (no recheck hint)", async (signal, inject) => {
    const s = await setup();
    await inject(s.redis, s.target);
    expect(await refusal(s)).toEqual({ code: -5, reason: "target_occupied", signal, recheckAfterMs: null });
  });

  it("-5 claims_index hints the earliest UNEXPIRED target claim expiry", async () => {
    const s = await setup();
    const now = await redisNowMs(s.redis);
    await s.redis.zadd(CLAIM_KEYS.index(s.target), now - 60_000, "expired", now + 120_000, "later", now + 60_000, "sooner");
    expect(await refusal(s)).toEqual({ code: -5, reason: "target_occupied", signal: "claims_index", recheckAfterMs: now + 60_000 });
  });

  it("-5 reports the FIRST blocking signal when several block", async () => {
    const s = await setup();
    await s.redis.zadd(CLAIM_KEYS.index(s.target), Date.now() + 60_000, "claim");
    await s.redis.set(SESSION_KEYS.heartbeat(s.target), "alive", "EX", 30);
    expect(await refusal(s)).toMatchObject({ code: -5, signal: "heartbeat" });
  });

  it("-6 source_busy claims_index hints the earliest unexpired source claim, or nothing when all expired", async () => {
    const s = await setup();
    const now = await redisNowMs(s.redis);
    await s.redis.zadd(CLAIM_KEYS.index(s.source), now - 1_000, "expired");
    expect(await refusal(s)).toEqual({ code: -6, reason: "source_busy", signal: "claims_index", recheckAfterMs: null });
    await s.redis.zadd(CLAIM_KEYS.index(s.source), now + 90_000, "live");
    expect(await refusal(s)).toEqual({ code: -6, reason: "source_busy", signal: "claims_index", recheckAfterMs: now + 90_000 });
  });

  it("-6 source_busy outbound_activity hints now plus the marker's TTL", async () => {
    const s = await setup();
    const before = await redisNowMs(s.redis);
    await s.redis.set(SESSION_KEYS.outboundActivity(s.source), "1", "EX", 3_600);
    const result = await refusal(s);
    const after = await redisNowMs(s.redis);
    expect(result).toMatchObject({ code: -6, reason: "source_busy", signal: "outbound_activity" });
    expect(result.recheckAfterMs).toBeGreaterThanOrEqual(before + 3_600_000 - 1_000);
    expect(result.recheckAfterMs).toBeLessThanOrEqual(after + 3_600_000);
  });

  it("-6 source_busy wrapper_claim", async () => {
    const s = await setup();
    await s.redis.set(SESSION_KEYS.wrapperClaim(s.source), "claimed");
    expect(await refusal(s)).toEqual({ code: -6, reason: "source_busy", signal: "wrapper_claim", recheckAfterMs: null });
  });

  it("restoreRuntimeMailbox surfaces a refusal as RuntimeMailboxAdoptionRefused {reason, signal, target, code}", async () => {
    const s = await setup();
    await s.redis.set(SESSION_KEYS.heartbeat(s.target), "alive", "EX", 30);
    const error = await rejection(restoreRuntimeMailbox(s.client, s.binding));
    expect(error).toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    expect(error).toMatchObject({ reason: "target_occupied", signal: "heartbeat", target: s.target, code: -5, recheckAfterMs: null });
    expect((error as Error).message).toMatch(/adoption refused \(-5\): target_occupied, first blocker heartbeat/);
  });

  it("restoreRuntimeMailbox keeps an infrastructure failure distinct from a refusal", async () => {
    const s = await setup();
    vi.spyOn(s.client, "adoptIdentity").mockRejectedValueOnce(new Error("Connection is closed."));
    const error = await rejection(restoreRuntimeMailbox(s.client, s.binding));
    expect(error).toBeInstanceOf(RuntimeMailboxAdoptionError);
    expect(error).not.toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    expect(error).toMatchObject({ target: s.target, message: "Connection is closed." });
  });
});

describe("runtime controller continuity status", () => {
  const adapter = async (identity: RuntimeBinding): Promise<RuntimeAdapter> =>
    ({ binding: identity, activate: async () => ({ status: "queued" }), close: async () => undefined });
  const controllerFor = (s: Awaited<ReturnType<typeof setup>>) => {
    const controller = createRuntimeController(s.client, { client: s.binding.client, working_directory: s.binding.working_directory }, adapter);
    closers.push(() => controller.close());
    return controller;
  };

  it.each(["codex", "pi"] as const)("%s: a refusal shows continuity with dispatcher off, tracks changes, and clears on success", async (kind) => {
    const s = await setup(kind);
    const controller = controllerFor(s);
    expect(controller.status()).toMatchObject({ dispatcher: "off", continuity: null });
    expect(controller.status()).not.toHaveProperty("readiness");

    await s.redis.set(SESSION_KEYS.heartbeat(s.target), "alive", "EX", 30);
    expect(await rejection(controller.bind(s.binding))).toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    const first = controller.status();
    expect(first).toMatchObject({
      agent: s.source, activation_ready: false, dispatcher: "off", readiness: "awaiting_adoption", runtime: s.binding,
      continuity: { current_mailbox: s.source, target: s.target, reason: "target_occupied", signal: "heartbeat", code: -5,
        recheck_after: null, recheck_basis: null, operator_action_required: false },
    });
    const firstContinuity = first.continuity as Record<string, string>;
    expect(firstContinuity.first_refused_at).toBe(firstContinuity.last_attempt_at);

    // The blocker changes: the heartbeat lapses but an unexpired claim remains.
    await s.redis.del(SESSION_KEYS.heartbeat(s.target));
    const expiry = (await redisNowMs(s.redis)) + 60_000;
    await s.redis.zadd(CLAIM_KEYS.index(s.target), expiry, "claim");
    expect(await rejection(controller.bind(s.binding))).toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    const second = controller.status().continuity as Record<string, unknown>;
    expect(second).toMatchObject({ signal: "claims_index", recheck_after: new Date(expiry).toISOString(),
      recheck_basis: "earliest_unexpired_claim_expiry", first_refused_at: firstContinuity.first_refused_at });
    expect(Date.parse(second.last_attempt_at as string)).toBeGreaterThanOrEqual(Date.parse(firstContinuity.first_refused_at!));

    await s.redis.zrem(CLAIM_KEYS.index(s.target), "claim");
    expect(await controller.bind(s.binding)).toMatchObject({ agent: s.target, activation_ready: true, dispatcher: "on", continuity: null });
    expect(controller.status()).not.toHaveProperty("readiness");
  });

  it("an infrastructure failure is not recorded as a pending adoption", async () => {
    const s = await setup();
    const controller = controllerFor(s);
    vi.spyOn(s.client, "adoptIdentity").mockRejectedValueOnce(new Error("Connection is closed."));
    const error = await rejection(controller.bind(s.binding));
    expect(error).toBeInstanceOf(RuntimeMailboxAdoptionError);
    expect(error).not.toBeInstanceOf(RuntimeMailboxAdoptionRefused);
    expect(controller.status()).toMatchObject({ activation_ready: false, dispatcher: "off", continuity: null });
    expect(controller.status()).not.toHaveProperty("readiness");
  });

  it("refusedContinuity marks operator action and resets first_refused_at for a new target", () => {
    const refused = (target: string, reason: AdoptionRefusalReason, code: number) =>
      new RuntimeMailboxAdoptionRefused(new AdoptionRefusedError("Continuity adoption refused", { code, reason, signal: null, recheckAfterMs: null }), target);
    const first = refusedContinuity(null, refused("t1", "registration_missing", -2), "p", Date.parse("2026-10-18T19:34:00Z"));
    expect(first).toMatchObject({ target: "t1", reason: "registration_missing", operator_action_required: true,
      first_refused_at: "2026-10-18T19:34:00.000Z", last_attempt_at: "2026-10-18T19:34:00.000Z" });
    const same = refusedContinuity(first, refused("t1", "source_busy", -6), "p", Date.parse("2026-10-18T19:35:00Z"));
    expect(same).toMatchObject({ operator_action_required: false, first_refused_at: "2026-10-18T19:34:00.000Z", last_attempt_at: "2026-10-18T19:35:00.000Z" });
    const other = refusedContinuity(same, refused("t2", "source_busy", -6), "p", Date.parse("2026-10-18T19:36:00Z"));
    expect(other.first_refused_at).toBe("2026-10-18T19:36:00.000Z");
  });
});

describe("binding supervision with a refused adoption", () => {
  const continuity = { current_mailbox: "provisional", target: "bound-target", reason: "target_occupied", signal: "claims_index", code: -5,
    first_refused_at: "2026-10-18T19:34:00.000Z", last_attempt_at: "2026-10-18T19:40:00.000Z", recheck_after: "2026-10-18T20:25:00.000Z",
    recheck_basis: "earliest_unexpired_claim_expiry", operator_action_required: false };

  it("withBindingState keeps the controller's awaiting_adoption and continuity, adding the retry fields", () => {
    const status = { status: "ok", agent: "provisional", activation_ready: false, dispatcher: "off", readiness: "awaiting_adoption",
      continuity, runtime: { client: "codex", runtime_id: "r", epoch: "r", working_directory: "/w" } };
    const retrying: CodexBindingState = { readiness: "binding_incomplete", binding_attempts: 7,
      next_attempt_at: "2026-10-18T19:41:00.000Z", last_attempt_at: "2026-10-18T19:40:30.000Z", last_binding_outcome: "adoption_refused" };
    expect(withBindingState(status, retrying)).toEqual({ ...status, binding_attempts: 7,
      next_attempt_at: "2026-10-18T19:41:00.000Z", last_attempt_at: "2026-10-18T19:40:30.000Z", last_binding_outcome: "adoption_refused" });
    // Without a pending adoption the supervisor's readiness still applies.
    const { readiness: _readiness, ...unrefused } = status;
    expect(withBindingState({ ...unrefused, continuity: null }, retrying)).toMatchObject({ readiness: "binding_incomplete", continuity: null });
  });

  it("publishes adoption_refused on state changes only, with reason, signal and target for the lifecycle log", async () => {
    const own = "01a10062-ae13-76e3-8760-5289291f4d03";
    const cwd = "/workspace/owned";
    const rpc = {
      close: vi.fn(async () => undefined),
      request: vi.fn(async (method: string, params: Record<string, unknown>) => {
        if (method === "thread/loaded/list") return { data: [own] };
        if (method === "thread/read") return { thread: { id: params.threadId, cwd } };
        if (method === "mcpServer/tool/call") return { structuredContent: { status: "ok", agent: "provisional", runtime: null } };
        throw Error("unexpected method");
      }),
    };
    let attempts = 0;
    let current: Record<string, unknown> | null = null;
    const signals = ["heartbeat", "heartbeat", "heartbeat", "claims_index", "claims_index"] as const;
    const runtime = {
      status: () => ({ activation_ready: false, runtime: attempts ? { runtime_id: own } : null, continuity: current }),
      bind: vi.fn(async (value: RuntimeBinding) => {
        const signal = signals[attempts++];
        if (signal) {
          current = { ...continuity, signal };
          throw new RuntimeMailboxAdoptionRefused(new AdoptionRefusedError("Continuity adoption refused",
            { code: -5, reason: "target_occupied", signal, recheckAfterMs: null }), "bound-target");
        }
        current = null;
        return { activation_ready: true, runtime: value };
      }),
    };
    const entries: Array<Record<string, unknown>> = [];
    expect(await bootstrapCodexBinding(runtime, "provisional", cwd, AbortSignal.timeout(5_000), rpc,
      { fastAttempts: 10, fastDelayMs: 1, random: () => 1,
        onState: (state) => entries.push({ ...bindingLifecycleEntry(state, runtime.status()) }) })).toBe(true);
    expect(runtime.bind).toHaveBeenCalledTimes(6);
    expect(entries).toEqual([
      { event: "native_binding_retrying", phase: "binding", code: undefined },
      { event: "native_binding_retrying", phase: "binding", code: "adoption_refused", reason: "target_occupied", signal: "heartbeat", target: "bound-target" },
      { event: "native_binding_retrying", phase: "binding", code: "adoption_refused", reason: "target_occupied", signal: "claims_index", target: "bound-target" },
      { event: "native_binding_ready", phase: "binding", code: "bound" },
    ]);
  });

  it("status stays current across unchanged refusals while the log records the change once (F3)", async () => {
    const own = "01a10062-ae13-76e3-8760-5289291f4d03";
    const rpc = {
      close: vi.fn(async () => undefined),
      request: vi.fn(async (method: string, params: Record<string, unknown>) => {
        if (method === "thread/loaded/list") return { data: [own] };
        if (method === "thread/read") return { thread: { id: params.threadId, cwd: "/w" } };
        return { structuredContent: { status: "ok", agent: "provisional", runtime: null } };
      }),
    };
    const stop = new AbortController();
    let attempts = 0;
    let clock = Date.parse("2026-10-19T00:00:00Z");
    const runtime = {
      status: () => ({ status: "ok", agent: "provisional", activation_ready: false, dispatcher: "off",
        readiness: attempts ? "awaiting_adoption" : undefined, continuity: attempts ? { ...continuity, signal: "heartbeat" } : null,
        runtime: attempts ? { runtime_id: own } : null }),
      bind: vi.fn(async () => {
        if (++attempts === 3) stop.abort();
        throw new RuntimeMailboxAdoptionRefused(new AdoptionRefusedError("Continuity adoption refused",
          { code: -5, reason: "target_occupied", signal: "heartbeat", recheckAfterMs: null }), "bound-target");
      }),
    };
    const statuses: Array<Record<string, unknown>> = [];
    const logged: Array<Record<string, unknown>> = [];
    expect(await bootstrapCodexBinding(runtime, "provisional", "/w", stop.signal, rpc, {
      fastAttempts: 10, fastDelayMs: 1, random: () => 1, now: () => (clock += 1_000),
      onProgress: (state) => statuses.push(withBindingState(runtime.status(), state)),
      onState: (state) => logged.push({ ...bindingLifecycleEntry(state, runtime.status()) }),
    })).toBe(false);
    // Every attempt refreshes the observable retry state ...
    const retries = statuses.filter((s) => typeof s.binding_attempts === "number" && (s.binding_attempts as number) > 0);
    expect(retries.map((s) => s.binding_attempts)).toEqual([1, 2, 3]);
    expect(new Set(retries.map((s) => s.next_attempt_at)).size).toBe(3);
    expect(new Set(retries.map((s) => s.last_attempt_at)).size).toBe(3);
    for (const s of retries) expect(s).toMatchObject({ readiness: "awaiting_adoption", last_binding_outcome: "adoption_refused", continuity: { signal: "heartbeat" } });
    // ... while three identical refusals log once (plus the start and the stop).
    expect(logged.filter((e) => e.code === "adoption_refused" && e.event === "native_binding_retrying")).toHaveLength(1);
    expect(logged.map((e) => e.event)).toEqual(["native_binding_retrying", "native_binding_retrying", "native_binding_stopped"]);
  });

  it("an infrastructure bind failure keeps the bind_failed outcome", async () => {
    const own = "01a10062-ae13-76e3-8760-5289291f4d03";
    const rpc = {
      close: vi.fn(async () => undefined),
      request: vi.fn(async (method: string, params: Record<string, unknown>) => {
        if (method === "thread/loaded/list") return { data: [own] };
        if (method === "thread/read") return { thread: { id: params.threadId, cwd: "/w" } };
        return { structuredContent: { status: "ok", agent: "a", runtime: null } };
      }),
    };
    const stop = new AbortController();
    const outcomes: Array<string | null> = [];
    const runtime = { status: () => ({ activation_ready: false, runtime: null, continuity: null }),
      bind: vi.fn(async () => { stop.abort(); throw new RuntimeMailboxAdoptionError(new Error("Connection is closed."), "t"); }) };
    expect(await bootstrapCodexBinding(runtime, "a", "/w", stop.signal, rpc,
      { fastDelayMs: 1, onState: (state) => outcomes.push(state.last_binding_outcome) })).toBe(false);
    expect(outcomes).toContain("bind_failed");
    expect(outcomes).not.toContain("adoption_refused");
  });

  it("the lifecycle log records reason, signal and target", async () => {
    const dir = await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "adoption-lifecycle-"));
    const previous = process.env.GPTQ_LOG_DIR;
    process.env.GPTQ_LOG_DIR = dir;
    try {
      const agent = `lifecycle-${randomUUID()}`;
      const log = createLifecycleLog(agent, "codex");
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      log.emit({ event: "native_binding_retrying", phase: "binding", code: "adoption_refused",
        reason: "source_busy", signal: "outbound_activity", target: "bound-target", session_id: "s1" });
      await log.close();
      const [line] = (await readFile(join(dir, `${agent}.jsonl`), "utf8")).trim().split("\n");
      expect(JSON.parse(line!)).toMatchObject({ event: "native_binding_retrying", code: "adoption_refused",
        reason: "source_busy", signal: "outbound_activity", target: "bound-target", session_id: "s1" });
    } finally {
      if (previous === undefined) delete process.env.GPTQ_LOG_DIR; else process.env.GPTQ_LOG_DIR = previous;
      await rm(dir, { recursive: true, force: true });
    }
  });
});
