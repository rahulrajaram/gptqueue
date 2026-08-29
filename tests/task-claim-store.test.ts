import { describe, it, expect, beforeEach, afterEach, afterAll } from "vitest";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";
import { Redis } from "ioredis";
import { spawn } from "child_process";
import { readFileSync } from "fs";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import {
  CLAIM_KEYS,
  DLQ_KEYS,
  SESSION_KEYS,
} from "../src/core/keys.js";
import {
  actorStatus,
  actorStatusSchema,
} from "../src/mcp-server/tools/actor-status.js";
import {
  claimTasks,
  claimTasksSchema,
} from "../src/mcp-server/tools/claim-tasks.js";
import {
  acknowledgeTasks,
  acknowledgeTasksSchema,
} from "../src/mcp-server/tools/acknowledge-tasks.js";
import {
  registerAgent,
  registerAgentSchema,
} from "../src/mcp-server/tools/register-agent.js";
import type { RuntimeLaunchContract } from "../src/core/actor-directory.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

// wake_if_offline admission now requires an operator allowlist. Scaffold one
// permitting the test launcher (process.execPath) and point
// GPTQUEUE_LAUNCH_ALLOWLIST at it so durable actors can be registered.
const allowlist = scaffoldLaunchAllowlist([
  {
    command: process.execPath,
    allowed_args_prefixes: [[], ["-e"]],
    comment: "test process.execPath launcher",
  },
]);
allowlist.set();
afterAll(() => allowlist.cleanup());

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

const T0 = "2030-01-01T00:00:00.000Z";

const pushTasks = async (redis: Redis, actorId: string, tasks: string[]) => {
  if (tasks.length > 0) {
    await redis.rpush(SESSION_KEYS.queue(actorId), ...tasks);
  }
};

const inboxDepth = async (redis: Redis, actorId: string) =>
  redis.llen(SESSION_KEYS.queue(actorId));

const expectOk = <T extends { ok: boolean }>(r: T): Extract<T, { ok: true }> => {
  expect(r.ok).toBe(true);
  if (!r.ok) throw new Error("expected ok result");
  return r;
};

describe("TaskClaimStore", () => {
  let redis: Redis;
  let store: TaskClaimStore;
  const actorId = "claim-actor";

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    store = new TaskClaimStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await redis.quit();
  });

  const claimReq = (overrides: Partial<Parameters<TaskClaimStore["claim"]>[0]> = {}) => ({
    actor_id: actorId,
    session_id: "session-runtime",
    max_batch: 3,
    ttl_seconds: 300,
    now: T0,
    ...overrides,
  });

  it("returns claim:null when the inbox is empty", async () => {
    const res = await store.claim(claimReq());
    expectOk(res);
    expect(res.claim).toBeNull();
  });

  it("claims N <= max_batch messages in pop order", async () => {
    await pushTasks(redis, actorId, ["b-1", "b-2", "b-3", "b-4"]);
    const res = await store.claim(claimReq({ max_batch: 3 }));
    const ok = expectOk(res);
    expect(ok.claim?.tasks).toEqual(["b-1", "b-2", "b-3"]);
    expect(ok.claim?.actor_id).toBe(actorId);
    expect(ok.claim?.session_id).toBe("session-runtime");
    expect(ok.claim?.claimed_at).toBe(T0);
    expect(ok.claim?.expires_at).toBe("2030-01-01T00:05:00.000Z"); // +300s
    // The popped tasks left the inbox; the rest remains.
    expect(await inboxDepth(redis, actorId)).toBe(1);
  });

  it("oversubscribed max_batch is bounded by the available messages", async () => {
    await pushTasks(redis, actorId, ["only-one"]);
    const res = await store.claim(claimReq({ max_batch: 16 }));
    const ok = expectOk(res);
    expect(ok.claim?.tasks).toEqual(["only-one"]);
    expect(await inboxDepth(redis, actorId)).toBe(0);
  });

  it("persists the record and zset index with the correct expiry score", async () => {
    await pushTasks(redis, actorId, ["x"]);
    const res = await store.claim(claimReq());
    const ok = expectOk(res);
    const claimId = ok.claim!.claim_id;

    const stored = await redis.hget(CLAIM_KEYS.claims, claimId);
    expect(stored).not.toBeNull();
    const parsed = JSON.parse(stored!) as { tasks: string[] };
    expect(parsed.tasks).toEqual(["x"]);

    // zset member with score = expires_at epoch ms.
    const score = await redis.zscore(CLAIM_KEYS.index(actorId), claimId);
    expect(score).toBe(String(Date.parse("2030-01-01T00:05:00.000Z")));
  });

  it("activeClaimFor finds a non-expired owned claim; wrong session -> null", async () => {
    await pushTasks(redis, actorId, ["y"]);
    const res = await store.claim(claimReq());
    const ok = expectOk(res);

    const own = await store.activeClaimFor({ actor_id: actorId, session_id: "session-runtime" });
    expect(own?.claim_id).toBe(ok.claim!.claim_id);

    // A different session never sees this runtime's claim.
    const wrong = await store.activeClaimFor({ actor_id: actorId, session_id: "session-other" });
    expect(wrong).toBeNull();
  });

  it("activeClaimFor skips corrupt stored JSON and falls back to null", async () => {
    await redis.zadd(CLAIM_KEYS.index(actorId), Date.now(), "junk-claim");
    await redis.hset(CLAIM_KEYS.claims, "junk-claim", "not-json{{{");
    expect(
      await store.activeClaimFor({ actor_id: actorId, session_id: "session-runtime" })
    ).toBeNull();
  });

  it("get returns the claim, null when absent, and store_corrupt on malformed JSON", async () => {
    expect((await store.get("missing-claim")).claim).toBeNull();

    await pushTasks(redis, actorId, ["z"]);
    const claimed = expectOk(await store.claim(claimReq()));
    const got = await store.get(claimed.claim!.claim_id);
    expect(got.ok).toBe(true);
    if (got.ok) expect(got.claim?.tasks).toEqual(["z"]);

    await redis.hset(CLAIM_KEYS.claims, "corrupt-id", "bad{{{");
    const corrupt = await store.get("corrupt-id");
    expect(corrupt.ok).toBe(false);
    if (!corrupt.ok) expect(corrupt.error.code).toBe("store_corrupt");
  });

  it("acknowledge removes the record and index and returns the task count", async () => {
    await pushTasks(redis, actorId, ["a", "b"]);
    const claimed = expectOk(await store.claim(claimReq()));
    const claimId = claimed.claim!.claim_id;

    const ack = await store.acknowledge({ claim_id: claimId, actor_id: actorId, session_id: "session-runtime" });
    const okAck = expectOk(ack);
    expect(okAck.acknowledged).toBe(2);
    // verify the record/index are cleaned.
    expect(await redis.hget(CLAIM_KEYS.claims, claimId)).toBeNull();
    expect(await redis.zscore(CLAIM_KEYS.index(actorId), claimId)).toBeNull();
  });

  it("a second acknowledge is unknown_claim", async () => {
    await pushTasks(redis, actorId, ["a"]);
    const claimed = expectOk(await store.claim(claimReq()));
    const claimId = claimed.claim!.claim_id;

    expectOk(await store.acknowledge({ claim_id: claimId, actor_id: actorId, session_id: "session-runtime" }));
    const again = await store.acknowledge({ claim_id: claimId, actor_id: actorId, session_id: "session-runtime" });
    expect(again.ok).toBe(false);
    if (!again.ok) expect(again.error.code).toBe("unknown_claim");
  });

  it("a foreign session acknowledge is not_claim_owner naming the owning session", async () => {
    await pushTasks(redis, actorId, ["x1"]);
    const claimed = expectOk(await store.claim(claimReq()));
    const claimId = claimed.claim!.claim_id;

    const foreign = await store.acknowledge({ claim_id: claimId, actor_id: actorId, session_id: "session-other" });
    expect(foreign.ok).toBe(false);
    if (!foreign.ok) {
      expect(foreign.error.code).toBe("not_claim_owner");
      expect(foreign.error.message).toContain("session-runtime");
    }
    // The claim is untouched by a failed foreign ack.
    expect(await redis.hget(CLAIM_KEYS.claims, claimId)).not.toBeNull();
  });

  it("rejects an invalid claim request", async () => {
    for (const bad of [
      claimReq({ actor_id: "" }),
      claimReq({ session_id: "" }),
      claimReq({ max_batch: 0 }),
      claimReq({ max_batch: 17 }),
      claimReq({ max_batch: 1.5 }),
      claimReq({ ttl_seconds: 0 }),
      claimReq({ ttl_seconds: 3601 }),
      claimReq({ max_concurrent_claims: 0 }),
      claimReq({ max_concurrent_claims: 1.5 }),
    ]) {
      const res = await store.claim(bad);
      expect(res.ok).toBe(false);
      if (!res.ok) expect(res.error.code).toBe("invalid_claim_request");
    }
  });

  it("enforces max_concurrent_claims atomically on the outstanding-claim count", async () => {
    await pushTasks(redis, actorId, ["c1", "c2"]);
    // First claim occupies the single slot.
    const first = expectOk(await store.claim(claimReq({ max_batch: 1, max_concurrent_claims: 1 })));
    expect(first.claim?.tasks).toEqual(["c1"]);

    // Second claim, same actor, at the ceiling -> concurrency_limit_reached,
    // and no messages were popped (the inbox still holds c2).
    const second = await store.claim(claimReq({ max_batch: 1, max_concurrent_claims: 1 }));
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("expected concurrency_limit_reached");
    expect(second.error.code).toBe("concurrency_limit_reached");
    expect(second.error.message).toContain("1"); // names the limit
    expect(await inboxDepth(redis, actorId)).toBe(1);

    // Acknowledging the outstanding claim frees the slot -> a new claim succeeds.
    expectOk(
      await store.acknowledge({
        claim_id: first.claim!.claim_id,
        actor_id: actorId,
        session_id: "session-runtime",
      })
    );
    const third = expectOk(await store.claim(claimReq({ max_batch: 1, max_concurrent_claims: 1 })));
    expect(third.claim?.tasks).toEqual(["c2"]);
  });

  it("max_concurrent_claims 3 allows three outstanding claims and rejects the fourth", async () => {
    await pushTasks(redis, actorId, ["t1", "t2", "t3", "t4"]);
    for (let i = 0; i < 3; i += 1) {
      const ok = expectOk(await store.claim(claimReq({ max_batch: 1, max_concurrent_claims: 3 })));
      expect(ok.claim).not.toBeNull();
    }
    expect(await redis.zcard(CLAIM_KEYS.index(actorId))).toBe(3);

    const fourth = await store.claim(claimReq({ max_batch: 1, max_concurrent_claims: 3 }));
    expect(fourth.ok).toBe(false);
    if (fourth.ok) throw new Error("expected concurrency_limit_reached");
    expect(fourth.error.code).toBe("concurrency_limit_reached");
    expect(await inboxDepth(redis, actorId)).toBe(1); // t4 untouched
  });

  it("undefined max_concurrent_claims means unlimited for plain agents", async () => {
    await pushTasks(redis, actorId, ["u1", "u2", "u3", "u4"]);
    // No directory record path passes undefined (unlimited): two outstanding
    // claims from the same (or different) sessions are never rejected.
    const a = expectOk(await store.claim(claimReq({ max_batch: 1 })));
    const b = expectOk(await store.claim(claimReq({ max_batch: 1 })));
    expect(a.claim).not.toBeNull();
    expect(b.claim).not.toBeNull();
    expect(await redis.zcard(CLAIM_KEYS.index(actorId))).toBe(2);
  });

  it("recoverExpired re-queues tasks in order and cleans record + index", async () => {
    await pushTasks(redis, actorId, ["r1", "r2"]);
    const claimed = expectOk(await store.claim(claimReq({ ttl_seconds: 1 })));
    const claimId = claimed.claim!.claim_id;

    // Advance past expiry and recover.
    const rec = await store.recoverExpired({ actor_id: actorId, now: "2030-01-01T00:00:02.000Z" });
    expectOk(rec);
    expect(rec.recovered).toBe(2);
    expect(await inboxDepth(redis, actorId)).toBe(2);
    expect(await redis.hget(CLAIM_KEYS.claims, claimId)).toBeNull();
    expect(await redis.zscore(CLAIM_KEYS.index(actorId), claimId)).toBeNull();

    // The recovered tasks re-enter at the tail in order and can be re-claimed.
    const re = await store.claim(claimReq());
    const ok = expectOk(re);
    expect(ok.claim?.tasks).toEqual(["r1", "r2"]);
  });

  it("recoverExpired reports 0 when nothing is expired", async () => {
    await pushTasks(redis, actorId, ["n1"]);
    expectOk(await store.claim(claimReq({ ttl_seconds: 300 })));

    const rec = await store.recoverExpired({ actor_id: actorId, now: T0 });
    expectOk(rec);
    expect(rec.recovered).toBe(0);
    // The live, unexpired claim still exists.
    expect(await redis.zcard(CLAIM_KEYS.index(actorId))).toBe(1);
  });

  describe("renew (claim renewal)", () => {
    const renewReq = (
      overrides: Partial<Parameters<TaskClaimStore["renew"]>[0]> = {}
    ) => ({
      claim_id: "missing",
      actor_id: actorId,
      session_id: "session-runtime",
      ttl_seconds: 300,
      budget_seconds: 86400,
      now: T0,
      ...overrides,
    });

    it("owner renewal extends expiry and keeps the zset score aligned with expires_at", async () => {
      await pushTasks(redis, actorId, ["r1"]);
      const claimed = expectOk(await store.claim(claimReq())); // claimed_at T0, expires 00:05:00
      const claimId = claimed.claim!.claim_id;

      const res = await store.renew(
        renewReq({
          claim_id: claimId,
          now: "2030-01-01T00:01:00.000Z",
        })
      );
      const ok = expectOk(res);
      // T0+1min + 300s = 00:06:00 (capped by the generous budget).
      expect(ok.expires_at).toBe("2030-01-01T00:06:00.000Z");
      // The zset score must exactly equal the new expiry's epoch-ms.
      const score = await redis.zscore(CLAIM_KEYS.index(actorId), claimId);
      expect(score).toBe(String(Date.parse(ok.expires_at)));
      // The hash record reflects the new expiry.
      const stored = JSON.parse(
        (await redis.hget(CLAIM_KEYS.claims, claimId))!
      ) as { expires_at: string };
      expect(stored.expires_at).toBe(ok.expires_at);
    });

    it("a foreign session renewal is not_claim_owner naming the owning session", async () => {
      await pushTasks(redis, actorId, ["x1"]);
      const claimed = expectOk(await store.claim(claimReq()));
      const claimId = claimed.claim!.claim_id;

      const foreign = await store.renew(
        renewReq({ claim_id: claimId, session_id: "session-other" })
      );
      expect(foreign.ok).toBe(false);
      if (!foreign.ok) {
        expect(foreign.error.code).toBe("not_claim_owner");
        expect(foreign.error.message).toContain("session-runtime");
      }
      // A failed foreign renewal leaves the claim untouched.
      const stored = JSON.parse(
        (await redis.hget(CLAIM_KEYS.claims, claimId))!
      ) as { expires_at: string };
      expect(stored.expires_at).toBe("2030-01-01T00:05:00.000Z");
    });

    it("renewing an unknown claim is a typed unknown_claim", async () => {
      const res = await store.renew(renewReq());
      expect(res.ok).toBe(false);
      if (!res.ok) expect(res.error.code).toBe("unknown_claim");
    });

    it("renewing an already-expired claim is claim_expired", async () => {
      await pushTasks(redis, actorId, ["e1"]);
      const claimed = expectOk(await store.claim(claimReq({ ttl_seconds: 1 })));
      const claimId = claimed.claim!.claim_id;

      const res = await store.renew(
        renewReq({ claim_id: claimId, now: "2030-01-01T00:00:02.000Z" })
      );
      expect(res.ok).toBe(false);
      if (!res.ok) expect(res.error.code).toBe("claim_expired");
    });

    it("renewing past the lifetime budget is budget_exceeded", async () => {
      await pushTasks(redis, actorId, ["b1"]);
      const claimed = expectOk(await store.claim(claimReq({ ttl_seconds: 300 })));
      const claimId = claimed.claim!.claim_id;

      // claimed_at = T0, budget 30s -> cap at T0+30s; renew at T0+40s is past it.
      const res = await store.renew(
        renewReq({
          claim_id: claimId,
          budget_seconds: 30,
          now: "2030-01-01T00:00:40.000Z",
        })
      );
      expect(res.ok).toBe(false);
      if (!res.ok) expect(res.error.code).toBe("budget_exceeded");
      // The claim is untouched by a refused renewal.
      expect(await redis.hget(CLAIM_KEYS.claims, claimId)).not.toBeNull();
    });

    it("rejects an invalid renew request", async () => {
      for (const bad of [
        { claim_id: "", session_id: "session-runtime" },
        { claim_id: "c", actor_id: "", session_id: "session-runtime" },
        { claim_id: "c", actor_id: actorId, session_id: "" },
        { claim_id: "c", actor_id: actorId, session_id: "s", ttl_seconds: 0 },
        { claim_id: "c", actor_id: actorId, session_id: "s", ttl_seconds: 3601 },
        { claim_id: "c", actor_id: actorId, session_id: "s", ttl_seconds: 1.5 },
      ]) {
        const res = await store.renew(
          renewReq(bad as Partial<Parameters<TaskClaimStore["renew"]>[0]>)
        );
        expect(res.ok).toBe(false);
        if (!res.ok) expect(res.error.code).toBe("invalid_renew_request");
      }
    });

    it("a renewal ahead of recovery prevents re-delivery (no double-apply)", async () => {
      await pushTasks(redis, actorId, ["q1"]);
      const claimed = expectOk(await store.claim(claimReq({ ttl_seconds: 300 }))); // expires 00:05:00
      const claimId = claimed.claim!.claim_id;

      const renewed = expectOk(
        await store.renew(
          renewReq({ claim_id: claimId, now: "2030-01-01T00:01:00.000Z" })
        )
      );
      expect(renewed.expires_at).toBe("2030-01-01T00:06:00.000Z");

      // Recovery at the ORIGINAL expiry finds nothing expired: the renewal moved
      // the zset score forward, so the claim is not re-delivered (no double-apply).
      const rec = expectOk(
        await store.recoverExpired({
          actor_id: actorId,
          now: "2030-01-01T00:05:30.000Z",
        })
      );
      expect(rec.recovered).toBe(0);
      expect(await inboxDepth(redis, actorId)).toBe(0);
      expect(await redis.hget(CLAIM_KEYS.claims, claimId)).not.toBeNull();
      expect(
        await redis.zscore(CLAIM_KEYS.index(actorId), claimId)
      ).toBe(String(Date.parse(renewed.expires_at)));
    });

    it("a renewed claim can still be acknowledged", async () => {
      await pushTasks(redis, actorId, ["a1", "a2"]);
      const claimed = expectOk(await store.claim(claimReq()));
      const claimId = claimed.claim!.claim_id;

      expectOk(
        await store.renew(
          renewReq({ claim_id: claimId, now: "2030-01-01T00:01:00.000Z" })
        )
      );
      const ack = expectOk(
        await store.acknowledge({
          claim_id: claimId,
          actor_id: actorId,
          session_id: "session-runtime",
        })
      );
      expect(ack.acknowledged).toBe(2);
      expect(await redis.hget(CLAIM_KEYS.claims, claimId)).toBeNull();
      expect(await redis.zscore(CLAIM_KEYS.index(actorId), claimId)).toBeNull();
    });

    it("e2e: claim short ttl, renew out of original expiry, recover only after the budget cap expires", async () => {
      await pushTasks(redis, actorId, ["w1"]);
      // Short-ttl claim: expires at T0+30s.
      const claimed = expectOk(await store.claim(claimReq({ ttl_seconds: 30 })));
      const claimId = claimed.claim!.claim_id;
      expect(claimed.claim!.expires_at).toBe("2030-01-01T00:00:30.000Z");

      // Renew at T0+10s with a 50s budget: capped at claimed_at+50s = T0+50s.
      const renewed = expectOk(
        await store.renew(
          renewReq({
            claim_id: claimId,
            ttl_seconds: 300,
            budget_seconds: 50,
            now: "2030-01-01T00:00:10.000Z",
          })
        )
      );
      expect(renewed.expires_at).toBe("2030-01-01T00:00:50.000Z");

      // Past the ORIGINAL expiry (T0+30s) but before the renewed expiry: NOT recovered.
      const early = expectOk(
        await store.recoverExpired({
          actor_id: actorId,
          now: "2030-01-01T00:00:40.000Z",
        })
      );
      expect(early.recovered).toBe(0);
      expect(await inboxDepth(redis, actorId)).toBe(0);

      // Past the budget-capped renewed expiry (T0+50s): the claim expires and its
      // task is recovered back to the inbox.
      const late = expectOk(
        await store.recoverExpired({
          actor_id: actorId,
          now: "2030-01-01T00:00:51.000Z",
        })
      );
      expect(late.recovered).toBe(1);
      expect(await inboxDepth(redis, actorId)).toBe(1);
      expect(await redis.hget(CLAIM_KEYS.claims, claimId)).toBeNull();
    });
  });
});

describe("task claim tools + workload derivation", () => {
  let redis: Redis;
  let client: RedisClient;

  const actorId = "workload-act";

  const launchOf = (): RuntimeLaunchContract => ({
    command: process.execPath,
    args: ["-e", "process.exit(0)"],
  });

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    client = new RedisClient(null, TEST_REDIS_URL);
    const reg = await client.register("publisher", "workload-sender", "sends");
    // Durable actor record for the target.
    const result = await client.actorDirectory.register({
      profile_input: {
        actor_id: actorId,
        alias: actorId,
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        state_directory: `/state/${actorId}`,
        runtime: "node",
        activation_policy: { mode: "wake_if_offline" },
        max_concurrency: 1,
      },
      launch: launchOf(),
      registered_by: reg.session_id,
      registered_at: T0,
    });
    expect(result.ok).toBe(true);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await client.shutdown();
  });

  it("workload derivation: an unacked claim classifies the session active, then idle after ack", async () => {
    // The runtime registers under the durable actor id -> live session S.
    const runtime = new RedisClient(null, TEST_REDIS_URL);
    try {
      const reg = await registerAgent(
        runtime,
        registerAgentSchema.parse({
          name: actorId,
          role: "both",
          description: "woken runtime session",
        })
      );
      await pushTasks(redis, actorId, ["w1", "w2"]);

      // Before any claim: live session with no claim -> idle.
      const before = await actorStatus(client, actorStatusSchema.parse({ actor_id: actorId }));
      expect(before.structuredContent).toMatchObject({ presence: "idle" });

      // Runtime claims a batch -> session holds an unacked claim -> active.
      const claim = await claimTasks(
        runtime,
        claimTasksSchema.parse({
          session_id: reg.session_id,
          max_batch: 2,
          ttl_seconds: 300,
        })
      );
      expect(claim.isError).toBeUndefined();
      const claimPayload = claim.structuredContent as {
        status: string;
        claimed: boolean;
        claim: { claim_id: string; tasks: string[]; expires_at: string };
      };
      expect(claimPayload.status).toBe("ok");
      expect(claimPayload.claimed).toBe(true);
      expect(claimPayload.claim.tasks).toEqual(["w1", "w2"]);
      expect(claimPayload.claim.expires_at).toBeTruthy();

      const during = await actorStatus(client, actorStatusSchema.parse({ actor_id: actorId }));
      // processing -> active
      expect(during.structuredContent).toMatchObject({ presence: "active" });
      expect(
        (during.structuredContent as { runtime: { workload: string } }).runtime.workload
      ).toBe("processing");

      // Acknowledge -> claim removed -> back to idle.
      const ack = await acknowledgeTasks(
        runtime,
        acknowledgeTasksSchema.parse({ session_id: reg.session_id, claim_id: claimPayload.claim.claim_id })
      );
      expect(ack.structuredContent).toMatchObject({ status: "ok", acknowledged: 2 });

      const after = await actorStatus(client, actorStatusSchema.parse({ actor_id: actorId }));
      expect(after.structuredContent).toMatchObject({ presence: "idle" });
    } finally {
      await runtime.shutdown();
    }
  });

  it("claim_tasks on an empty inbox yields an explicit empty-batch result; ack of an unknown claim is an error", async () => {
    const runtime = new RedisClient(null, TEST_REDIS_URL);
    try {
      const reg = await registerAgent(
        runtime,
        registerAgentSchema.parse({ name: actorId, role: "both", description: "runtime" })
      );

      const empty = await claimTasks(
        runtime,
        claimTasksSchema.parse({ session_id: reg.session_id, max_batch: 1, ttl_seconds: 300 })
      );
      expect(empty.structuredContent).toMatchObject({ status: "ok", claimed: false });
      expect((empty.structuredContent as { claim: unknown }).claim).toBeNull();

      // Acknowledging a claim that does not exist is a structured error.
      const badAck = await acknowledgeTasks(
        runtime,
        acknowledgeTasksSchema.parse({ session_id: reg.session_id, claim_id: "does-not-exist" })
      );
      expect(badAck.isError).toBe(true);
      expect(badAck.structuredContent).toMatchObject({
        status: "error",
        error: { code: "unknown_claim" },
      });
    } finally {
      await runtime.shutdown();
    }
  });
});

describe("claim_tasks concurrency enforcement (directory max_concurrency)", () => {
  let redis: Redis;
  let owner: RedisClient;
  const actorId = "conc-act";

  const registerConcActor = async (max_concurrency: number) => {
    const reg = await owner.register("publisher", "conc-owner", "registers actor");
    const result = await owner.actorDirectory.register({
      profile_input: {
        actor_id: actorId,
        alias: actorId,
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        state_directory: `/state/${actorId}`,
        runtime: "node",
        activation_policy: { mode: "store_only" },
        max_concurrency,
      },
      launch: null,
      registered_by: reg.session_id,
      registered_at: T0,
    });
    expect(result.ok).toBe(true);
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    owner = new RedisClient(null, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await owner.shutdown();
    await redis.quit();
  });

  it("max_concurrency 1: a second session's claim is refused until the first acks", async () => {
    await registerConcActor(1);
    const r1 = new RedisClient(null, TEST_REDIS_URL);
    const r2 = new RedisClient(null, TEST_REDIS_URL);
    try {
      const reg1 = await registerAgent(r1, registerAgentSchema.parse({ name: actorId, role: "both", description: "runtime one" }));
      const reg2 = await registerAgent(r2, registerAgentSchema.parse({ name: actorId, role: "both", description: "runtime two" }));
      const reg1Session = (reg1.structuredContent as { session_id: string }).session_id;
      const reg2Session = (reg2.structuredContent as { session_id: string }).session_id;
      expect(reg1Session).toBeTruthy();
      expect(reg2Session).toBeTruthy();
      expect(reg1Session).not.toBe(reg2Session);

      await pushTasks(redis, actorId, ["x1", "x2", "x3", "x4"]);
      const first = await claimTasks(r1, claimTasksSchema.parse({ session_id: reg1Session, max_batch: 2, ttl_seconds: 300 }));
      expect(first.structuredContent).toMatchObject({ status: "ok", claimed: true });

      // Second session, same actor -> hits the admitted max_concurrency ceiling.
      const second = await claimTasks(r2, claimTasksSchema.parse({ session_id: reg2Session, max_batch: 1, ttl_seconds: 300 }));
      expect(second.isError).toBe(true);
      expect(second.structuredContent).toMatchObject({
        status: "error",
        error: { code: "concurrency_limit_reached" },
      });

      // Ack the first claim -> slot frees -> the second session now claims.
      const claimId = (first.structuredContent as { claim: { claim_id: string } }).claim.claim_id;
      const ack = await acknowledgeTasks(r1, acknowledgeTasksSchema.parse({ session_id: reg1Session, claim_id: claimId }));
      expect(ack.structuredContent).toMatchObject({ status: "ok" });

      const third = await claimTasks(r2, claimTasksSchema.parse({ session_id: reg2Session, max_batch: 1, ttl_seconds: 300 }));
      expect(third.structuredContent).toMatchObject({ status: "ok", claimed: true });
    } finally {
      await r1.shutdown();
      await r2.shutdown();
    }
  });

  it("plain agent (no directory record) claims without any ceiling", async () => {
    const plain = new RedisClient(null, TEST_REDIS_URL);
    try {
      const reg = await registerAgent(
        plain,
        registerAgentSchema.parse({ name: "plain-claim", role: "both", description: "no record" })
      );
      await pushTasks(redis, "plain-claim", ["p1", "p2", "p3"]);
      const a = await claimTasks(plain, claimTasksSchema.parse({ session_id: reg.session_id, max_batch: 1, ttl_seconds: 300 }));
      const b = await claimTasks(plain, claimTasksSchema.parse({ session_id: reg.session_id, max_batch: 1, ttl_seconds: 300 }));
      expect(a.structuredContent).toMatchObject({ status: "ok", claimed: true });
      expect(b.structuredContent).toMatchObject({ status: "ok", claimed: true });
    } finally {
      await plain.shutdown();
    }
  });
});

describe("attachSpawn (wake lease spawn evidence)", () => {
  let redis: Redis;
  let client: RedisClient;
  const actorId = "spawn-act";

  const launchOf = (): RuntimeLaunchContract => ({
    command: process.execPath,
    args: ["-e", "process.exit(0)"],
  });

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    client = new RedisClient(null, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await client.shutdown();
  });

  it("attachSpawn attaches a pid to a matching lease; mismatch -> attached:false", async () => {
    const acquired = await client.wakeLease.acquire({
      actor_id: actorId,
      issued_by_session: "session-waker",
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    const leaseId = acquired.lease.lease_id;

    const attached = await client.wakeLease.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid: 4242,
      spawned_at: T0,
    });
    expect(attached.ok).toBe(true);
    expect(attached.attached).toBe(true);
    const lease = await client.wakeLease.get(actorId);
    expect(lease?.spawned_pid).toBe(4242);
    expect(lease?.spawned_at).toBe(T0);

    // A mismatched lease id leaves the lease untouched (idempotent, not error).
    const mismatch = await client.wakeLease.attachSpawn({
      actor_id: actorId,
      lease_id: "some-other-lease",
      pid: 9999,
      spawned_at: T0,
    });
    expect(mismatch.attached).toBe(false);
    expect((await client.wakeLease.get(actorId))?.spawned_pid).toBe(4242);
  });

  it("actor_status surfaces a live spawned pid and pid_liveness in the wake_lease payload", async () => {
    const reg = await client.register("publisher", "spawn-sender", "registers actor");
    await client.actorDirectory.register({
      profile_input: {
        actor_id: actorId,
        alias: actorId,
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        state_directory: `/state/${actorId}`,
        runtime: "node",
        activation_policy: { mode: "wake_if_offline" },
        max_concurrency: 1,
      },
      launch: launchOf(),
      registered_by: reg.session_id,
      registered_at: T0,
    });

    // A live child so the reconciliation probe (`isPidAlive`) retains the lease
    // and classifies `starting` rather than clearing it as a dead activation.
    const sleeper = spawn(process.execPath, ["-e", "setTimeout(() => {}, 15000)"]);
    const livePid = sleeper.pid!;

    const acquired = await client.wakeLease.acquire({
      actor_id: actorId,
      issued_by_session: reg.session_id,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    await client.wakeLease.attachSpawn({
      actor_id: actorId,
      lease_id: acquired.lease.lease_id,
      pid: livePid,
      spawned_at: T0,
    });

    try {
      const res = await actorStatus(client, actorStatusSchema.parse({ actor_id: actorId }));
      expect(res.structuredContent).toMatchObject({ presence: "starting" });
      expect(res.structuredContent).toMatchObject({
        wake_lease: {
          lease_id: acquired.lease.lease_id,
          spawned_pid: livePid,
          spawned_at: T0,
          pid_liveness: "alive",
        },
      });
    } finally {
      try {
        process.kill(livePid);
      } catch {
        // already gone
      }
    }
  });

  it("actor_status clears a dead-pid wake lease so the actor classifies offline", async () => {
    const reg = await client.register("publisher", "spawn-sender-2", "registers actor");
    await client.actorDirectory.register({
      profile_input: {
        actor_id: actorId,
        alias: actorId,
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        state_directory: `/state/${actorId}`,
        runtime: "node",
        activation_policy: { mode: "wake_if_offline" },
        max_concurrency: 1,
      },
      launch: launchOf(),
      registered_by: reg.session_id,
      registered_at: T0,
    });

    // A process that exits immediately, leaving a dead pid behind.
    const dead = spawn(process.execPath, ["-e", "process.exit(0)"]);
    const deadPid = dead.pid!;
    await new Promise<void>((resolve) => dead.once("exit", () => resolve()));

    const acquired = await client.wakeLease.acquire({
      actor_id: actorId,
      issued_by_session: reg.session_id,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    await client.wakeLease.attachSpawn({
      actor_id: actorId,
      lease_id: acquired.lease.lease_id,
      pid: deadPid,
      spawned_at: T0,
    });

    // Reconciliation observes the dead pid -> clears the lease -> offline.
    const res = await actorStatus(client, actorStatusSchema.parse({ actor_id: actorId }));
    expect(res.structuredContent).toMatchObject({ presence: "offline_launchable" });
    expect((res.structuredContent as { wake_lease: unknown }).wake_lease).toBeNull();
    expect(await client.wakeLease.get(actorId)).toBeNull();
  });
});

describe("dead-letter queue (tranche 2)", () => {
  let redis: Redis;
  let store: TaskClaimStore;
  const actorId = "dlq-actor";

  // The recovery Lua constants (DLQ_PROVISIONAL) are frozen imports, so tests
  // that need a smaller cap / bound invoke the raw script with a small ARGV.
  const dlqLua = readFileSync(
    new URL("../src/mcp-server/lua/claims-recover.lua", import.meta.url),
    "utf-8"
  );

  // Directly eval the recovery script with an arbitrary cap / DLQ bound.
  const evalRecovery = async (
    nowISO: string,
    cap: number,
    maxLength: number,
    counterTtl = 604800
  ): Promise<[number, number]> =>
    (await redis.eval(
      dlqLua,
      4,
      CLAIM_KEYS.index(actorId),
      CLAIM_KEYS.claims,
      SESSION_KEYS.queue(actorId),
      DLQ_KEYS.list(actorId),
      Date.parse(nowISO),
      cap,
      maxLength,
      counterTtl
    )) as [number, number];

  const msgTask = (id: string) =>
    JSON.stringify({
      id,
      from: "sender",
      to: actorId,
      timestamp: "2030-01-01T00:00:00.000Z",
      type: "task",
      payload: { content: id },
    });

  const claimDLQ = (
    overrides: Partial<Parameters<TaskClaimStore["claim"]>[0]> = {}
  ) => ({
    actor_id: actorId,
    session_id: "session-runtime",
    max_batch: 1,
    ttl_seconds: 1,
    now: T0,
    ...overrides,
  });

  // Push -> claim -> recover once with cap 0 (any recovery exceeds the cap), so
  // a single pass dead-letters a message for the DLQ-bound tests.
  const deadletterOnce = async (
    id: string,
    nowISO: string,
    maxLength: number
  ): Promise<[number, number]> => {
    await pushTasks(redis, actorId, [msgTask(id)]);
    expectOk(await store.claim(claimDLQ()));
    return evalRecovery(nowISO, 0, maxLength);
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    store = new TaskClaimStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await redis.quit();
  });

  it("recovery increments a per-message counter while the task stays on the inbox under the cap", async () => {
    await pushTasks(redis, actorId, [msgTask("m1")]);
    expectOk(await store.claim(claimDLQ()));

    const [recovered, deadlettered] = await evalRecovery(
      "2030-01-01T00:00:02.000Z",
      5,
      1000
    );
    expect(recovered).toBe(1);
    expect(deadlettered).toBe(0);
    // Counter incremented to 1; task back on the inbox, not dead-lettered.
    expect(await redis.get(CLAIM_KEYS.recoverCount(actorId, "m1"))).toBe("1");
    expect(await inboxDepth(redis, actorId)).toBe(1);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(0);
  });

  it("cap exceeded lands the task in the DLQ (not the inbox) and deletes the counter", async () => {
    await pushTasks(redis, actorId, [msgTask("m1")]);
    expectOk(await store.claim(claimDLQ()));

    // First recovery: counter -> 1, under cap=1, re-queued to the inbox.
    let [r1, d1] = await evalRecovery("2030-01-01T00:00:02.000Z", 1, 1000);
    expect(r1).toBe(1);
    expect(d1).toBe(0);

    // Re-claim the recovered task, then recover again -> counter -> 2 > cap -> DLQ.
    expectOk(await store.claim(claimDLQ()));
    const [r2, d2] = await evalRecovery("2030-01-01T00:00:04.000Z", 1, 1000);
    expect(r2).toBe(0);
    expect(d2).toBe(1);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(1);
    expect(await inboxDepth(redis, actorId)).toBe(0);
    expect(await redis.get(CLAIM_KEYS.recoverCount(actorId, "m1"))).toBeNull();
  });

  it("deadLetterEntries lists DLQ entries with decoded ids and timestamps; requeue returns a fresh budget", async () => {
    await deadletterOnce("m1", "2030-01-01T00:00:02.000Z", 1000);

    const listed = expectOk(
      await store.deadLetterEntries({ actor_id: actorId })
    );
    expect(listed.entries.length).toBe(1);
    expect(listed.entries[0].message_id).toBe("m1");
    expect(listed.entries[0].deadlettered_at).toBe("2030-01-01T00:00:00.000Z");
    expect(JSON.parse(listed.entries[0].payload).id).toBe("m1");

    // Requeue moves the raw payload back to the inbox tail and deletes the counter.
    const requeued = expectOk(
      await store.requeue({ actor_id: actorId, message_id: "m1" })
    );
    expect(requeued.requeued).toBe(1);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(0);
    expect(await inboxDepth(redis, actorId)).toBe(1);
    expect(
      await redis.get(CLAIM_KEYS.recoverCount(actorId, "m1"))
    ).toBeNull();

    // Fresh budget: reclaim and recover once (cap=1) stays on the inbox.
    expectOk(
      await store.claim(claimDLQ({ now: "2030-01-01T00:00:05.000Z" }))
    );
    const [r, d] = await evalRecovery("2030-01-01T00:00:07.000Z", 1, 1000);
    expect(r).toBe(1);
    expect(d).toBe(0);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(0);
  });

  it("requeue of a missing message is a typed dlq_entry_not_found", async () => {
    const missing = await store.requeue({
      actor_id: actorId,
      message_id: "does-not-exist",
    });
    expect(missing.ok).toBe(false);
    if (!missing.ok) expect(missing.error.code).toBe("dlq_entry_not_found");
  });

  it("ack clears the per-message recovery counters it owns", async () => {
    await pushTasks(redis, actorId, [msgTask("a1"), msgTask("a2")]);
    const claimed = expectOk(await store.claim(claimDLQ({ max_batch: 2 })));
    // Seed counters as if prior recoveries had accumulated.
    await redis.set(CLAIM_KEYS.recoverCount(actorId, "a1"), "2");
    await redis.set(CLAIM_KEYS.recoverCount(actorId, "a2"), "1");

    const ack = expectOk(
      await store.acknowledge({
        claim_id: claimed.claim!.claim_id,
        actor_id: actorId,
        session_id: "session-runtime",
      })
    );
    expect(ack.acknowledged).toBe(2);
    expect(
      await redis.get(CLAIM_KEYS.recoverCount(actorId, "a1"))
    ).toBeNull();
    expect(
      await redis.get(CLAIM_KEYS.recoverCount(actorId, "a2"))
    ).toBeNull();
  });

  it("DLQ LTRIM bound is enforced, keeping the newest entries", async () => {
    await deadletterOnce("d1", "2030-01-01T00:00:02.000Z", 2);
    await deadletterOnce("d2", "2030-01-01T00:00:04.000Z", 2);
    await deadletterOnce("d3", "2030-01-01T00:00:06.000Z", 2);

    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(2);
    const kept = await redis.lrange(DLQ_KEYS.list(actorId), 0, -1);
    // LPUSH keeps newest at the head: [d3, d2]; d1 was trimmed (dropped).
    expect(JSON.parse(kept[0]).id).toBe("d3");
    expect(JSON.parse(kept[1]).id).toBe("d2");
  });

  it("concurrent recovery never double-counts a shared message id", async () => {
    // Two separate expired claims, each holding a task with the SAME message id.
    await pushTasks(redis, actorId, [msgTask("m1")]);
    expectOk(await store.claim(claimDLQ()));
    await pushTasks(redis, actorId, [msgTask("m1")]);
    expectOk(await store.claim(claimDLQ()));

    const [a, b] = await Promise.all([
      evalRecovery("2030-01-01T00:00:02.000Z", 5, 1000),
      evalRecovery("2030-01-01T00:00:02.000Z", 5, 1000),
    ]);
    // Exactly two tasks recovered total; the counter reflects two increments,
    // so EVAL's atomic INCR never lost an update under concurrency.
    expect(a[0] + b[0]).toBe(2);
    expect(a[1] + b[1]).toBe(0);
    expect(await redis.get(CLAIM_KEYS.recoverCount(actorId, "m1"))).toBe("2");
  });

  it("envelope decode survives a legacy payload without a message id", async () => {
    // Legacy/undecodable envelope: valid JSON but no stable id.
    const legacy = JSON.stringify({
      from: "sender",
      to: actorId,
      type: "task",
      payload: { content: "legacy" },
    });
    await pushTasks(redis, actorId, [legacy]);
    expectOk(await store.claim(claimDLQ()));

    // Recovery re-queues the legacy task (counter skipped), never dead-letters it.
    const [r1, d1] = await evalRecovery("2030-01-01T00:00:02.000Z", 0, 1000);
    expect(r1).toBe(1);
    expect(d1).toBe(0);
    expect(await inboxDepth(redis, actorId)).toBe(1);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(0);

    // Ack of a claim containing a legacy task skips counter cleanup, no error.
    const c2 = expectOk(await store.claim(claimDLQ()));
    const ack = expectOk(
      await store.acknowledge({
        claim_id: c2.claim!.claim_id,
        actor_id: actorId,
        session_id: "session-runtime",
      })
    );
    expect(ack.acknowledged).toBe(1);

    // Id-bearing messages STILL dead-letter on the next cap after the legacy rode along.
    await pushTasks(redis, actorId, [msgTask("m1")]);
    expectOk(await store.claim(claimDLQ()));
    const [r2, d2] = await evalRecovery("2030-01-01T00:00:04.000Z", 0, 1000);
    expect(r2).toBe(0);
    expect(d2).toBe(1);
    expect(await redis.llen(DLQ_KEYS.list(actorId))).toBe(1);
  });
});

