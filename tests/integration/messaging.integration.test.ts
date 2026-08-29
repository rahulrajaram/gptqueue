import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
  vi,
} from "vitest";
import { readFileSync } from "node:fs";
import type { Redis } from "ioredis";
import {
  setupIntegrationServer,
  type IntegrationServer,
} from "../helpers/integration-server.js";
import { connectAgent, type Agent } from "../helpers/mcp-agent.js";
import { TaskClaimStore } from "../../src/core/task-claim-store.js";
import {
  CLAIM_KEYS,
  SESSION_KEYS,
  DLQ_KEYS,
} from "../../src/core/keys.js";

// Per-file timeout override (does not touch the global vitest config).
// Several integration tests intentionally wait out 1s leases / 5-cycler
// recovery loops, so the default 15s is much too tight for them.
vi.setConfig({ testTimeout: 150_000, hookTimeout: 120_000 });

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Short unique suffix for per-test agent/worktree names (no cross-talk). */
const uniq = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`;

/** A message id from a claimed task envelope. */
const taskIdOf = (raw: string): string => (JSON.parse(raw) as { id: string }).id;

interface Ctx {
  server: IntegrationServer;
  redis: Redis;
  agents: Agent[];
}

let ctx: Ctx;

beforeAll(async () => {
  const server = await setupIntegrationServer();
  ctx = { server, redis: server.redis, agents: [] };
}, 120_000);

afterAll(async () => {
  for (const a of ctx.agents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  await ctx.server.cleanup();
}, 60_000);

beforeEach(async () => {
  ctx.agents = [];
  await ctx.server.flushGptqKeys();
}, 60_000);

afterEach(async () => {
  for (const a of ctx.agents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  await ctx.server.flushGptqKeys();
}, 60_000);

/** Connect a fresh agent on the integration server and track it for cleanup. */
async function connect(
  name: string,
  opts?: { role?: "publisher" | "consumer" | "both"; description?: string }
) {
  const agent = await connectAgent(ctx.server.baseUrl, name, opts);
  ctx.agents.push(agent);
  return agent;
}

/** Assert the wire claimed a given number of tasks and return the claim. */
function expectClaimed(res: { data: Record<string, any> }) {
  expect(res.data.status).toBe("ok");
  expect(res.data.claimed).toBe(true);
  return res.data.claim as { claim_id: string; tasks: string[]; expires_at: string };
}

// ---------------------------------------------------------------------------
// A. Registration & identity
// ---------------------------------------------------------------------------
describe("registration & identity wire semantics", () => {
  it("register returns a session_id; duplicate name from a second connection gets a distinct session; list_agents shows both", async () => {
    const a = await connect(uniq("reg-a"));
    const b = await connect(uniq("reg-b"));

    const list = await a.call("list_agents", {});
    expect(Array.isArray(list.data)).toBe(true);
    const names = (list.data as Array<{ name: string }>).map((x) => x.name);
    expect(names).toContain(a.name);
    expect(names).toContain(b.name);

    // Same name from a second connection -> a DISTINCT session, same registry name.
    const dup = await connect(a.name);
    expect(dup.sessionId).toBeTruthy();
    expect(dup.sessionId).not.toBe(a.sessionId);
    const list2 = await a.call("list_agents", {});
    const names2 = (list2.data as Array<{ name: string }>).map((x) => x.name);
    expect(names2.filter((n) => n === a.name).length).toBe(1);
  });

  it("rename on the same connection closes the previous session (old session_id stops working)", async () => {
    const conn = await connect(uniq("rename-a"));
    const oldSession = conn.sessionId;

    // Same connection, new name -> the old session is closed and a new one issued.
    const reg2 = await conn.call("register_agent", {
      name: uniq("rename-b"),
      role: "both",
      description: "renamed",
    });
    expect(reg2.data.status).toBe("registered");
    expect(reg2.data.session_id).toBeTruthy();
    expect(reg2.data.session_id).not.toBe(oldSession);

    // The old session_id is gone: a call bound to it fails with SESSION_UNAVAILABLE.
    const stale = await conn.call("send_message", {
      session_id: oldSession,
      to: conn.name,
      content: "should-not-deliver",
    });
    expect(stale.isError).toBe(true);
    expect(stale.data.error?.code).toBe("SESSION_UNAVAILABLE");
  });

  it("close_session preserves the mailbox; unregister_agent deletes it", async () => {
    const sender = await connect(uniq("close-src"));
    const closeTgt = await connect(uniq("close-tgt"));
    const unregTgt = await connect(uniq("unreg-tgt"));

    // Seed one message into each target.
    await sender.call("send_message", {
      session_id: sender.sessionId,
      to: closeTgt.name,
      content: "preserve me",
    });
    await sender.call("send_message", {
      session_id: sender.sessionId,
      to: unregTgt.name,
      content: "delete me",
    });

    // close_session: mailbox + registry survive; the session is gone.
    const closed = await closeTgt.call("close_session", { session_id: closeTgt.sessionId });
    expect(closed.data.status).toBe("session_closed");
    expect(closed.data.mailbox_preserved).toBe(true);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(closeTgt.name))).toBe(1);
    const listAfterClose = (await sender.call("list_agents", {})).data as Array<{ name: string }>;
    expect(listAfterClose.some((x) => x.name === closeTgt.name)).toBe(true);

    // unregister_agent: mailbox + registry deleted. The wire result body is a
    // legacy plain string (not a JSON envelope), so assert via content + Redis
    // state rather than a structured `status` field.
    const unreg = await unregTgt.call("unregister_agent", { session_id: unregTgt.sessionId });
    expect(unreg.contentText).toMatch(/unregistered/);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(unregTgt.name))).toBe(0);
    const listAfterUnreg = (await sender.call("list_agents", {})).data as Array<{ name: string }>;
    expect(listAfterUnreg.some((x) => x.name === unregTgt.name)).toBe(false);
  });

  it("a call with an unknown/expired session_id fails with SESSION_UNAVAILABLE", async () => {
    const conn = await connect(uniq("unknown-sess"));
    const res = await conn.call("send_message", {
      session_id: "definitely-not-a-real-session",
      to: conn.name,
      content: "nope",
    });
    expect(res.isError).toBe(true);
    expect(res.data.status).toBe("error");
    expect(res.data.error?.code).toBe("SESSION_UNAVAILABLE");
  });
});

// ---------------------------------------------------------------------------
// B. Send/receive legacy path (at-most-once)
// ---------------------------------------------------------------------------
describe("legacy send/receive delivery plane", () => {
  it("a sends to b; b receive_message pops it at-most-once; empty receive returns no_messages", async () => {
    const s = await connect(uniq("legacy-src"));
    const r = await connect(uniq("legacy-dst"));

    const sent = await s.call("send_message", {
      session_id: s.sessionId,
      to: r.name,
      content: "hello legacy",
      type: "task",
    });
    expect(sent.data.status).toBe("sent");
    expect(sent.data.message_id).toBeTruthy();

    const recv = await r.call("receive_message", { session_id: r.sessionId, timeout: 3 });
    // receive_message's wire body is the raw message envelope, not a status wrap.
    expect(recv.data.payload.content).toBe("hello legacy");
    expect(recv.data.from).toBe(s.name);

    // Already popped (at-most-once): a short-polling receive sees nothing left.
    const again = await r.call("receive_message", { session_id: r.sessionId, timeout: 1 });
    expect(again.data.status).toBe("no_messages");
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(0);
  });

  it("receive with timeout 0 on an empty inbox blocks and returns as soon as a message arrives", async () => {
    const s = await connect(uniq("to0-src"));
    const r = await connect(uniq("to0-dst"));
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(0);

    const pending = r.call("receive_message", { session_id: r.sessionId, timeout: 0 });
    await delay(300); // ensure the BLPOP is now blocking on the empty inbox
    const sent = await s.call("send_message", {
      session_id: s.sessionId,
      to: r.name,
      content: "wake via timeout-0",
    });
    expect(sent.data.status).toBe("sent");
    const recv = await pending;
    expect(recv.data.payload.content).toBe("wake via timeout-0");
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(0);
  });

  it("bounded inbox: fills to the 10-entry bound then refuses the overflow with QUEUE_FULL", async () => {
    const s = await connect(uniq("bounded-src"));
    const r = await connect(uniq("bounded-dst"));
    const BOUND = 10;

    for (let i = 0; i < BOUND; i += 1) {
      const sent = await s.call("send_message", {
        session_id: s.sessionId,
        to: r.name,
        content: `msg-${i}`,
      });
      expect(sent.data.status).toBe("sent");
    }
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(BOUND);

    // The (BOUND+1)th push cannot fit -> structured QUEUE_FULL error, inbox unchanged.
    const overflow = await s.call("send_message", {
      session_id: s.sessionId,
      to: r.name,
      content: "one-too-many",
    });
    expect(overflow.data.status).toBe("error");
    expect(overflow.data.error?.code).toBe("QUEUE_FULL");
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(BOUND);
  });
});

// ---------------------------------------------------------------------------
// C. Claim/ack at-least-once
// ---------------------------------------------------------------------------
describe("claim/ack at-least-once delivery", () => {
  it("claim max_batch 2/1/empty then ack each claim -> inbox drained", async () => {
    const s = await connect(uniq("claim-src"));
    const b = await connect(uniq("claim-dst"));

    for (let i = 0; i < 3; i += 1) {
      await s.call("send_message", { session_id: s.sessionId, to: b.name, content: `c${i}` });
    }
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(3);

    const claim1 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 2, ttl_seconds: 300 })
    );
    expect(claim1.tasks).toHaveLength(2);

    const claim2 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 2, ttl_seconds: 300 })
    );
    expect(claim2.tasks).toHaveLength(1);

    const empty = await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 2, ttl_seconds: 300 });
    expect(empty.data.claimed).toBe(false);
    expect(empty.data.claim).toBeNull();

    // Acknowledge both claims.
    const ack1 = await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: claim1.claim_id });
    expect(ack1.data).toMatchObject({ status: "ok", acknowledged: 2 });
    const ack2 = await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: claim2.claim_id });
    expect(ack2.data).toMatchObject({ status: "ok", acknowledged: 1 });

    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
    const after = await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 2, ttl_seconds: 300 });
    expect(after.data.claimed).toBe(false);
  });

  it("partial-batch: ack only the first claim; the other messages stay pending for the next claim", async () => {
    const s = await connect(uniq("partial-src"));
    const b = await connect(uniq("partial-dst"));
    for (let i = 0; i < 3; i += 1) {
      await s.call("send_message", { session_id: s.sessionId, to: b.name, content: `p${i}` });
    }

    // Claim 1 of the 3, ack it -> the other 2 remain in the inbox.
    const one = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(one.tasks).toHaveLength(1);
    await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: one.claim_id });
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(2);

    // The next claim pulls in the two that stayed pending.
    const two = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 2, ttl_seconds: 300 })
    );
    expect(two.tasks).toHaveLength(2);
    await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: two.claim_id });
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// E. Renewal
// ---------------------------------------------------------------------------
describe("claim renewal", () => {
  it("renew extends expiry past the original ttl; renewed claim is not recovered; ack still works", async () => {
    const s = await connect(uniq("renew-src"));
    const b = await connect(uniq("renew-dst"));
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "renew me" });

    const claim = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 3 })
    );
    const originalExpiry = Date.parse(claim.expires_at);

    const renew1 = await b.call("renew_claim", {
      session_id: b.sessionId,
      claim_id: claim.claim_id,
      ttl_seconds: 300,
    });
    expect(renew1.data.status).toBe("ok");
    const renewedExpiry = Date.parse(renew1.data.expires_at as string);
    expect(renewedExpiry).toBeGreaterThan(originalExpiry);

    // A second renewal before expiry is also ok (still inside lifetime budget).
    const renew2 = await b.call("renew_claim", {
      session_id: b.sessionId,
      claim_id: claim.claim_id,
      ttl_seconds: 300,
    });
    expect(renew2.data.status).toBe("ok");

    // Wait PAST the original 3s ttl but far before the renewed expiry: the
    // renewed claim must NOT be recovered/re-delivered.
    await delay(3200);
    const probe = await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 });
    expect(probe.data.claimed).toBe(false);
    expect(
      await ctx.redis.zscore(CLAIM_KEYS.index(b.name), claim.claim_id)
    ).not.toBeNull();

    // The renewed claim can still be acknowledged cleanly.
    const ack = await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: claim.claim_id });
    expect(ack.data).toMatchObject({ status: "ok", acknowledged: 1 });
    expect(
      await ctx.redis.zscore(CLAIM_KEYS.index(b.name), claim.claim_id)
    ).toBeNull();
  });

  it("renewing a claim that already expired is claim_expired (wire)", async () => {
    const s = await connect(uniq("exp-src"));
    const b = await connect(uniq("exp-dst"));
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "expire me" });

    const claim = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 1 })
    );
    await delay(1300); // let the 1s lease lapse
    const renew = await b.call("renew_claim", {
      session_id: b.sessionId,
      claim_id: claim.claim_id,
      ttl_seconds: 300,
    });
    expect(renew.isError).toBe(true);
    expect(renew.data.error?.code).toBe("claim_expired");
  });

  it("budget exhaustion is refused with budget_exceeded (direct TaskClaimStore.renew path)", async () => {
    const store = new TaskClaimStore(ctx.redis);
    const actor = uniq("budget-act");
    const T0 = "2030-01-01T00:00:00.000Z";
    await ctx.redis.rpush(
      SESSION_KEYS.queue(actor),
      JSON.stringify({ id: "budget-msg", from: "x", to: actor, timestamp: T0, type: "task", payload: { content: "b" } })
    );
    const claimed = await store.claim({
      actor_id: actor,
      session_id: "sess-budget",
      max_batch: 1,
      ttl_seconds: 300,
      now: T0,
    });
    if (!claimed.ok) throw new Error("expected claim ok");

    // Renew far past the 30s budget -> budget_exceeded; claim stays intact.
    const res = await store.renew({
      claim_id: claimed.claim!.claim_id,
      actor_id: actor,
      session_id: "sess-budget",
      ttl_seconds: 300,
      budget_seconds: 30,
      now: "2030-01-01T00:00:40.000Z",
    });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error.code).toBe("budget_exceeded");
    }
    expect(await ctx.redis.hget(CLAIM_KEYS.claims, claimed.claim!.claim_id)).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// F. Recovery / redelivery
// ---------------------------------------------------------------------------
describe("recovery & redelivery", () => {
  it("an unacked claim past TTL is recovered to the inbox and re-claimable; late ack of the original claim fails", async () => {
    const s = await connect(uniq("rec-src"));
    const b = await connect(uniq("rec-dst"));
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "recover me" });

    const first = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 1 })
    );
    const originalClaimId = first.claim_id;
    await delay(1300);

    // A fresh claim lazily recovers the expired message and hands it back.
    const second = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(second.claim_id).not.toBe(originalClaimId);
    expect(taskIdOf(second.tasks[0]!)).toBe(taskIdOf(first.tasks[0]!));

    // The ORIGINAL claim was invalidated by recovery -> late ack is unknown_claim.
    const lateAck = await b.call("acknowledge_tasks", {
      session_id: b.sessionId,
      claim_id: originalClaimId,
    });
    expect(lateAck.isError).toBe(true);
    expect(lateAck.data.error?.code).toBe("unknown_claim");

    // Ack the replacement claim -> drained.
    await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: second.claim_id });
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// G. Dead-letter queue
// ---------------------------------------------------------------------------
describe("dead-letter queue recovery quarantine", () => {
  // Direct Lua eval of the recovery script with an arbitrary small cap, so this
  // test can drive a message to the DLQ without waiting out 5 real 1s leases.
  const dlqLua = readFileSync(
    new URL("../../src/mcp-server/lua/claims-recover.lua", import.meta.url),
    "utf-8"
  );
  const evalRecovery = async (actor: string, cap: number, nowMs: number): Promise<[number, number]> =>
    (await ctx.redis.eval(
      dlqLua,
      4,
      CLAIM_KEYS.index(actor),
      CLAIM_KEYS.claims,
      SESSION_KEYS.queue(actor),
      DLQ_KEYS.list(actor),
      nowMs,
      cap,
      1000,
      604800
    )) as [number, number];

  it("cap 1 (direct Lua): two unacked recover cycles dead-letter the message and requeue restores a fresh budget", async () => {
    const s = await connect(uniq("dlq-src"));
    const b = await connect(uniq("dlq-dst"));
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "die msg" });
    const farFuture = Date.now() + 2 * 3600 * 1000; // any stored claim is "expired"

    // First claim -> recover once (counter 1 <= cap 1) -> back to inbox.
    const c1 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    const msgId = taskIdOf(c1.tasks[0]!);
    const [rec1, dead1] = await evalRecovery(b.name, 1, farFuture);
    expect(rec1).toBe(1);
    expect(dead1).toBe(0);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(1);

    // Reclaim -> recover again (counter 2 > cap 1) -> dead-lettered, inbox empty.
    const c2 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(c2.tasks).toHaveLength(1);
    const [rec2, dead2] = await evalRecovery(b.name, 1, farFuture);
    expect(rec2).toBe(0);
    expect(dead2).toBe(1);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(1);

    // dlq_status surfaces it (wire).
    const dlq = await b.call("dlq_status", { session_id: b.sessionId });
    expect(dlq.data.status).toBe("ok");
    expect((dlq.data.entries as Array<{ message_id: string }>)[0]!.message_id).toBe(msgId);

    // Requeue -> fresh budget, back to inbox.
    const requeued = await b.call("dlq_requeue", { session_id: b.sessionId, message_id: msgId });
    expect(requeued.data).toMatchObject({ status: "ok", requeued: 1 });
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(0);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(1);

    // Fresh budget: recover once under cap 1 stays on the inbox (not dead-lettered).
    const c3 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(c3.tasks).toHaveLength(1);
    const [rec3, dead3] = await evalRecovery(b.name, 1, farFuture);
    expect(rec3).toBe(1);
    expect(dead3).toBe(0);
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(0);

    // The recovery consumed c3's claim; reclaim once more and ack to clean up.
    const c4 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    const ack = await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: c4.claim_id });
    expect(ack.data.status).toBe("ok");
  });

  it("full wire loop at RECOVER_CAP 5: after repeated unacked recovery the message lands in the DLQ, then requeue restores it", async () => {
    const s = await connect(uniq("dlq5-src"));
    const b = await connect(uniq("dlq5-dst"));
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "wire-cap5" });
    const msgLine = await ctx.redis.lindex(SESSION_KEYS.queue(b.name), 0);
    const msgId = taskIdOf(msgLine!);

    // 7 claim cycles at ttl 1s: the 7th recovery increments past cap 5 -> DLQ.
    let lastDeadlettered = false;
    for (let i = 0; i < 7; i += 1) {
      const r = await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 1 });
      if (r.data.claimed === true) {
        await delay(1300); // let the 1s lease lapse before the next (recovering) claim
      } else {
        lastDeadlettered = true;
        break;
      }
    }
    expect(lastDeadlettered).toBe(true);
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(1);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);

    // A further claim is empty; DLQ is visible over the wire.
    const empty = await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 });
    expect(empty.data.claimed).toBe(false);
    const dlq = await b.call("dlq_status", { session_id: b.sessionId });
    const entries = dlq.data.entries as Array<{ message_id: string }>;
    expect(entries.map((e) => e.message_id)).toContain(msgId);

    // Requeue -> fresh recovery budget -> reclaim + ack -> clean.
    await b.call("dlq_requeue", { session_id: b.sessionId, message_id: msgId });
    const final = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(taskIdOf(final.tasks[0]!)).toBe(msgId);
    await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: final.claim_id });
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(0);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
  });

  it("poison-message handling with a mixed batch: only the unacked poison dead-letters; acked siblings never do", async () => {
    const s = await connect(uniq("poison-src"));
    const b = await connect(uniq("poison-dst"));

    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "good-1" });
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "good-2" });
    await s.call("send_message", { session_id: s.sessionId, to: b.name, content: "poisoned-message" });

    // Ack the two good messages in their own single-message claims.
    for (let i = 0; i < 2; i += 1) {
      const c = expectClaimed(
        await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
      );
      await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: c.claim_id });
    }

    // The poison is claimed and left unacked; drive it to the DLQ with cap 1.
    const poison = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    const poisonId = taskIdOf(poison.tasks[0]!);
    const farFuture = Date.now() + 2 * 3600 * 1000;
    await evalRecovery(b.name, 1, farFuture); // counter 0->1, recover
    const poison2 = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    expect(taskIdOf(poison2.tasks[0]!)).toBe(poisonId);
    const [rec, dead] = await evalRecovery(b.name, 1, farFuture); // counter 1->2 > cap -> DLQ
    expect(rec).toBe(0);
    expect(dead).toBe(1);

    const dlq = await b.call("dlq_status", { session_id: b.sessionId });
    const dlqIds = (dlq.data.entries as Array<{ message_id: string }>).map((e) => e.message_id);
    expect(dlqIds).toEqual([poisonId]); // ONLY the poison ever dead-letters

    // Clean up the poison claim's remaining state is already gone (dead-lettered).
    expect(await ctx.redis.llen(DLQ_KEYS.list(b.name))).toBe(1);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// I. Idempotent send
// ---------------------------------------------------------------------------
describe("idempotent send", () => {
  it("reusing an idempotency_key returns the original message_id and enqueues only once", async () => {
    const s = await connect(uniq("idem-src"));
    const r = await connect(uniq("idem-dst"));

    const first = await s.call("send_message", {
      session_id: s.sessionId,
      to: r.name,
      content: "unique work",
      idempotency_key: "k-retry-1",
    });
    expect(first.data.status).toBe("sent");
    expect(first.data.deduplicated).toBe(false);
    const mid = first.data.message_id as string;

    const second = await s.call("send_message", {
      session_id: s.sessionId,
      to: r.name,
      content: "unique work",
      idempotency_key: "k-retry-1",
    });
    expect(second.data.status).toBe("duplicate");
    expect(second.data.deduplicated).toBe(true);
    expect(second.data.message_id).toBe(mid);

    // Exactly one envelope in the inbox carrying that message id.
    expect(await ctx.redis.llen(SESSION_KEYS.queue(r.name))).toBe(1);
    const raw = await ctx.redis.lindex(SESSION_KEYS.queue(r.name), 0);
    expect(taskIdOf(raw!)).toBe(mid);
  });
});

// ---------------------------------------------------------------------------
// J. Durable-actor gate
// ---------------------------------------------------------------------------
describe("durable-actor receive gate", () => {
  it("a store_only durable record blocks receive_message but claim_tasks still works; inbox untouched", async () => {
    const a = await connect(uniq("gate-src"));
    const b = await connect(uniq("gate-dst"));
    await a.call("send_message", { session_id: a.sessionId, to: b.name, content: "gated" });
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(1);

    // Register a durable (store_only) actor record owned by b's actor_id.
    const reg = await a.call("actor_register", {
      session_id: a.sessionId,
      actor_id: b.name,
      alias: `${b.name}-alias`,
      activation_policy_mode: "store_only",
      max_concurrency: 1,
    });
    expect(reg.data.status).toBe("ok");

    // receive_message is now rejected; the inbox is NOT consumed.
    const gated = await b.call("receive_message", { session_id: b.sessionId, timeout: 1 });
    expect(gated.isError).toBe(true);
    expect(gated.data.error?.code).toBe("durable_actor_claim_required");
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(1);

    // claim_tasks still works for the durable actor.
    const claim = expectClaimed(
      await b.call("claim_tasks", { session_id: b.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    await b.call("acknowledge_tasks", { session_id: b.sessionId, claim_id: claim.claim_id });
    expect(await ctx.redis.llen(SESSION_KEYS.queue(b.name))).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// K. Concurrency ceiling
// ---------------------------------------------------------------------------
describe("claim concurrency ceiling (directory max_concurrency)", () => {
  it("max_concurrency 1: a second session's claim is rejected until the first acks", async () => {
    const actor = uniq("conc");
    const owner = await connect(uniq("conc-owner"));
    const ownerReg = await owner.call("actor_register", {
      session_id: owner.sessionId,
      actor_id: actor,
      alias: actor,
      activation_policy_mode: "store_only",
      max_concurrency: 1,
    });
    expect(ownerReg.data.status).toBe("ok");

    const c1 = await connect(actor);
    const c2 = await connect(actor);
    const sender = await connect(uniq("conc-src"));
    await sender.call("send_message", { session_id: sender.sessionId, to: actor, content: "x1" });
    await sender.call("send_message", { session_id: sender.sessionId, to: actor, content: "x2" });

    const first = expectClaimed(
      await c1.call("claim_tasks", { session_id: c1.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    const second = await c2.call("claim_tasks", { session_id: c2.sessionId, max_batch: 1, ttl_seconds: 300 });
    expect(second.isError).toBe(true);
    expect(second.data.error?.code).toBe("concurrency_limit_reached");

    await c1.call("acknowledge_tasks", { session_id: c1.sessionId, claim_id: first.claim_id });
    const third = expectClaimed(
      await c2.call("claim_tasks", { session_id: c2.sessionId, max_batch: 1, ttl_seconds: 300 })
    );
    await c2.call("acknowledge_tasks", { session_id: c2.sessionId, claim_id: third.claim_id });
  });

  it("max_concurrency 2 admits two outstanding claims and rejects the third", async () => {
    const actor = uniq("conc2");
    const owner = await connect(uniq("conc2-owner"));
    const ownerReg = await owner.call("actor_register", {
      session_id: owner.sessionId,
      actor_id: actor,
      alias: actor,
      activation_policy_mode: "store_only",
      max_concurrency: 2,
    });
    expect(ownerReg.data.status).toBe("ok");

    const c1 = await connect(actor);
    const c2 = await connect(actor);
    const c3 = await connect(actor);
    const sender = await connect(uniq("conc2-src"));
    for (let i = 0; i < 4; i += 1) {
      await sender.call("send_message", { session_id: sender.sessionId, to: actor, content: `t${i}` });
    }

    const r1 = expectClaimed(await c1.call("claim_tasks", { session_id: c1.sessionId, max_batch: 1, ttl_seconds: 300 }));
    const r2 = expectClaimed(await c2.call("claim_tasks", { session_id: c2.sessionId, max_batch: 1, ttl_seconds: 300 }));
    const r3 = await c3.call("claim_tasks", { session_id: c3.sessionId, max_batch: 1, ttl_seconds: 300 });
    expect(r3.isError).toBe(true);
    expect(r3.data.error?.code).toBe("concurrency_limit_reached");

    await c1.call("acknowledge_tasks", { session_id: c1.sessionId, claim_id: r1.claim_id });
    await c2.call("acknowledge_tasks", { session_id: c2.sessionId, claim_id: r2.claim_id });
    const r4 = expectClaimed(await c3.call("claim_tasks", { session_id: c3.sessionId, max_batch: 1, ttl_seconds: 300 }));
    await c3.call("acknowledge_tasks", { session_id: c3.sessionId, claim_id: r4.claim_id });
  });
});

// ---------------------------------------------------------------------------
// L. Worktree custody through the wire
// ---------------------------------------------------------------------------
describe("worktree custody wire lifecycle", () => {
  const SHA = "a".repeat(40);

  it("initial claim -> already_held -> release -> graceful_handoff reclaim", async () => {
    const a = await connect(uniq("cust-a"));
    const a2 = await connect(uniq("cust-a2"));
    const ws = `/worktrees/ws-${uniq("one")}`;

    const claimRes = await a.call("custody_claim", {
      session_id: a.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-initial",
      lease_seconds: 300,
    });
    expect(claimRes.data.status).toBe("ok");
    expect(claimRes.data.record.state).toBe("held");
    expect(claimRes.data.record.mode).toBe("initial");
    expect(claimRes.data.record.custodian.session_id).toBe(a.sessionId);

    // A second session claiming the same worktree -> already_held.
    const conflict = await a2.call("custody_claim", {
      session_id: a2.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-other",
      lease_seconds: 300,
    });
    expect(conflict.isError).toBe(true);
    expect(conflict.data.error?.code).toBe("already_held");

    // The holder sees the record via custody_status.
    const status = await a.call("custody_status", { worktree_path: ws });
    expect(status.data.status).toBe("ok");
    expect(status.data.record.state).toBe("held");

    // The holding session releases with a structured handoff -> released.
    const release = await a.call("custody_release", {
      session_id: a.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tracked_tree_state: "clean",
      untracked_inventory: [],
      unfinished_work: "left the tree clean",
      next_step: "next custodian may proceed",
      hazards: [],
    });
    expect(release.data.status).toBe("ok");
    expect(release.data.record.state).toBe("released");
    expect(release.data.record.handoff.next_step).toBe("next custodian may proceed");

    // Reclaim by a new custodian -> graceful_handoff mode.
    const reclaim = await a2.call("custody_claim", {
      session_id: a2.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-reclaim",
      lease_seconds: 300,
    });
    expect(reclaim.data.status).toBe("ok");
    expect(reclaim.data.record.state).toBe("held");
    expect(reclaim.data.record.mode).toBe("graceful_handoff");
  });

  it("lease expiry forfeits the record; takeover without inventory fails, with inventory succeeds", async () => {
    const a = await connect(uniq("cust-exp-a"));
    const a2 = await connect(uniq("cust-exp-a2"));
    const ws = `/worktrees/ws-${uniq("expire")}`;

    const claimRes = await a.call("custody_claim", {
      session_id: a.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-exp",
      lease_seconds: 1,
    });
    expect(claimRes.data.status).toBe("ok");

    // Let the 1s lease lapse, then observe lazy forfeiture via custody_status.
    await delay(1500);
    const status = await a.call("custody_status", { worktree_path: ws });
    expect(status.data.status).toBe("ok");
    expect(status.data.record.state).toBe("forfeited");

    // Takeover WITHOUT inventory is refused.
    const noInv = await a2.call("custody_claim", {
      session_id: a2.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-no-inv",
      lease_seconds: 300,
    });
    expect(noInv.isError).toBe(true);
    expect(noInv.data.error?.code).toBe("takeover_requires_inventory");

    // Takeover WITH an inventory succeeds as successor_takeover.
    const takeover = await a2.call("custody_claim", {
      session_id: a2.sessionId,
      worktree_path: ws,
      repo_head: SHA,
      tree_fingerprint: "fp-takeover",
      lease_seconds: 300,
      inventory: [".work/inventory.yaml", "notes.md"],
    });
    expect(takeover.data.status).toBe("ok");
    expect(takeover.data.record.state).toBe("held");
    expect(takeover.data.record.mode).toBe("successor_takeover");
  });
});