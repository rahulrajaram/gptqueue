import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { startInboxDispatcher } from "../src/registered-shell/inbox-dispatcher.js";
import type { RuntimeAdapter, RuntimeBinding } from "../src/registered-shell/runtime.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const redis = new Redis(url);
const live: Array<{ agent: string; dispatcher: Awaited<ReturnType<typeof startInboxDispatcher>> }> = [];
const owned = new Set<string>();
const binding = (id: string): RuntimeBinding => ({ client: "codex", runtime_id: id, epoch: "epoch-1", working_directory: "/workspace" });
const task = (to: string, from = "sender") => ({ id: randomUUID(), from, to, timestamp: new Date().toISOString(), type: "task" as const, payload: { content: "do work" } });
const eventually = async (check: () => Promise<boolean>) => { for (let i = 0; i < 100; i += 1) { if (await check()) return; await new Promise((r) => setTimeout(r, 10)); } throw new Error("condition timed out"); };

afterEach(async () => {
  for (const { dispatcher } of live.splice(0)) await dispatcher.close().catch(() => undefined);
  for (const agent of owned) await redis.del(`gptq:runtime-binding:${agent}`, `gptq:activation:${agent}`, `gptq:inbox-events:${agent}`, `gptq:inbox-trace:${agent}`, SESSION_KEYS.queue(agent), CLAIM_KEYS.index(agent));
  owned.clear();
  await redis.del(CLAIM_KEYS.claims);
});
afterAll(async () => { await redis.quit(); });

describe("inbox dispatcher", () => {
  it("activates from a queued task without destructive receive and coalesces notifications", async () => {
    const agent = `dispatcher-${randomUUID()}`; const mailbox = new MailboxStore(redis, redis);
    const calls: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-a"), activate: async (request) => { if (!request.recover_only) calls.push(request.operation_id); return { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher }); owned.add(agent);
    await mailbox.send(task(agent)); await mailbox.send(task(agent));
    await eventually(async () => calls.length === 1);
    expect(calls).toHaveLength(1);
    expect(await redis.llen(SESSION_KEYS.queue(agent))).toBe(2);
    expect(await redis.zcard(CLAIM_KEYS.index(agent))).toBe(0);
  });

  it("keeps an ambiguous native submission recover-only and fences an old dispatcher", async () => {
    const agent = `dispatcher-${randomUUID()}`; const mailbox = new MailboxStore(redis, redis);
    const requests: Array<{ operation_id: string; recover_only?: boolean }> = [];
    const first: RuntimeAdapter = { binding: binding("runtime-old"), activate: async (request) => { requests.push(request); return { status: "ambiguous" }; }, close: async () => {} };
    const old = await startInboxDispatcher(redis, agent, first, { intervalMs: 10 }); live.push({ agent, dispatcher: old }); owned.add(agent);
    await mailbox.send(task(agent)); await eventually(async () => requests.length >= 1);
    await old.close();
    const secondCalls: Array<{ operation_id: string; recover_only?: boolean }> = [];
    const second: RuntimeAdapter = { binding: { ...binding("runtime-old"), epoch: "epoch-2" }, activate: async (request) => { secondCalls.push(request); return { status: "queued" }; }, close: async () => {} };
    const newer = await startInboxDispatcher(redis, agent, second, { intervalMs: 10 }); live.push({ agent, dispatcher: newer });
    expect(await redis.get(`gptq:runtime-binding:${agent}`)).toContain("epoch-2");
    expect(requests[0]?.recover_only).toBe(false);
    await eventually(async () => secondCalls.length >= 1);
    expect(secondCalls.every((request) => request.recover_only === true)).toBe(true);
    expect(secondCalls[0]?.operation_id).toBe(requests[0]?.operation_id);
    expect(secondCalls[0]?.recover_only).toBe(true);
    expect(await old.close()).toBeUndefined();
  });

  it("claims, replies, and acknowledges through the real stores", async () => {
    const agent = `dispatcher-${randomUUID()}`; const sender = `sender-${randomUUID()}`; owned.add(agent); owned.add(sender); const mailbox = new MailboxStore(redis, redis); const claims = new TaskClaimStore(redis);
    const message = task(agent, sender); await mailbox.send(message);
    let claimId = "";
    let acknowledged = false;
    let delivered: { operation_id: string; prompt: string; recover_only?: boolean } | undefined;
    const adapter: RuntimeAdapter = { binding: binding("runtime-claims"), activate: async (request) => {
      delivered = request;
      const result = await claims.claim({ actor_id: agent, session_id: "session-1", max_batch: 1, ttl_seconds: 30, now: new Date().toISOString() });
      if (result.ok && result.claim) { claimId = result.claim.claim_id; await mailbox.send({ id: randomUUID(), from: agent, to: sender, timestamp: new Date().toISOString(), type: "result", payload: { content: "done", in_reply_to: message.id } }); await claims.acknowledge({ actor_id: agent, session_id: "session-1", claim_id: claimId }); acknowledged = true; }
      return { status: "completed", turn_id: "turn-1" };
    }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    const senderCalls: string[] = [];
    const senderAdapter: RuntimeAdapter = { binding: binding("runtime-sender"), activate: async (request) => { senderCalls.push(request.operation_id); return { status: "queued" }; }, close: async () => {} };
    const senderDispatcher = await startInboxDispatcher(redis, sender, senderAdapter, { intervalMs: 10 }); live.push({ agent: sender, dispatcher: senderDispatcher });
    // claimId is set before the reply and the acknowledgement run; wait for both to finish.
    await eventually(async () => Boolean(claimId) && acknowledged);
    expect(delivered?.operation_id).toMatch(/^[a-f0-9]{64}$/);
    expect(delivered?.prompt).toContain(agent);
    expect(delivered?.prompt).toContain(delivered!.operation_id);
    expect(delivered?.recover_only).toBe(false);
    expect(await redis.zcard(CLAIM_KEYS.index(agent))).toBe(0);
    expect(JSON.parse((await redis.lindex(SESSION_KEYS.queue(sender), 0))!).payload.in_reply_to).toBe(message.id);
    await eventually(async () => senderCalls.length === 1);
  });

  it("does not activate for an empty inbox and retries a busy runtime when work arrives", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); let calls = 0;
    const adapter: RuntimeAdapter = { binding: binding("runtime-busy"), activate: async () => { calls += 1; return calls === 1 ? { status: "busy" } : { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await new Promise((resolve) => setTimeout(resolve, 40)); expect(calls).toBe(0);
    await mailbox.send(task(agent)); await eventually(async () => calls >= 1); await mailbox.send(task(agent));
    await eventually(async () => calls >= 2); expect(calls).toBeGreaterThanOrEqual(2);
  });
  it("backs off repeated activation of a runtime that stays busy", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); let calls = 0;
    const at: number[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-backoff"), activate: async () => { calls += 1; at.push(Date.now()); return { status: "busy" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 50, maxBackoffMs: 400 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    for (let waited = 0; calls < 5 && waited < 3_000; waited += 20) await new Promise((r) => setTimeout(r, 20));
    // Waits double from the interval (50, 100, 200, 400 ms); Redis rounds each blocking tick up to ~100 ms,
    // so without backoff every gap stays near one tick.
    const gaps = at.slice(1).map((t, i) => t - at[i]!);
    expect(gaps).toHaveLength(4); expect(gaps[3]!).toBeGreaterThanOrEqual(300);
    const requested = (await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+")).filter(([, fields]) => fields.includes("activation_requested"));
    expect(requested.length).toBe(calls);
  });
  it("traces one deferral per reason streak, distinguishing busy from unavailable", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); let calls = 0;
    // busy twice, then unavailable twice: two deferral traces, one per distinct code.
    const adapter: RuntimeAdapter = { binding: binding("runtime-defer"),
      activate: async () => ({ status: ++calls <= 2 ? "busy" : "unavailable" }), close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 20, maxBackoffMs: 20 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    for (let waited = 0; calls < 4 && waited < 3_000; waited += 10) await new Promise((r) => setTimeout(r, 10));
    const deferred = (await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+")).filter(([, fields]) => fields.includes("activation_deferred"));
    expect(deferred).toHaveLength(2);
    expect(deferred[0]![1]).toContain("runtime_busy");
    expect(deferred[1]![1]).toContain("runtime_unavailable");
    // No trace floods despite repeated retries within each reason streak.
    expect(calls).toBeGreaterThanOrEqual(4);
  });
  it("bounds completed retries, records exhaustion, and keeps pending work", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); const requests: string[] = [];
    const prompts: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-exhaust"), activate: async (request) => { prompts.push(request.prompt); requests.push(request.operation_id); return { status: "completed", turn_id: `turn-${requests.length}` }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, maxAttempts: 3 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => (await redis.get(`gptq:activation:${agent}`))?.includes('"state":"exhausted"') ?? false);
    await redis.xadd(`gptq:inbox-events:${agent}`, "*", "type", "task");
    await new Promise(r => setTimeout(r, 50));
    expect(new Set(requests).size).toBe(3);
    expect(requests).toHaveLength(3);
    expect(prompts.every((prompt, index) => prompt.includes(agent) && prompt.includes(requests[index]!))).toBe(true);
    expect(await redis.llen(SESSION_KEYS.queue(agent))).toBe(1);
    const traces = await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+"); expect(traces.map(([, fields]) => fields.join(" ")).join(" ")).toContain("activation_attempts_exhausted");
  });
  it("re-arms activation for new work that arrives after an exhausted message", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); const requests: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-rearm"), activate: async (request) => { requests.push(request.operation_id); return { status: "completed", turn_id: `turn-${requests.length}` }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, maxAttempts: 2 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => (await redis.get(`gptq:activation:${agent}`))?.includes('"state":"exhausted"') ?? false);
    expect(requests).toHaveLength(2);
    await mailbox.send(task(agent)); // new work while the first message is still pending
    await eventually(async () => requests.length >= 3);
    const record = JSON.parse((await redis.get(`gptq:activation:${agent}`))!) as { message_ids: string[] };
    expect(record.message_ids).toHaveLength(2);
  });
  it("recovers unavailable activation and stops after ownership is fenced", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); let calls = 0;
    const prompts: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-fence"), activate: async (request) => { prompts.push(request.prompt); calls += 1; return calls === 1 ? { status: "unavailable" } : { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => calls >= 2);
    expect(prompts.every((prompt) => prompt.includes(agent))).toBe(true);
    await redis.set(`gptq:runtime-binding:${agent}`, "foreign-owner", "EX", 30);
    await dispatcher.closed;
    const stoppedAt = calls;
    await mailbox.send(task(agent));
    await new Promise(r => setTimeout(r, 50));
    expect(calls).toBe(stoppedAt);
    expect(await redis.get(`gptq:runtime-binding:${agent}`)).toBe("foreign-owner");
  });

  const traceCodes = async (agent: string) => (await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+")).map(([, fields]) => fields.join(" "));
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  it("returns a pre-submit failure to pending and later activates with a fresh submission", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis);
    const requests: Array<{ operation_id: string; recover_only?: boolean }> = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-throw"), activate: async (request) => {
      requests.push(request);
      if (requests.length === 1) throw new Error("thread/read socket closed");
      return { status: "queued" };
    }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => (await redis.get(`gptq:activation:${agent}`))?.includes('"state":"accepted"') ?? false);
    // The retry is a real submission, not a recover-only probe that would report ambiguous forever.
    expect(requests[1]?.recover_only).toBe(false);
    expect(requests[1]?.operation_id).toBe(requests[0]?.operation_id);
    expect((await traceCodes(agent)).join(" ")).toContain("activation_submit_failed");
  });

  it("re-attempts an accepted activation under a new operation id after the settle deadline", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis);
    const requests: Array<{ operation_id: string; recover_only?: boolean }> = [];
    // A follow-up accepted then silently dropped: recovery keeps reporting queued.
    const adapter: RuntimeAdapter = { binding: binding("runtime-drop"), activate: async (request) => { requests.push(request); return { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, settleTimeoutMs: 150 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => new Set(requests.filter((r) => !r.recover_only).map((r) => r.operation_id)).size >= 2);
    expect((await traceCodes(agent)).join(" ")).toContain("activation_deadline_expired");
  });

  it("bounds an ambiguous record recovered after restart and then re-attempts it", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis);
    const first: RuntimeAdapter = { binding: binding("runtime-amb"), activate: async () => ({ status: "ambiguous" }), close: async () => {} };
    const old = await startInboxDispatcher(redis, agent, first, { intervalMs: 10 }); live.push({ agent, dispatcher: old });
    await mailbox.send(task(agent));
    await eventually(async () => (await redis.get(`gptq:activation:${agent}`))?.includes('"state":"ambiguous"') ?? false);
    await old.close();
    const original = (JSON.parse((await redis.get(`gptq:activation:${agent}`))!) as { operation_id: string }).operation_id;
    const requests: Array<{ operation_id: string; recover_only?: boolean }> = [];
    const second: RuntimeAdapter = { binding: { ...binding("runtime-amb"), epoch: "epoch-2" }, activate: async (request) => {
      requests.push(request); return request.recover_only ? { status: "ambiguous" } : { status: "queued" };
    }, close: async () => {} };
    const newer = await startInboxDispatcher(redis, agent, second, { intervalMs: 10, settleTimeoutMs: 150 }); live.push({ agent, dispatcher: newer });
    await eventually(async () => requests.some((r) => !r.recover_only));
    expect(requests[0]).toMatchObject({ operation_id: original, recover_only: true });
    expect(requests.find((r) => !r.recover_only)?.operation_id).not.toBe(original);
  });

  it("re-arms an exhausted record after the cooldown without new work", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); const requests: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-cool"), activate: async (request) => { requests.push(request.operation_id); return { status: "completed", turn_id: `turn-${requests.length}` }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, maxAttempts: 2, exhaustedCooldownMs: 150 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => requests.length >= 3);
    expect(new Set(requests).size).toBe(requests.length);
    expect((await traceCodes(agent)).join(" ")).toContain("activation_cooldown_elapsed");
  });

  it("re-acquires a lease lost during one long activation and still delivers", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); let calls = 0;
    const adapter: RuntimeAdapter = { binding: binding("runtime-lease"), activate: async () => {
      calls += 1;
      // The 30 s lease lapses while native calls are slow.
      if (calls === 1) { await redis.del(`gptq:runtime-binding:${agent}`); await sleep(30); return { status: "busy" }; }
      return { status: "queued" };
    }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, leaseRenewMs: 10_000 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => (await redis.get(`gptq:activation:${agent}`))?.includes('"state":"accepted"') ?? false);
    expect(await redis.get(`gptq:runtime-binding:${agent}`)).toContain("runtime-lease");
    expect((await traceCodes(agent)).join(" ")).toContain("lease_reacquired");
    let settled = false; void dispatcher.closed.then(() => { settled = true; }); await sleep(20); expect(settled).toBe(false);
  });

  // Documented limitation (not tested): the pending read and activate are not
  // atomic, so mail claimed between them can still be followed by one prompt.
  // Worst case is one empty turn; claims prevent duplicate work.
  it("never wakes an agent for mail it already holds in a claim", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); const claims = new TaskClaimStore(redis);
    await mailbox.send(task(agent));
    const held = await claims.claim({ actor_id: agent, session_id: "s", max_batch: 1, ttl_seconds: 30, now: new Date().toISOString() });
    expect(held.ok && held.claim?.tasks).toHaveLength(1);
    const submissions: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-held"), activate: async (request) => { submissions.push(request.operation_id); return { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await sleep(150);
    expect(submissions).toHaveLength(0);
  });

  it("wakes an idle agent for new mail even while it holds an open claim", async () => {
    // A lead that claims work, idles, and acks later must still be woken for new
    // mail; the open claim once suppressed every wake for hours.
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis); const claims = new TaskClaimStore(redis);
    await mailbox.send(task(agent));
    const held = await claims.claim({ actor_id: agent, session_id: "s", max_batch: 1, ttl_seconds: 30, now: new Date().toISOString() });
    expect(held.ok && held.claim?.tasks).toHaveLength(1);
    const prompts: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-idle-holder"), activate: async (request) => { if (!request.recover_only) prompts.push(request.prompt); return { status: "queued" }; }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    const fresh = task(agent); await mailbox.send(fresh);
    await eventually(async () => prompts.length >= 1);
    expect(prompts).toHaveLength(1);
    expect(await redis.zcard(CLAIM_KEYS.index(agent))).toBe(1); // the held claim is untouched
  });

  it("renders the wake prompt with the bound client's tool naming (I-1)", async () => {
    const mailbox = new MailboxStore(redis, redis);
    const promptFor = async (client: "codex" | "opencode") => {
      const agent = `dispatcher-${randomUUID()}`; owned.add(agent);
      const prompts: string[] = [];
      const identity = client === "codex" ? binding(`runtime-${client}`)
        : { client: "opencode" as const, runtime_id: "ses-1", epoch: "ses-1", working_directory: "/workspace" };
      const adapter: RuntimeAdapter = { binding: identity, activate: async (request) => { prompts.push(request.prompt); return { status: "queued" }; }, close: async () => {} };
      const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
      await mailbox.send(task(agent));
      await eventually(async () => prompts.length >= 1);
      return prompts[0]!;
    };
    expect(await promptFor("opencode")).toContain("Call gptqueue_claim_tasks");
    expect(await promptFor("codex")).toContain("Call claim_tasks");
  });

  it("refuses a second live dispatcher with the typed RUNTIME_BINDING_HELD code (I-2)", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent);
    const adapter: RuntimeAdapter = { binding: binding("runtime-held"), activate: async () => ({ status: "queued" }), close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await expect(startInboxDispatcher(redis, agent, { ...adapter, binding: binding("runtime-rival") }, { intervalMs: 10 }))
      .rejects.toMatchObject({ code: "RUNTIME_BINDING_HELD" });
  });

  it("re-attempts an undelivered activation at once under a fresh operation, without reporting a turn (I-3)", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis);
    const requests: string[] = [];
    const adapter: RuntimeAdapter = { binding: binding("runtime-dropped"),
      activate: async (request) => { requests.push(request.operation_id); return requests.length === 1 ? { status: "undelivered" } : { status: "queued" }; },
      close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10, maxBackoffMs: 60_000 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => requests.length >= 2);
    expect(new Set(requests).size).toBe(2);
    const traces = await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+");
    const dropped = traces.filter(([, fields]) => fields.includes("native_delivery_dropped"));
    expect(dropped).toHaveLength(1);
    expect(dropped[0]![1]).toContain(requests[0]!);
    expect(traces.some(([, fields]) => fields.includes("turn_id"))).toBe(false);
  });

  it("retries a busy activation as soon as the runtime reports idle, not after backoff (I-4)", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent); const mailbox = new MailboxStore(redis, redis);
    const at: number[] = [];
    const listeners = new Set<() => void>();
    let unsubscribed = 0;
    const adapter: RuntimeAdapter = { binding: binding("runtime-idle-hint"),
      activate: async () => { at.push(Date.now()); return at.length === 1 ? { status: "busy" } : { status: "queued" }; },
      onIdle: (listener) => { listeners.add(listener); return () => { unsubscribed += 1; listeners.delete(listener); }; },
      close: async () => {} };
    // Without the hint the retry waits at least one 3 s interval/backoff.
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 3_000, maxBackoffMs: 60_000 }); live.push({ agent, dispatcher });
    await mailbox.send(task(agent));
    await eventually(async () => at.length >= 1);
    await new Promise((r) => setTimeout(r, 100));
    const idleAt = Date.now();
    for (const listener of listeners) listener();
    await eventually(async () => at.length >= 2);
    expect(at[1]! - idleAt).toBeLessThan(1_000);
    await dispatcher.close();
    expect(unsubscribed).toBe(1);
  });

  it("surfaces a typed diagnostic when a live foreign owner fences the dispatcher", async () => {
    const agent = `dispatcher-${randomUUID()}`; owned.add(agent);
    const adapter: RuntimeAdapter = { binding: binding("runtime-fenced"), activate: async () => ({ status: "queued" }), close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    await redis.set(`gptq:runtime-binding:${agent}`, "foreign-owner", "EX", 30);
    await dispatcher.closed;
    expect((await traceCodes(agent)).join(" ")).toContain("dispatcher_lease_lost");
  });
});
