import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { startInboxDispatcher } from "../src/registered-shell/inbox-dispatcher.js";
import type { RuntimeAdapter, RuntimeBinding } from "../src/registered-shell/runtime.js";

const url = "redis://127.0.0.1:6379/15";
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
    expect(secondCalls[0]?.recover_only).toBe(true);
    expect(await old.close()).toBeUndefined();
  });

  it("claims, replies, and acknowledges through the real stores", async () => {
    const agent = `dispatcher-${randomUUID()}`; const sender = `sender-${randomUUID()}`; owned.add(agent); owned.add(sender); const mailbox = new MailboxStore(redis, redis); const claims = new TaskClaimStore(redis);
    const message = task(agent, sender); await mailbox.send(message);
    let claimId = "";
    let delivered: { operation_id: string; prompt: string; recover_only?: boolean } | undefined;
    const adapter: RuntimeAdapter = { binding: binding("runtime-claims"), activate: async (request) => {
      delivered = request;
      const result = await claims.claim({ actor_id: agent, session_id: "session-1", max_batch: 1, ttl_seconds: 30, now: new Date().toISOString() });
      if (result.ok && result.claim) { claimId = result.claim.claim_id; await mailbox.send({ id: randomUUID(), from: agent, to: sender, timestamp: new Date().toISOString(), type: "result", payload: { content: "done", in_reply_to: message.id } }); await claims.acknowledge({ actor_id: agent, session_id: "session-1", claim_id: claimId }); }
      return { status: "completed", turn_id: "turn-1" };
    }, close: async () => {} };
    const dispatcher = await startInboxDispatcher(redis, agent, adapter, { intervalMs: 10 }); live.push({ agent, dispatcher });
    const senderCalls: string[] = [];
    const senderAdapter: RuntimeAdapter = { binding: binding("runtime-sender"), activate: async (request) => { senderCalls.push(request.operation_id); return { status: "queued" }; }, close: async () => {} };
    const senderDispatcher = await startInboxDispatcher(redis, sender, senderAdapter, { intervalMs: 10 }); live.push({ agent: sender, dispatcher: senderDispatcher });
    await eventually(async () => Boolean(claimId));
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
});
