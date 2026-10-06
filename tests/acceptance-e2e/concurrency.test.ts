/**
 * DL-8: 4 senders x 3 recipients x 25 messages, with concurrent claim/ack,
 * an abandoned short-lease claim per recipient (lease recovery), and replies.
 * Oracle: exactly-once acknowledgement by message id per recipient, no
 * cross-delivery, nothing lost, every reply correlated to its exact request.
 */
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { RedisClient } from "../../src/mcp-server/redis-client.js";
import { claimTasks, claimTasksSchema } from "../../src/mcp-server/tools/claim-tasks.js";
import { acknowledgeTasks, acknowledgeTasksSchema } from "../../src/mcp-server/tools/acknowledge-tasks.js";
import { sendMessage, sendMessageSchema } from "../../src/mcp-server/tools/send-message.js";
import { parseToolResult, REDIS_URL, startSender, waitFor, type QueuedMessage, type Sender } from "./harness.js";

const SENDERS = 4, RECIPIENTS = 3, PER_PAIR = 25, WORKERS_PER_RECIPIENT = 3;
const open: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of open.splice(0).reverse()) await close().catch(() => undefined); });

type Worker = Readonly<{ name: string; client: RedisClient }>;
const claim = async (worker: Worker, max: number, ttl: number) => {
  const payload = parseToolResult(await claimTasks(worker.client, claimTasksSchema.parse({ session_id: worker.client.sessionId, max_batch: max, ttl_seconds: ttl })));
  const c = payload.claim as { claim_id: string; tasks: string[] } | null;
  return c ? { claim_id: c.claim_id, tasks: c.tasks.map((raw) => (typeof raw === "string" ? JSON.parse(raw) : raw) as QueuedMessage) } : null;
};

describe("DL-8 concurrency stress", () => {
  it("4 senders x 3 recipients x 25 messages: exactly once per recipient, no cross-delivery, nothing lost", async () => {
    const senders: Sender[] = [];
    for (let i = 0; i < SENDERS; i += 1) { const s = await startSender(`dl8-${i}`); senders.push(s); open.push(() => s.close()); }
    const recipients: Worker[] = [];
    for (let i = 0; i < RECIPIENTS; i += 1) {
      const client = new RedisClient(null, REDIS_URL); const name = `e2e-dl8-r${i}-${randomUUID()}`;
      await client.register("both", name); recipients.push({ name, client });
      open.push(async () => { await client.closeCurrentSession(); await client.shutdown(); });
    }
    const expected = new Map(recipients.map((r) => [r.name, new Set<string>()]));
    const acks = new Map<string, number>();               // message id -> acknowledgements
    const claimedBy = new Map<string, Set<string>>();       // message id -> recipients that claimed it
    const crossDelivered: string[] = [];
    const abandoned = new Set<string>();
    let sendingDone = false;

    // Concurrent sending: every sender to every recipient, interleaved.
    const sending = Promise.all(senders.map(async (sender) => {
      for (let n = 0; n < PER_PAIR; n += 1) {
        await Promise.all(recipients.map(async (r) => {
          const { message_id } = await sender.send(r.name, `compute:${n}+${r.name.length}`, { idempotency_key: `dl8-${sender.name}-${r.name}-${n}` });
          expected.get(r.name)!.add(message_id);
        }));
      }
    })).then(() => { sendingDone = true; });

    // One abandoned claim per recipient, with a 1 s lease, once mail exists: recovery must redeliver it.
    await waitFor(async () => (await Promise.all(recipients.map((r) => r.client.adapterConnection.llen(`gptq:q:${r.name}`)))).every((n) => n >= 2), 10_000, "initial mail");
    for (const r of recipients) { const c = await claim(r, 2, 1); for (const t of c?.tasks ?? []) abandoned.add(t.id); }

    // Concurrent workers per recipient, sharing the recipient's session, racing on claim/ack.
    const done = (r: Worker) => sendingDone && [...expected.get(r.name)!].every((id) => (acks.get(id) ?? 0) > 0);
    const work = (r: Worker) => (async () => {
      const deadline = Date.now() + 40_000;
      while (!done(r) && Date.now() < deadline) {
        const c = await claim(r, 4, 30);
        if (!c) { await new Promise((resolve) => setTimeout(resolve, 20)); continue; }
        for (const task of c.tasks) {
          if (task.to !== r.name) crossDelivered.push(`${task.id}:${task.to}->${r.name}`);
          claimedBy.set(task.id, (claimedBy.get(task.id) ?? new Set()).add(r.name));
          parseToolResult(await sendMessage(r.client, sendMessageSchema.parse({ session_id: r.client.sessionId, to: task.from, type: "result",
            content: `ok:${task.id}`, in_reply_to: task.id, idempotency_key: `reply-${task.id}` })));
        }
        parseToolResult(await acknowledgeTasks(r.client, acknowledgeTasksSchema.parse({ session_id: r.client.sessionId, claim_id: c.claim_id })));
        for (const task of c.tasks) acks.set(task.id, (acks.get(task.id) ?? 0) + 1);
      }
    })();
    // Senders consume their replies while sending, as live agents do; an undrained
    // sender would correctly hit its mailbox bound (QUEUE_FULL, DL-6).
    const replyTotal = RECIPIENTS * PER_PAIR;
    const draining = senders.map(async (sender) => {
      const deadline = Date.now() + 45_000;
      while (sender.received.length < replyTotal && Date.now() < deadline) {
        if (!(await sender.drain()).length) await new Promise((resolve) => setTimeout(resolve, 20));
      }
    });
    await Promise.all([sending, ...recipients.flatMap((r) => Array.from({ length: WORKERS_PER_RECIPIENT }, () => work(r))), ...draining]);

    expect(crossDelivered).toEqual([]);
    expect(abandoned.size).toBe(2 * RECIPIENTS);
    for (const r of recipients) {
      const ids = expected.get(r.name)!;
      expect(ids.size).toBe(SENDERS * PER_PAIR);
      const lost = [...ids].filter((id) => (acks.get(id) ?? 0) === 0);
      const duplicated = [...ids].filter((id) => (acks.get(id) ?? 0) > 1);
      expect({ recipient: r.name, lost, duplicated }).toEqual({ recipient: r.name, lost: [], duplicated: [] });
      for (const id of ids) expect([...claimedBy.get(id)!]).toEqual([r.name]);
      expect(await r.client.adapterConnection.llen(`gptq:q:${r.name}`)).toBe(0);
    }
    // No acknowledgement for anything that was never sent.
    const all = new Set(recipients.flatMap((r) => [...expected.get(r.name)!]));
    expect([...acks.keys()].filter((id) => !all.has(id))).toEqual([]);

    // Every sender gets exactly one correlated reply per request, from the intended recipient.
    for (const sender of senders) {
      expect(sender.received).toHaveLength(replyTotal);
      const byRequest = new Map<string, string[]>();
      for (const { message } of sender.received) byRequest.set(message.payload.in_reply_to!, [...(byRequest.get(message.payload.in_reply_to!) ?? []), message.from]);
      expect(byRequest.size).toBe(RECIPIENTS * PER_PAIR);
      for (const [request, from] of byRequest) {
        expect(from).toHaveLength(1);
        const intended = recipients.find((r) => expected.get(r.name)!.has(request))!;
        expect(from[0]).toBe(intended.name);
      }
    }
  }, 60_000);
});
