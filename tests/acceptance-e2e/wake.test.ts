/**
 * WK-1 / DL-5, WK-2, WK-3, WK-4, WK-6 end to end, per runtime, with no human input.
 * See tests/acceptance-e2e/README.md and harness.ts for the fake hosts.
 */
import { afterEach, describe, expect, it } from "vitest";
import { checkExchangeEvidence } from "../acceptance/oracle.js";
import {
  ALL_RUNTIMES, expectedAnswer, startRecipient, startSender, taskContent, traceConsumption, waitFor,
  type Recipient, type Runtime, type Sender,
} from "./harness.js";

const open: Array<{ close(): Promise<void> }> = [];
afterEach(async () => { for (const item of open.splice(0).reverse()) await item.close().catch(() => undefined); });

const setup = async (runtime: Runtime): Promise<{ sender: Sender; recipient: Recipient }> => {
  const sender = await startSender(`to-${runtime}`); open.push(sender);
  const recipient = await startRecipient(runtime); open.push(recipient);
  return { sender, recipient };
};
/** Wait until the sender holds a reply for every request id, draining (claim + ack) as replies land. */
const awaitReplies = async (sender: Sender, ids: readonly string[], timeoutMs = 15_000) => {
  await waitFor(async () => {
    await sender.drain();
    const answered = new Set(sender.received.map(({ message }) => message.payload.in_reply_to));
    return ids.every((id) => answered.has(id));
  }, timeoutMs, `${ids.length} correlated replies`);
};

describe.each(ALL_RUNTIMES)("%s recipient", (runtime) => {
  it("WK-1/DL-5: an idle recipient claims, replies with exact in_reply_to and acks, unprompted", async () => {
    const { sender, recipient } = await setup(runtime);
    const request = await sender.send(recipient.name(), taskContent(20, 22));
    await awaitReplies(sender, [request.message_id]);
    const handled = recipient.handled.find(({ message }) => message.id === request.message_id)!;
    const reply = sender.received.find(({ message }) => message.payload.in_reply_to === request.message_id)!;
    // Consumption comes from server state (inbox trace rows), not from what the harness believes it did.
    const requestTrace = await traceConsumption(recipient.name(), request.message_id);
    const replyTrace = await traceConsumption(sender.name, reply.message.id);
    expect(requestTrace.claim_id).toBe(handled.claim_id);
    expect(replyTrace.claim_id).toBe(reply.claim_id);
    const consumption = (messageId: string, actor: string, trace: typeof requestTrace) => trace.claim_id === undefined ? undefined
      : { message_id: messageId, actor, consumed: true, claim_id: trace.claim_id, acknowledged: trace.acknowledged };
    const verdict = checkExchangeEvidence({
      sender: sender.name, recipient: recipient.name(),
      request: { id: request.message_id, from: handled.message.from, to: handled.message.to },
      reply: { id: reply.message.id, from: reply.message.from, to: reply.message.to, in_reply_to: reply.message.payload.in_reply_to, content: reply.message.payload.content },
      request_consumption: consumption(handled.message.id, recipient.name(), requestTrace),
      reply_consumption: consumption(reply.message.id, sender.name, replyTrace),
      expected_reply_content: expectedAnswer(20, 22),
      execution: { status: "completed" },
    });
    expect(verdict.reasons).toEqual([]);
    expect(verdict.outcome).toBe("meets");
    expect(handled.reply_id).toBe(reply.message.id);
    expect(recipient.handled.filter(({ message }) => message.id === request.message_id)).toHaveLength(1);
  });

  it("WK-2: a task arriving mid-turn is handled in the next turn", async () => {
    const { sender, recipient } = await setup(runtime);
    recipient.hold();
    const request = await sender.send(recipient.name(), taskContent(1, 2));
    // Nothing is handled while the (user) turn is in progress.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(recipient.handled).toHaveLength(0);
    const turnsBefore = recipient.turnStarts.length;
    const injectionsBefore = recipient.injections.length;
    const endedAt = await recipient.release();
    await awaitReplies(sender, [request.message_id]);
    const handled = recipient.handled.find(({ message }) => message.id === request.message_id)!;
    expect(handled.at).toBeGreaterThanOrEqual(endedAt);
    // Handled in a new model turn that began after the busy turn ended.
    expect(recipient.turnStarts.length).toBeGreaterThan(turnsBefore);
    expect(recipient.turnStarts.at(-1)!).toBeGreaterThanOrEqual(endedAt);
    if (runtime === "claude") {
      // The busy turn's own Stop hook must continue the session (decision: block), not the idle watcher.
      expect(recipient.injectionVia!.slice(injectionsBefore)[0]).toBe("stop_hook");
    }
    expect(sender.received.find(({ message }) => message.payload.in_reply_to === request.message_id)!.message.payload.content).toBe(expectedAnswer(1, 2));
  });

  it("WK-3: a result for the recipient's outstanding work gives it a turn", async () => {
    const { sender: peer, recipient } = await setup(runtime);
    const outstanding = await recipient.send(peer.name, taskContent(5, 5));
    await waitFor(async () => (await peer.drain()).length > 0 || peer.received.length > 0, 5_000, "peer receives outstanding task");
    const task = peer.received.find(({ message }) => message.id === outstanding)!;
    expect(task.message.from).toBe(recipient.name());
    const turnsBefore = recipient.injections.length;
    const result = await peer.send(recipient.name(), "sum=10", { type: "result", in_reply_to: outstanding, idempotency_key: `r-${outstanding}` });
    await waitFor(() => recipient.handled.some(({ message }) => message.id === result.message_id), 10_000, "result handled");
    expect(recipient.injections.length).toBeGreaterThan(turnsBefore);
    const handled = recipient.handled.find(({ message }) => message.id === result.message_id)!;
    expect(handled.message.payload.in_reply_to).toBe(outstanding);
    expect(handled.acknowledged).toBe(true);
    // A reply to a reply is never sent automatically.
    expect(handled.reply_id).toBeUndefined();
  });

  it("WK-4: a burst of 20 tasks is fully handled within a bounded number of activations", async () => {
    const { sender, recipient } = await setup(runtime);
    const before = recipient.injections.length;
    const sent = await Promise.all(Array.from({ length: 20 }, (_, i) => sender.send(recipient.name(), taskContent(i, 100))));
    await awaitReplies(sender, sent.map(({ message_id }) => message_id), 20_000);
    const activations = recipient.injections.length - before;
    // Each turn drains up to 16 per claim until empty; the dispatcher may add at most
    // one empty turn per claim race. 20 tasks must not cost one activation each.
    expect(activations).toBeGreaterThanOrEqual(1);
    expect(activations).toBeLessThanOrEqual(4);
    const ids = recipient.handled.map(({ message }) => message.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const [i, { message_id }] of sent.entries()) {
      expect(sender.received.find(({ message }) => message.payload.in_reply_to === message_id)!.message.payload.content).toBe(expectedAnswer(i, 100));
    }
  }, 30_000);

  it("WK-6: restarting the dispatcher/sidecar mid-stream strands nothing", async () => {
    const { sender, recipient } = await setup(runtime);
    const first = await Promise.all(Array.from({ length: 5 }, (_, i) => sender.send(recipient.name(), taskContent(i, 1))));
    await waitFor(() => recipient.handled.length >= 1, 10_000, "first batch underway");
    const restart = recipient.restart();
    // Arrivals during the restart window.
    const during = await Promise.all(Array.from({ length: 5 }, (_, i) => sender.send(recipient.name(), taskContent(i, 2))));
    await restart;
    const after = await Promise.all(Array.from({ length: 5 }, (_, i) => sender.send(recipient.name(), taskContent(i, 3))));
    const all = [...first, ...during, ...after].map(({ message_id }) => message_id);
    await awaitReplies(sender, all, 25_000);
    const acked = recipient.handled.filter(({ acknowledged }) => acknowledged).map(({ message }) => message.id);
    expect(new Set(acked)).toEqual(new Set(all));
    expect(acked).toHaveLength(all.length);
  }, 40_000);
});
