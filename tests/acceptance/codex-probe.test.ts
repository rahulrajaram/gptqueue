import { describe, expect, it, vi } from "vitest";
import { mkdir, writeFile } from "node:fs/promises";
import { randomInt } from "node:crypto";
import { join } from "node:path";
import { Redis } from "ioredis";
import {
  artifactRoot, callArgs, enabled, inboxEvents, redisUrl, runId, startCodexPeer, toolCalls,
  trace, waitFor, type Json,
} from "./codex-support.js";

vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });

const asString = (value: unknown): string | null => typeof value === "string" && value.length > 0 ? value : null;
const completedClaim = (rows: readonly Json[], messageId: string) => {
  const claimed = rows.filter((row) => row.stage === "task_claimed" && row.message_id === messageId).map((row) => row.claim_id).filter((v): v is string => typeof v === "string");
  return claimed.some((id) => rows.some((row) => row.stage === "task_acknowledged" && row.claim_id === id));
};

describe.skipIf(!enabled)("real native Codex acceptance probe", () => {
  it("uses automatic readiness, completes a natural task/result exchange, and observes a native child", async () => {
    const root = join(artifactRoot, runId);
    await mkdir(root, { recursive: true });
    const redis = new Redis(redisUrl);
    const receipt: Json = { run_id: runId, database: 15, started_at: new Date().toISOString(), automatic_hook: true, passed: false };
    let sender: Awaited<ReturnType<typeof startCodexPeer>> | undefined;
    let recipient: Awaited<ReturnType<typeof startCodexPeer>> | undefined;
    try {
      sender = await startCodexPeer({ cwd: join(root, "sender") });
      recipient = await startCodexPeer({ cwd: join(root, "recipient") });
      receipt.sender = { thread_id: sender.threadId, agent: sender.agent, ready: sender.ready };
      receipt.recipient = { thread_id: recipient.threadId, agent: recipient.agent, ready: recipient.ready };
      expect(sender.agent).not.toBe(recipient.agent);
      expect(sender.ready.runtime).toMatchObject({ client: "codex", runtime_id: sender.threadId });
      expect(recipient.ready.runtime).toMatchObject({ client: "codex", runtime_id: recipient.threadId });

      const left = randomInt(11, 51);
      const right = randomInt(51, 91);
      const expected = String(left + right);
      const content = `Please add ${left} and ${right}. Return the decimal answer only.`;
      const sent = await sender.tool("send_message", {
        to: recipient.agent,
        type: "task",
        content,
        idempotency_key: `codex-acceptance-${runId}`,
      });
      const sendPayload = sent.structuredContent as Json | undefined;
      const taskId = asString(sendPayload?.message_id);
      expect(sendPayload?.status).toBe("sent");
      expect(taskId).not.toBeNull();
      receipt.task = { content, message_id: taskId, send: sendPayload };

      const delivery = await waitFor(async () => {
        const [recipientEvents, senderEvents, recipientTrace, senderTrace] = await Promise.all([
          inboxEvents(redis, recipient!.agent), inboxEvents(redis, sender!.agent), trace(redis, recipient!.agent), trace(redis, sender!.agent),
        ]);
        return { recipientEvents, senderEvents, recipientTrace, senderTrace };
      }, (value) => {
        const inboundTask = value.recipientEvents.find((row) => row.message_id === taskId && row.from === sender!.agent && row.to === recipient!.agent && row.type === "task");
        const result = value.senderEvents.find((row) => row.from === recipient!.agent && row.to === sender!.agent && row.type === "result" && row.in_reply_to === taskId);
        return inboundTask !== undefined && typeof result?.message_id === 'string' && completedClaim(value.recipientTrace, taskId!) && completedClaim(value.senderTrace, result.message_id);
      }, 240_000, "correlated task/result delivery and both acknowledgements");
      const resultEvent = delivery.senderEvents.find((row) => row.from === recipient!.agent && row.to === sender!.agent && row.type === "result" && row.in_reply_to === taskId)!;
      expect(resultEvent.from).toBe(recipient.agent);
      expect(resultEvent.to).toBe(sender.agent);
      expect(resultEvent.in_reply_to).toBe(taskId);
      const replyCall = toolCalls(await recipient.history()).find((call) => call.tool === "send_message" && callArgs(call)?.in_reply_to === taskId);
      const replyArgs = replyCall ? callArgs(replyCall) : null;
      expect(replyArgs?.to).toBe(sender.agent);
      expect(replyArgs?.type).toBe("result");
      expect(replyArgs?.in_reply_to).toBe(taskId);
      expect(typeof replyArgs?.content).toBe("string");
      expect(String(replyArgs?.content ?? "").trim()).toBe(expected);
      expect(await redis.llen(`gptq:q:${sender.agent}`)).toBe(0);
      expect(await redis.llen(`gptq:q:${recipient.agent}`)).toBe(0);
      receipt.delivery = { task_event: delivery.recipientEvents.find((row) => row.message_id === taskId), result_event: resultEvent, reply_call: replyArgs, recipient_trace: delivery.recipientTrace, sender_trace: delivery.senderTrace };

      // This is an actual native collaboration request from the parent thread.
      // We only inspect the resulting child thread and its own read-only status;
      // the child is not treated as an independent GPTQueue peer automatically.
      const childLeft = randomInt(10, 40);
      const childRight = childLeft + randomInt(1, 21);
      await sender.prompt(`Use your native collaboration tool to spawn one short-lived helper. Ask it to compare ${childLeft} and ${childRight}, wait for its answer, and report the answer. Do not use coordination tools for this observation.`);
      const isSpawn = (call: { tool?: string; receiverThreadIds?: unknown }) => ["spawnAgent", "spawn_agent"].includes(call.tool ?? "") && Array.isArray(call.receiverThreadIds);
      const parentWithChild = await waitFor(() => sender!.history(), (history) => toolCalls(history).some((call) => isSpawn(call) && (call.receiverThreadIds as unknown[]).length > 0), 180_000, "native spawn_agent child observation");
      const spawn = toolCalls(parentWithChild).find((call) => isSpawn(call))!;
      const childIds = (spawn.receiverThreadIds as unknown[]).filter((value): value is string => typeof value === "string");
      const children = [] as Json[];
      for (const childId of childIds) {
        let thread: Json | null = null;
        let status: unknown;
        try { thread = await sender.inspectThread(childId); } catch (error) { throw new Error(`native child thread/read failed for ${childId}: ${String(error)}`); }
        try { status = await sender.inspectTool(childId, "get_runtime_status", {}); } catch (error) { status = { error: String(error) }; }
        expect(thread?.id).toBe(childId);
        const parentThreadId = typeof thread?.parentThreadId === "string" ? thread.parentThreadId : null;
        children.push({ receiver_thread_id: childId, observed_thread_id: thread?.id ?? null, parent_thread_id: parentThreadId, parent_thread_id_match: parentThreadId === null ? null : parentThreadId === sender.threadId, session_id: thread?.sessionId ?? null, source: thread?.source ?? null, runtime_status: status });
      }
      expect(children.length).toBeGreaterThan(0);
      receipt.native_child = { parent_thread_id: sender.threadId, child_threads: children, parent_spawn_call: spawn };
      receipt.passed = true;
    } catch (error) {
      receipt.error = String(error);
      throw error;
    } finally {
      await Promise.all([sender?.close(), recipient?.close()]);
      await writeFile(join(root, "receipt.json"), JSON.stringify(receipt, null, 2) + "\n");
      await redis.quit();
    }
  });
});
