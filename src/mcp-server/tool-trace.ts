/**
 * Inbox trace stages for the consume/reply tools (AC-3/LT-0 parity): every
 * client's claim_tasks, acknowledge_tasks and correlated send_message write
 * the same gptq:inbox-trace stages and fields, through InboxEvents.trace.
 * Tracing never fails the tool call (InboxEvents.trace swallows errors).
 */
import { InboxEvents } from "../core/inbox-events.js";

const now = () => new Date().toISOString();

/** task_claimed per task in a claim payload (tasks are serialized envelopes). */
export const traceClaimed = async (events: InboxEvents, agent: string, claim: Record<string, unknown>): Promise<void> => {
  const tasks = Array.isArray(claim.tasks) ? claim.tasks : [];
  for (const raw of tasks) {
    let task: unknown;
    try { task = typeof raw === "string" ? JSON.parse(raw) : raw; } catch { continue; }
    if (task && typeof task === "object" && "id" in task && typeof task.id === "string") {
      await events.trace(agent, { stage: "task_claimed", timestamp: now(), claim_id: String(claim.claim_id), message_id: task.id });
    }
  }
};

export const traceAcknowledged = (events: InboxEvents, agent: string, claimId: string): Promise<void> =>
  events.trace(agent, { stage: "task_acknowledged", timestamp: now(), claim_id: claimId });

/**
 * message_acknowledged per message a subset ack (acknowledge_messages)
 * settled. Never task_acknowledged: that stage means the WHOLE claim was
 * acknowledged, and diagnostics read it that way.
 */
export const traceMessagesAcknowledged = async (
  events: InboxEvents, agent: string, claimId: string, messageIds: readonly string[],
): Promise<void> => {
  const at = now();
  for (const id of messageIds) {
    await events.trace(agent, { stage: "message_acknowledged", timestamp: at, claim_id: claimId, message_id: id });
  }
};

/** reply_sent for a sent result/error message. */
export const traceReplySent = async (
  events: InboxEvents,
  agent: string,
  payload: Record<string, unknown> | undefined,
  params: Readonly<{ type?: string; in_reply_to?: string }>,
): Promise<void> => {
  if (payload?.status === "sent" && (params.type === "result" || params.type === "error")) {
    await events.trace(agent, { stage: "reply_sent", timestamp: now(), message_id: String(payload.message_id), in_reply_to: params.in_reply_to });
  }
};
