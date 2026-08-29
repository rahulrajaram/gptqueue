import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { dlqRequeueResult } from "./task-claim-result.js";

export const dlqRequeueSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  message_id: z
    .string()
    .min(1)
    .describe(
      "Message id of a DLQ entry (from dlq_status) to move back to your own inbox tail"
    ),
});

/**
 * Move one dead-lettered message from the caller's own DLQ back to its inbox
 * tail, restoring a fresh recovery budget (its recovery counter is cleared).
 * The matching DLQ entry must exist; otherwise a structured
 * `dlq_entry_not_found` error is returned.
 */
export async function dlqRequeue(
  client: RedisClient,
  params: z.infer<typeof dlqRequeueSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const actor_id = client.requireRegistered();
  if (!client.sessionId) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const result = await client.taskClaim.requeue({
    actor_id,
    message_id: params.message_id,
  });

  return dlqRequeueResult(result);
}