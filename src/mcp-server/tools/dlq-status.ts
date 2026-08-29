import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { dlqStatusResult } from "./task-claim-result.js";

export const dlqStatusSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  limit: z
    .number()
    .int()
    .min(1)
    .max(1000)
    .default(50)
    .describe(
      "Maximum number of DLQ entries to return, newest first (default 50)"
    ),
});

/**
 * List the calling agent's dead-letter queue (DLQ), newest first. A message
 * lands on the DLQ when lazy recovery has re-queued it more than
 * RECOVER_CAP times (a provisional policy constant) without a successful
 * acknowledge. Each entry carries the raw payload plus the decoded message
 * id and timestamp. Empty DLQ is an explicit empty list, not an error.
 */
export async function dlqStatus(
  client: RedisClient,
  params: z.infer<typeof dlqStatusSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const actor_id = client.requireRegistered();
  if (!client.sessionId) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const result = await client.taskClaim.deadLetterEntries({
    actor_id,
    limit: params.limit,
  });

  return dlqStatusResult(result);
}