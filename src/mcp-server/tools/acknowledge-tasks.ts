import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { acknowledgeTasksResult } from "./task-claim-result.js";

export const acknowledgeTasksSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  claim_id: z
    .string()
    .min(1)
    .describe(
      "Claim id returned by claim_tasks, confirming delivery of that batch"
    ),
});

/**
 * Acknowledge a claim_id returned by claim_tasks, confirming same-runtime
 * batch delivery. Only the claiming session (matching actor_id and session_id)
 * may acknowledge its own claim; acknowledged tasks are removed so they are
 * not re-delivered. A second ack is `unknown_claim`; a foreign session is
 * `not_claim_owner`.
 */
export async function acknowledgeTasks(
  client: RedisClient,
  params: z.infer<typeof acknowledgeTasksSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const actor_id = client.requireRegistered();
  const session_id = client.sessionId;
  if (!session_id) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const result = await client.taskClaim.acknowledge({
    claim_id: params.claim_id,
    actor_id,
    session_id,
  });

  return acknowledgeTasksResult(result);
}