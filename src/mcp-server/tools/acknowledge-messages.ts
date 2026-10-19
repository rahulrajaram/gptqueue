import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ACK_MESSAGES_MAX } from "../../core/task-claim-store.js";
import { bindSession } from "./session-binding.js";
import { acknowledgeMessagesResult } from "./task-claim-result.js";

/**
 * Strict on purpose: an unknown field is refused, never stripped. This is a
 * separate tool (not a `message_ids` field on acknowledge_tasks) because the
 * acknowledge_tasks schema is not strict, so an older server would drop the
 * selector and acknowledge the WHOLE claim.
 */
export const acknowledgeMessagesSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  claim_id: z
    .string()
    .min(1)
    .describe("Claim id returned by claim_tasks that holds the messages"),
  message_ids: z
    .array(z.string().min(1))
    .min(1)
    .max(ACK_MESSAGES_MAX)
    .describe(
      `1-${ACK_MESSAGES_MAX} distinct ids of messages in that claim to acknowledge; the rest stay claimed with the same expiry`
    ),
}).strict();

/**
 * Acknowledge selected messages of a claim held by the calling session. The
 * remainder stays in the same claim; a claim emptied this way is deleted, as
 * by acknowledge_tasks. Refusals write nothing.
 */
export async function acknowledgeMessages(
  client: RedisClient,
  params: z.infer<typeof acknowledgeMessagesSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(
    client,
    params.session_id
  );
  const result = await client.taskClaim.acknowledgeMessages({
    claim_id: params.claim_id,
    actor_id,
    session_id,
    message_ids: params.message_ids,
  });
  return acknowledgeMessagesResult(result);
}
