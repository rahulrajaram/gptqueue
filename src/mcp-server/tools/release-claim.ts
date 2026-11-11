import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { releaseClaimResult } from "./task-claim-result.js";

export const releaseClaimSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  claim_id: z
    .string()
    .min(1)
    .describe("Claim id returned by claim_tasks to release"),
});

/**
 * Release one of your own outstanding claims: its tasks are put back on your
 * inbox so they can be claimed again, and the claim is removed. Only the
 * claiming session may release its own claim. Recovery counters increment
 * exactly as lazy recovery does, so a message that keeps being released is
 * dead-lettered once it exceeds the cap.
 */
export async function releaseClaim(
  client: RedisClient,
  params: z.infer<typeof releaseClaimSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(
    client,
    params.session_id
  );
  const result = await client.taskClaim.release({
    claim_id: params.claim_id,
    actor_id,
    session_id,
  });
  return releaseClaimResult(result);
}
