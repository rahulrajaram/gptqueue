import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import { bindSession } from "./session-binding.js";

export const claimDetailsSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  claim_id: z
    .string()
    .min(1)
    .describe("Claim id whose full payloads to return"),
});

/**
 * Fetch one claim's full payloads by id — the recovery read for a caller that
 * has lost or compacted away its claim_tasks output (list_my_claims gives the
 * claim_id and counts; this returns the task strings). Caller-bound: only the
 * claiming session's own claim is returned.
 */
export async function claimDetails(
  client: RedisClient,
  params: z.infer<typeof claimDetailsSchema>
) {
  const { sessionId: session_id } = await bindSession(client, params.session_id);
  const result = await client.taskClaim.get(params.claim_id);
  if (!result.ok) {
    return toolResult(
      { status: "error", error: { code: result.error.code, message: result.error.message } },
      true
    );
  }
  if (result.claim === null) {
    return toolResult(
      { status: "error", error: { code: "unknown_claim", message: `no outstanding claim '${params.claim_id}'` } },
      true
    );
  }
  if (result.claim.session_id !== session_id) {
    return toolResult(
      { status: "error", error: { code: "not_claim_owner", message: `claim '${params.claim_id}' is owned by another session` } },
      true
    );
  }
  return toolResult({
    status: "ok",
    claim: {
      claim_id: result.claim.claim_id,
      actor_id: result.claim.actor_id,
      claimed_at: result.claim.claimed_at,
      expires_at: result.claim.expires_at,
      tasks: result.claim.tasks,
    },
  });
}
