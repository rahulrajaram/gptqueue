import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const listMyClaimsSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
});

/**
 * f-2979: list the calling session's own outstanding (non-expired) claims, so
 * a claim lost from context (e.g. after compaction) can be recovered by id —
 * renewed, replied to, and acknowledged — instead of re-parsed from old tool
 * output. Caller-bound: only this connection's actor and session are listed;
 * no task payloads are returned, only per-claim identity and lease timing.
 * Empty is an explicit empty list, not an error.
 */
export async function listMyClaims(
  client: RedisClient,
  params: z.infer<typeof listMyClaimsSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(client, params.session_id);
  const claims = await client.taskClaim.activeClaimsFor({ actor_id, session_id, now: new Date().toISOString() });
  return toolResult({
    status: "ok",
    count: claims.length,
    claims: claims.map((claim) => ({
      claim_id: claim.claim_id,
      claimed_at: claim.claimed_at,
      expires_at: claim.expires_at,
      task_count: claim.tasks.length,
    })),
  });
}
