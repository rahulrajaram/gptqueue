import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { renewClaimResult } from "./task-claim-result.js";
import { DLQ_PROVISIONAL } from "../../core/keys.js";

export const renewClaimSchema = z.object({
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
      "Claim id returned by claim_tasks whose expiry to extend"
    ),
  ttl_seconds: z
    .number()
    .int()
    .min(1)
    .max(3600)
    .default(300)
    .describe(
      "Lease extension in whole seconds, between 1 and 3600 (default 300), applied from the renew instant"
    ),
});

/**
 * Renew an outstanding claim_id returned by claim_tasks, extending its expiry
 * by `ttl_seconds`. Only the claiming session (matching actor_id and
 * session_id) may renew its own claim. The extension is applied from the
 * renew instant but capped by the claim's provisional lifetime budget
 * (DLQ_PROVISIONAL.CLAIM_LIFETIME_BUDGET_SECONDS) rendered from its
 * claimed_at, so an endlessly-renewing runtime cannot hold a batch forever.
 * A foreign session is `not_claim_owner`, a post-expiry renewal is
 * `claim_expired`, and budget exhaustion is `budget_exceeded`.
 */
export async function renewClaim(
  client: RedisClient,
  params: z.infer<typeof renewClaimSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(
    client,
    params.session_id
  );

  const result = await client.taskClaim.renew({
    claim_id: params.claim_id,
    actor_id,
    session_id,
    ttl_seconds: params.ttl_seconds,
    budget_seconds: DLQ_PROVISIONAL.CLAIM_LIFETIME_BUDGET_SECONDS,
    now: new Date().toISOString(),
  });

  return renewClaimResult(result);
}