import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { claimTasksResult } from "./task-claim-result.js";
import { toolResult } from "../tool-result.js";

export const claimTasksSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  max_batch: z
    .number()
    .int()
    .min(1)
    .max(16)
    .default(1)
    .describe(
      "Maximum number of tasks to claim in one batch, between 1 and 16 (default 1)"
    ),
  ttl_seconds: z
    .number()
    .int()
    .min(1)
    .max(3600)
    .default(300)
    .describe(
      "Claim lease duration in whole seconds, between 1 and 3600 (default 300)"
    ),
});

/**
 * Atomically claim up to max_batch messages from the caller's own durable
 * inbox as an at-least-once delivery batch for the calling session. Claiming
 * is from your own inbox (actor_id = the registered agent name). An empty
 * inbox yields an explicit empty-batch result (claim:null), not an error.
 * Lazy recovery of any expired unacked claim runs first.
 *
 * Durable actors (those with an ActorDirectory record) claim under their
 * admitted `max_concurrency`: enforcement is atomic in the claim script, so
 * once an actor has that many outstanding unacked claims, a further claim is
 * refused with a `concurrency_limit_reached` error. Plain agents (no directory
 * record) claim without any ceiling (unlimited), preserving historical
 * behavior.
 */
export async function claimTasks(
  client: RedisClient,
  params: z.infer<typeof claimTasksSchema>
) {
  const { agent: actor_id, sessionId: session_id } = await bindSession(
    client,
    params.session_id
  );

  // Durable actors carry an admitted concurrency ceiling; plain agents pass
  // undefined (unlimited), which is exactly the pre-enforcement behavior.
  let max_concurrent_claims: number | undefined;
  const dir = await client.actorDirectory.get(actor_id);
  if (dir.ok && dir.record !== null) {
    // H4 invariant guard: a durable directory record, if present for the
    // caller's name, must carry the SAME actor_id as the caller name. Since
    // actor_register derives actor_id from the registered name, a mismatch
    // is impossible by construction; this is a defensive assertion that fails
    // closed (no claim issued) rather than silently operating under a bruised
    // identity binding.
    if (dir.record.profile.actor_id !== actor_id) {
      return toolResult(
        {
          status: "error",
          error: {
            code: "identity_mismatch",
            message: `directory record for '${actor_id}' carries actor_id '${dir.record.profile.actor_id}'; refusing to claim under a mismatched identity`,
          },
        },
        true
      );
    }
    max_concurrent_claims = dir.record.profile.max_concurrency;
  }

  const result = await client.taskClaim.claim({
    actor_id,
    session_id,
    max_batch: params.max_batch,
    ttl_seconds: params.ttl_seconds,
    ...(max_concurrent_claims === undefined
      ? {}
      : { max_concurrent_claims }),
    now: new Date().toISOString(),
  });

  return claimTasksResult(result);
}