import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { claimTasksResult } from "./task-claim-result.js";

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
  await ensureSessionBinding(client, params.session_id);
  const actor_id = client.requireRegistered();
  const session_id = client.sessionId;
  if (!session_id) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  // Durable actors carry an admitted concurrency ceiling; plain agents pass
  // undefined (unlimited), which is exactly the pre-enforcement behavior.
  let max_concurrent_claims: number | undefined;
  const dir = await client.actorDirectory.get(actor_id);
  if (dir.ok && dir.record !== null) {
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