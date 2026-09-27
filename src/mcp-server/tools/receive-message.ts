import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const receiveMessageSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  timeout: z
    .number()
    .int()
    .min(0)
    .max(60)
    .default(5)
    .describe("Blocking timeout in whole seconds (default 5; range 0-60; 0 returns immediately)"),
});

/**
 * Typed domain error for durable actors attempting the legacy destructive pop.
 * Durable actors own a directory record and must consume at-least-once via
 * claim_tasks / acknowledge_tasks; receive_message would BLPOP (at-most-once).
 * Shape matches the repo's typed-error result convention (status/error/isError).
 */
function durableActorClaimRequired(name: string) {
  return Object.freeze(
    toolResult(
      {
        status: "error",
        error: {
          code: "durable_actor_claim_required",
          message: `Agent '${name}' has a durable actor-directory record; consume its inbox with claim_tasks and acknowledge the returned claim with acknowledge_tasks instead of receive_message`,
        },
      },
      true
    )
  );
}

export async function receiveMessage(
  client: RedisClient,
  params: z.infer<typeof receiveMessageSchema>,
  signal?: AbortSignal
) {
  await ensureSessionBinding(client, params.session_id);
  const name = client.requireRegistered();

  // Gate: a durable actor owns a directory record and must claim at-least-once.
  const dir = await client.actorDirectory.get(name);
  if (dir.ok && dir.record !== null) {
    return durableActorClaimRequired(name);
  }

  const message = await client.receiveMessage(params.timeout, signal);

  if (message) {
    return toolResult({ status: "message", message }, false, message);
  } else {
    return toolResult({ status: "no_messages", timeout: params.timeout });
  }
}
