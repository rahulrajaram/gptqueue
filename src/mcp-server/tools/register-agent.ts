import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import { AGENT_NAME_PATTERN } from "../../core/session-store.js";

export const registerAgentSchema = z.object({
  name: z
    .string()
    .min(1)
    .max(128)
    .regex(AGENT_NAME_PATTERN, "Agent name must be 1-128 characters from letters, digits, '.', '_' or '-'.")
    .describe(
      "Name for this agent. Ask the user to choose one if not already known."
    ),
  role: z
    .enum(["publisher", "consumer", "both"])
    .describe("Role of this agent: publisher, consumer, or both"),
  description: z
    .string()
    .describe(
      "Human-readable description of what this agent does and what it can help with. Other agents use this to decide who to talk to. Be specific."
    ),
  shared: z
    .boolean()
    .optional()
    .describe(
      "Opt into sharing this name with other live sessions that also opted in. Names are exclusive by default: registering a name a live session holds fails with AGENT_NAME_HELD. Defaults to GPTQ_AGENT_SHARED=1 when set."
    ),
});

/**
 * Best-effort runtime_ready side effect: after a runtime registers under the
 * same durable actor id, any still-outstanding wake lease for that actor is
 * cleared. A clear failure must NOT fail registration, so errors are swallowed.
 */
async function clearOutstandingWakeLease(
  client: RedisClient,
  actorId: string
): Promise<void> {
  try {
    const lease = await client.wakeLease.get(actorId);
    if (lease !== null) {
      await client.wakeLease.clear({ actor_id: actorId, lease_id: lease.lease_id });
    }
  } catch {
    // Best-effort only: the launched runtime is now registered and ready, so
    // registration must succeed even if the lease clear is delayed/raced.
  }
}

export async function registerAgent(
  client: RedisClient,
  params: z.infer<typeof registerAgentSchema>
) {
  const result = await client.register(
    params.role,
    params.name,
    params.description,
    undefined,
    params.shared === undefined ? {} : { shared: params.shared }
  );
  await clearOutstandingWakeLease(client, result.name);
  return toolResult({
            status: "registered",
            name: result.name,
            session_id: result.session_id,
            role: params.role,
            description: params.description,
  });
}
