import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const unregisterAgentSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
});

export async function unregisterAgent(
  client: RedisClient,
  params: z.infer<typeof unregisterAgentSchema>
) {
  const { agent: name } = await bindSession(client, params.session_id);
  await client.unregister();
  return toolResult(
    {
      status: "unregistered",
      agent: name,
      mailbox_deleted: true,
    },
    false,
    `Agent "${name}" unregistered and cleaned up`
  );
}
