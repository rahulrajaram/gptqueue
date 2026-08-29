import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { bindSession } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const closeSessionSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
});

export async function closeSession(
  client: RedisClient,
  params: z.infer<typeof closeSessionSchema>
) {
  // bindSession validates that the call is bound to a durable session before
  // closeCurrentSession tears local state down, so a lost session surfaces the
  // same SESSION_UNAVAILABLE code as the other session-bound tools.
  await bindSession(client, params.session_id);
  const name = await client.closeCurrentSession();
  return toolResult({
            status: "session_closed",
            agent: name,
            mailbox_preserved: true,
  });
}
