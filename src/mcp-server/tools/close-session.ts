import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";

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
  await ensureSessionBinding(client, params.session_id);
  const name = await client.closeCurrentSession();
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(
          {
            status: "session_closed",
            agent: name,
            mailbox_preserved: true,
          },
          null,
          2
        ),
      },
    ],
  };
}
