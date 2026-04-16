import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";

export const receiveMessageSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  timeout: z
    .number()
    .default(5)
    .describe("Blocking timeout in seconds (default 5)"),
});

export async function receiveMessage(
  client: RedisClient,
  params: z.infer<typeof receiveMessageSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const message = await client.receiveMessage(params.timeout);

  if (message) {
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify(message, null, 2),
        },
      ],
    };
  } else {
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify({ status: "no_messages", timeout: params.timeout }),
        },
      ],
    };
  }
}
