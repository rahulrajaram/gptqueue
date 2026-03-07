import { z } from "zod";
import type { RedisClient } from "../redis-client.js";

export const receiveMessageSchema = z.object({
  timeout: z
    .number()
    .default(5)
    .describe("Blocking timeout in seconds (default 5)"),
});

export async function receiveMessage(
  client: RedisClient,
  params: z.infer<typeof receiveMessageSchema>
) {
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
