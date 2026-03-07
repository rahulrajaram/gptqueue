import { z } from "zod";
import type { RedisClient } from "../redis-client.js";

export const queueStatusSchema = z.object({
  agent: z
    .string()
    .optional()
    .describe("Agent name to check (omit for all agents)"),
});

export async function getQueueStatus(
  client: RedisClient,
  params: z.infer<typeof queueStatusSchema>
) {
  const statuses = await client.getQueueStatus(params.agent);
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(statuses, null, 2),
      },
    ],
  };
}
