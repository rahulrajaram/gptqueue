import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";

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
  return toolResult({ status: "ok", queues: statuses }, false, statuses);
}
