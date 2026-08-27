import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";

export async function listAgents(client: RedisClient) {
  const agents = await client.listAgents();
  return toolResult({ status: "ok", agents }, false, agents);
}
