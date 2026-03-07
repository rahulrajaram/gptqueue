import type { RedisClient } from "../redis-client.js";

export async function listAgents(client: RedisClient) {
  const agents = await client.listAgents();
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(agents, null, 2),
      },
    ],
  };
}
