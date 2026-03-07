import type { RedisClient } from "../redis-client.js";

export async function unregisterAgent(client: RedisClient) {
  await client.unregister();
  return {
    content: [
      {
        type: "text" as const,
        text: `Agent "${client.agentName}" unregistered and cleaned up`,
      },
    ],
  };
}
