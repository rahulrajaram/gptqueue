import { z } from "zod";
import type { RedisClient } from "../redis-client.js";

export const registerAgentSchema = z.object({
  name: z
    .string()
    .describe(
      "Name for this agent. Ask the user to choose one if not already known."
    ),
  role: z
    .enum(["publisher", "consumer", "both"])
    .describe("Role of this agent: publisher, consumer, or both"),
  description: z
    .string()
    .describe(
      "Human-readable description of what this agent does and what it can help with. Other agents use this to decide who to talk to. Be specific."
    ),
});

export async function registerAgent(
  client: RedisClient,
  params: z.infer<typeof registerAgentSchema>
) {
  const resolvedName = await client.register(
    params.role,
    params.name,
    params.description
  );
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(
          {
            status: "registered",
            name: resolvedName,
            role: params.role,
            description: params.description,
          },
          null,
          2
        ),
      },
    ],
  };
}
