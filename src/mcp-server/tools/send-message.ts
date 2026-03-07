import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import type { RedisClient } from "../redis-client.js";
import type { QueueMessage } from "../types.js";

export const sendMessageSchema = z.object({
  to: z.string().describe("Target agent name"),
  content: z.string().describe("Message content"),
  type: z
    .enum(["task", "result", "status", "error", "ping"])
    .default("task")
    .describe("Message type"),
  metadata: z
    .record(z.string(), z.unknown())
    .optional()
    .describe("Optional metadata"),
  in_reply_to: z
    .string()
    .optional()
    .describe("Message ID this is replying to"),
});

export async function sendMessage(
  client: RedisClient,
  params: z.infer<typeof sendMessageSchema>
) {
  const message: QueueMessage = {
    id: uuidv4(),
    from: client.requireRegistered(),
    to: params.to,
    timestamp: new Date().toISOString(),
    type: params.type,
    payload: {
      content: params.content,
      metadata: params.metadata,
      in_reply_to: params.in_reply_to,
    },
  };

  const success = await client.sendMessage(message);

  if (success) {
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify(
            { status: "sent", message_id: message.id, to: params.to },
            null,
            2
          ),
        },
      ],
    };
  } else {
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify(
            {
              status: "failed",
              reason: "Queue full after retries",
              to: params.to,
            },
            null,
            2
          ),
        },
      ],
      isError: true,
    };
  }
}
