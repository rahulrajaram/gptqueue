import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import type { RedisClient } from "../redis-client.js";
import type { QueueMessage } from "../types.js";
import { ensureSessionBinding } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const sendMessageSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
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
  idempotency_key: z
    .string()
    .min(1)
    .max(128)
    .optional()
    .describe(
      "Caller-generated key for retry safety. Reusing the same key for the same sender returns the original message_id without enqueueing a duplicate."
    ),
});

export async function sendMessage(
  client: RedisClient,
  params: z.infer<typeof sendMessageSchema>
) {
  await ensureSessionBinding(client, params.session_id);

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

  const outcome = params.idempotency_key
    ? await client.sendMessageIdempotent(message, params.idempotency_key)
    : { status: (await client.sendMessage(message)) ? "sent" as const : "full" as const, messageId: message.id };

  if (outcome.status !== "full") {
    return toolResult({
      status: outcome.status,
      message_id: outcome.messageId,
      to: params.to,
      deduplicated: outcome.status === "duplicate",
    });
  } else {
    return toolResult({
      status: "error",
      error: {
        code: "QUEUE_FULL",
        message: "Queue full after retries",
        retryable: true,
      },
      to: params.to,
    }, true);
  }
}
