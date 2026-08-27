import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { toolResult } from "../tool-result.js";

export const receiveMessageSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  timeout: z
    .number()
    .int()
    .min(0)
    .max(60)
    .default(5)
    .describe("Blocking timeout in whole seconds (default 5; range 0-60)"),
});

export async function receiveMessage(
  client: RedisClient,
  params: z.infer<typeof receiveMessageSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const message = await client.receiveMessage(params.timeout);

  if (message) {
    return toolResult({ status: "message", message }, false, message);
  } else {
    return toolResult({ status: "no_messages", timeout: params.timeout });
  }
}
