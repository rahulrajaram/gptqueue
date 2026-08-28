import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";

export const custodyStatusSchema = z.object({
  worktree_path: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Absolute path of one worktree to inspect; omit to list every stored custody record"
    ),
});

export async function custodyStatus(
  client: RedisClient,
  params: z.infer<typeof custodyStatusSchema>
) {
  const result = await client.custody.status({
    worktree_path: params.worktree_path,
    now: new Date().toISOString(),
  });

  if (!result.ok) {
    return toolResult(
      {
        status: "error",
        error: { code: result.error.code, message: result.error.message },
      },
      true
    );
  }
  return "record" in result
    ? toolResult({ status: "ok", record: result.record })
    : toolResult({ status: "ok", records: result.records });
}