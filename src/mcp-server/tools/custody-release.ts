import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { custodyOpResult } from "./custody-result.js";

export const custodyReleaseSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  worktree_path: z
    .string()
    .min(1)
    .describe("Absolute path of the worktree to release"),
  repo_head: z
    .string()
    .min(1)
    .describe(
      "Commit sha at the point of release, recorded in the structured handoff"
    ),
  tracked_tree_state: z
    .enum(["clean", "dirty"])
    .describe("Whether the tracked tree is clean or dirty at release"),
  untracked_inventory: z
    .array(z.string())
    .default([])
    .describe("Untracked files present at release (required when the tree is dirty)"),
  unfinished_work: z
    .string()
    .min(1)
    .describe("Human note describing work left unfinished"),
  next_step: z
    .string()
    .min(1)
    .describe("What the next custodian should do"),
  hazards: z
    .array(z.string())
    .default([])
    .describe("Optional notes about hazards a successor should know"),
  authored_by: z
    .enum(["origin", "successor_reconstructed"])
    .default("origin")
    .describe("Who authored the handoff (default: origin)"),
});

export async function custodyRelease(
  client: RedisClient,
  params: z.infer<typeof custodyReleaseSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const actor_name = client.requireRegistered();
  const session_id = client.sessionId;
  if (!session_id) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const handoff = {
    schema_version: 1,
    authored_by: params.authored_by,
    repo_head: params.repo_head,
    tracked_tree_state: params.tracked_tree_state,
    untracked_inventory: params.untracked_inventory,
    unfinished_work: params.unfinished_work,
    hazards: params.hazards,
    next_step: params.next_step,
  };

  const result = await client.custody.release({
    worktree_path: params.worktree_path,
    actor_name,
    session_id,
    handoff,
  });

  return custodyOpResult(result, { status: "released" });
}