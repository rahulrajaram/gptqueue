import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { ensureSessionBinding } from "./session-binding.js";
import { custodyOpResult } from "./custody-result.js";

export const custodyClaimSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  worktree_path: z
    .string()
    .min(1)
    .describe("Absolute path of the worktree to claim"),
  repo_head: z
    .string()
    .min(1)
    .describe("Current commit sha of the worktree"),
  tree_fingerprint: z
    .string()
    .min(1)
    .describe(
      "Opaque digest of the worktree's git status, supplied by the caller"
    ),
  lease_seconds: z
    .number()
    .int()
    .min(1)
    .max(86400)
    .describe("Lease duration in whole seconds, between 1 and 86400"),
  inventory: z
    .array(z.string().min(1))
    .optional()
    .describe(
      "Required only when claiming a forfeited worktree (successor takeover): a non-empty list of untracked or reconstructed files"
    ),
});

export async function custodyClaim(
  client: RedisClient,
  params: z.infer<typeof custodyClaimSchema>
) {
  await ensureSessionBinding(client, params.session_id);
  const actor_name = client.requireRegistered();
  const session_id = client.sessionId;
  if (!session_id) {
    throw new Error(
      "Session not bound. Call register_agent first with a name and retain the session_id."
    );
  }

  const result = await client.custody.claim({
    worktree_path: params.worktree_path,
    repo_head: params.repo_head,
    tree_fingerprint: params.tree_fingerprint,
    lease_seconds: params.lease_seconds,
    inventory: params.inventory,
    actor_name,
    session_id,
    now: new Date().toISOString(),
  });

  return custodyOpResult(result, { status: "claimed" });
}