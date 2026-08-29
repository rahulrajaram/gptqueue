import { toolResult } from "../tool-result.js";
import type {
  ClaimResult,
  AcknowledgeResult,
  RenewResult,
  DeadLetterEntriesResult,
  RequeueResult,
} from "../../core/task-claim-store.js";

/**
 * Map a TaskClaimStore claim result to an MCP tool result. A pending claim
 * surfaces the full claim payload (claim_id, tasks, expires_at, plus owning
 * identities); an empty inbox is an explicit empty-batch indication, NOT an
 * error. Domain failures surface as structured `status: "error"` results.
 */
export function claimTasksResult(result: ClaimResult) {
  if (!result.ok) {
    return errorResult(result);
  }
  if (result.claim === null) {
    return toolResult({ status: "ok", claimed: false, claim: null });
  }
  return toolResult({
    status: "ok",
    claimed: true,
    claim: {
      claim_id: result.claim.claim_id,
      actor_id: result.claim.actor_id,
      session_id: result.claim.session_id,
      claimed_at: result.claim.claimed_at,
      expires_at: result.claim.expires_at,
      tasks: result.claim.tasks,
    },
  });
}

/** Map a TaskClaimStore acknowledge result to an MCP tool result. */
export function acknowledgeTasksResult(result: AcknowledgeResult) {
  if (!result.ok) {
    return errorResult(result);
  }
  return toolResult({ status: "ok", acknowledged: result.acknowledged });
}

/** Map a TaskClaimStore renew result to an MCP tool result. */
export function renewClaimResult(result: RenewResult) {
  if (!result.ok) {
    return errorResult(result);
  }
  return toolResult({
    status: "ok",
    claim_id: result.claim_id,
    expires_at: result.expires_at,
  });
}

/** Map a TaskClaimStore dead-letter queue listing result to an MCP tool result. */
export function dlqStatusResult(result: DeadLetterEntriesResult) {
  if (!result.ok) {
    return errorResult(result);
  }
  return toolResult({
    status: "ok",
    entries: result.entries,
    count: result.entries.length,
  });
}

/** Map a TaskClaimStore DLQ requeue result to an MCP tool result. */
export function dlqRequeueResult(result: RequeueResult) {
  if (!result.ok) {
    return errorResult(result);
  }
  return toolResult({ status: "ok", requeued: result.requeued });
}

function errorResult(result: { error: { code: string; message: string } }) {
  return toolResult(
    {
      status: "error",
      error: { code: result.error.code, message: result.error.message },
    },
    true
  );
}