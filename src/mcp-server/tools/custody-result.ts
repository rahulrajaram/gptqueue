import { toolResult } from "../tool-result.js";
import type { CustodyOpResult } from "../../core/custody-store.js";

/**
 * Map a custody store operation result to an MCP tool result. Domain failures
 * surface as structured `status: "error"` error results, matching how queue
 * tools report domain errors (e.g. QUEUE_FULL).
 */
export function custodyOpResult(result: CustodyOpResult) {
  if (!result.ok) {
    return toolResult(
      {
        status: "error",
        error: { code: result.error.code, message: result.error.message },
      },
      true
    );
  }
  // The wire contract is status "ok" plus the record; record.state says
  // whether the worktree is now held, released, and so on.
  return toolResult({ status: "ok", record: result.record });
}