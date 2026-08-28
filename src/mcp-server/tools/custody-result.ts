import { toolResult, type ToolPayload } from "../tool-result.js";
import type { CustodyOpResult } from "../../core/custody-store.js";

/**
 * Map a custody store operation result to an MCP tool result. Domain failures
 * surface as structured `status: "error"` error results, matching how queue
 * tools report domain errors (e.g. QUEUE_FULL).
 */
export function custodyOpResult(
  result: CustodyOpResult,
  okPayload: ToolPayload = {}
) {
  if (!result.ok) {
    return toolResult(
      {
        status: "error",
        error: { code: result.error.code, message: result.error.message },
      },
      true
    );
  }
  return toolResult({ ...okPayload, status: "ok", record: result.record });
}