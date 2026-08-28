import { toolResult, type ToolPayload } from "../tool-result.js";
import type { ActorDirectoryResult } from "../../core/actor-directory.js";

/**
 * Map an actor directory registration result to an MCP tool result. Domain
 * failures surface as structured `status: "error"` error results, matching how
 * queue and custody tools report domain errors.
 */
export function actorRegisterResult(
  result: ActorDirectoryResult,
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