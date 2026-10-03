/**
 * Workload derivation for presence assembly.
 *
 * Both `actor_status` and `send_message`'s wake-on-send gate assemble a
 * `RuntimeIncarnation` from a live session. Since this slice added durable
 * task claims, workload is observation-based: a runtime holding an unacked
 * claim is "processing" (it has accepted a batch of work in flight); all
 * other live sessions are "idle". Importing this from both sites keeps the two
 * assembly paths identical without changing actor_status's result shape.
 */

import type { TaskClaimStore } from "../../core/task-claim-store.js";

/**
 * Return the workload to attribute to a live session, based on whether the
 * observed runtime currently holds a non-expired unacked claim. Best-effort:
 * any store error collapses to "idle" so presence assembly and wake gating can
 * never fail because of workload derivation.
 */
export async function workloadForSession(
  claims: TaskClaimStore,
  actorId: string,
  sessionId: string
): Promise<"processing" | "idle"> {
  try {
    const active = await claims.activeClaimFor({
      actor_id: actorId,
      session_id: sessionId,
      now: new Date().toISOString(),
    });
    return active !== null ? "processing" : "idle";
  } catch {
    return "idle";
  }
}