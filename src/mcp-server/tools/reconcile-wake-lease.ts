/**
 * Pid-liveness reconciliation of unknown activation outcomes.
 *
 * When an activation is dispatched, the wake lease records the spawned pid. If
 * the launched runtime never registers (no runtime session ever appears), the
 * lease pins the actor in `starting` until its TTL lapses — an indefinite
 * "activation in flight" even though the process may have died without ever
 * registering. This helper resolves that `activation_unknown` observationally:
 * a lease that carries a `spawned_pid` which is no longer alive is a failed
 * activation, so the lease is cleared (best-effort, matched on its own id).
 * Once cleared, the caller's presence assembly classifies from offline state
 * and re-wake becomes possible. There is no blind re-wake here and no
 * indefinite `starting`: the resolution is based strictly on observed process
 * death.
 *
 * Used by BOTH presence-assembly sites (actor-status.ts and
 * send-message.ts's maybeWake). The caller is responsible for the
 * reconcile → get-lease-again → classify ordering: when `cleared` is true the
 * lease is gone, so the caller treats presence as having no wake_lease_id.
 */

import type { RedisClient } from "../redis-client.js";
import { isPidAlive } from "../launcher.js";

/** Liveness of a wake lease's spawn evidence, computed after reconciliation. */
export type PidLiveness = "alive" | "dead" | "unknown";

export interface ReconcileOutcome {
  /**
   * Whether the reconcile cleared the actor's wake lease because its spawned
   * process died. When true, the lease is gone and presence must assemble
   * from offline (no wake_lease_id).
   */
  readonly cleared: boolean;
  /**
   * Observation of the lease's spawn evidence. `alive` = live spawned_pid;
   * `dead` = spawned_pid present but the process is gone (this is what clears
   * the lease); `unknown` = lease without spawned_pid (no spawn evidence to
   * probe); `undefined` = no lease at all.
   */
  readonly pid_liveness: PidLiveness | undefined;
}

/**
 * Reconcile an actor's outstanding wake lease against process liveness,
 * clearing a dead-pid lease so the actor can return to offline and be
 * re-woken. Best-effort: a failure to read or clear the lease never throws and
 * never blocks presence assembly.
 */
export async function reconcileWakeLease(
  client: Pick<RedisClient, "wakeLease">,
  actorId: string
): Promise<ReconcileOutcome> {
  let lease;
  try {
    lease = await client.wakeLease.get(actorId);
  } catch {
    // A failed read must not poison presence assembly; treat as no lease.
    return { cleared: false, pid_liveness: undefined };
  }
  if (lease === null) {
    return { cleared: false, pid_liveness: undefined };
  }
  if (lease.spawned_pid === undefined) {
    // No spawn evidence to probe; the activation may still be coming up.
    return { cleared: false, pid_liveness: "unknown" };
  }
  if (isPidAlive(lease.spawned_pid)) {
    return { cleared: false, pid_liveness: "alive" };
  }
  // The launched process died without registering -> failed activation. Clear
  // the lease, matched on its own id so a concurrent replacement is untouched.
  try {
    const cleared = await client.wakeLease.clear({
      actor_id: actorId,
      lease_id: lease.lease_id,
    });
    return { cleared: cleared.cleared, pid_liveness: "dead" };
  } catch {
    // Best-effort: even if the clear raced, the pid is dead; the lease TTL
    // independently expires the failed activation back to offline.
    return { cleared: false, pid_liveness: "dead" };
  }
}