/**
 * Shared presence assembly for durable actors.
 *
 * Both `actor_status` (actor-status.ts) and `send_message`'s wake-on-send gate
 * (send-message.ts -> maybeWake) must derive presence for a durable actor from
 * the SAME raw inputs: the actor-directory record, a live leased session's
 * runtime incarnation (workload derived observationally), an outstanding wake
 * lease, and its pid-liveness reconciliation. This single helper is imported
 * by both sites so the two assembly paths stay byte-identical: runtime
 * assembly, the reconcile-before-classify ordering (a dead-pid lease is
 * cleared so the actor classifies from offline and re-wake becomes possible),
 * and the observe-again lease read all happen in exactly one place.
 *
 * The reconcile ordering invariant lives here: `reconcileWakeLease` is always
 * run BEFORE re-reading the lease. When reconciliation cleared the lease, the
 * actor is treated as having no outstanding wake lease at all.
 */

import type { ActorDirectoryRecord } from "../../core/actor-directory.js";
import type {
  LaunchContractReadiness,
  RuntimeIncarnation,
} from "../../core/actor-presence.js";
import type { WakeLease } from "../../core/wake-lease.js";
import type { RedisClient } from "../redis-client.js";
import { workloadForSession } from "./workload-for-session.js";
import {
  reconcileWakeLease,
  type PidLiveness,
} from "./reconcile-wake-lease.js";

/** Presence-assembly dependencies narrowed to what assembly actually touches. */
export type PresenceAssemblyClient = Pick<
  RedisClient,
  "actorDirectory" | "sessions" | "taskClaim" | "wakeLease"
>;

/**
 * A wake lease with the pid-liveness observation computed during
 * reconciliation, surfaced for observability. `pid_liveness` is present only
 * when the lease was read on the no-runtime (post-reconcile) path.
 */
export type AssembledWakeLease = WakeLease & {
  readonly pid_liveness?: PidLiveness;
};

/** The fully-assembled presence inputs for one durable actor record. */
export interface AssembledPresence {
  readonly launch_contract: LaunchContractReadiness;
  readonly runtime: RuntimeIncarnation | undefined;
  /** null when cleared or absent; otherwise the outstanding lease (usually with pid_liveness). */
  readonly wake_lease: AssembledWakeLease | null;
}

/**
 * Derive a runtime incarnation from a live session and its observed workload.
 * Workload is observation-based (a session holding an unacked claim is
 * "processing"), and the lease id is the session id.
 */
function runtimeFromSession(
  sessionId: string,
  workload: "processing" | "idle"
): RuntimeIncarnation {
  return Object.freeze({
    incarnation_id: sessionId,
    session_id: sessionId,
    lease_id: sessionId,
    workload,
  });
}

/**
 * Assemble presence inputs for a durable actor record, byte-identically for
 * actor_status and send_message's wake gate. Best-effort: any store error
 * collapses to a conservative/incomplete presence; it never throws.
 */
export async function assemblePresenceInput(
  client: PresenceAssemblyClient,
  record: ActorDirectoryRecord
): Promise<AssembledPresence> {
  const launch_contract = client.actorDirectory.contractReadiness(record);

  const presence = await client.sessions.getPresence(record.profile.actor_id);
  if (presence.online && presence.active_sessions.length > 0) {
    // A live (leased) session is a running runtime incarnation that outranks any
    // outstanding wake lease, so runtime-attached presence reads the lease only
    // for observability (classification ignores it via runtime-first precedence)
    // and the two reads are independent.
    const sessionId = presence.active_sessions[0]!;
    const [workload, lease] = await Promise.all([
      workloadForSession(client.taskClaim, record.profile.actor_id, sessionId),
      client.wakeLease.get(record.profile.actor_id),
    ]);
    return Object.freeze({
      launch_contract,
      runtime: runtimeFromSession(sessionId, workload),
      wake_lease: lease !== null ? { ...lease } : null,
    });
  }

  // Reconcile-before-classify INVARIANT: reconcile the outstanding lease
  // against process liveness first. A dead-pid lease is a failed activation
  // and is cleared, so the actor classifies from offline and re-wake becomes
  // possible. A live/un-probed lease is retained so an in-flight start
  // coalesces. Only after `cleared === false` do we re-read the lease.
  let wake_lease: AssembledWakeLease | null = null;
  const reconcile = await reconcileWakeLease(client, record.profile.actor_id);
  if (!reconcile.cleared) {
    const lease = await client.wakeLease.get(record.profile.actor_id);
    if (lease !== null) {
      // Surface the spawn liveness observed during reconciliation.
      wake_lease = { ...lease, pid_liveness: reconcile.pid_liveness };
    }
  }

  return Object.freeze({ launch_contract, runtime: undefined, wake_lease });
}