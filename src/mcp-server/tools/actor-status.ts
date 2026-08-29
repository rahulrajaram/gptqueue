import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import {
  classifyPresence,
  type RuntimeIncarnation,
} from "../../core/actor-presence.js";
import { workloadForSession } from "./workload-for-session.js";
import {
  reconcileWakeLease,
  type PidLiveness,
} from "./reconcile-wake-lease.js";

export const actorStatusSchema = z.object({
  actor_id: z.string().min(1).describe("Durable actor identity to inspect"),
});

/**
 * Derive a runtime incarnation from a live session and its observed workload.
 * The report defaults a leased but un-observed runtime to the supplied
 * workload, which is observation-derived: a session holding an unacked claim
 * is "processing" (the runtime accepted work in flight), otherwise "idle".
 * The lease id is the session id (the session's Redis lease key is keyed on
 * the session id).
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

export async function actorStatus(
  client: RedisClient,
  params: z.infer<typeof actorStatusSchema>
) {
  // A read, like custody_status: usable before registration; never throws for
  // domain failures.
  const dir = await client.actorDirectory.get(params.actor_id);

  let record = null;
  let launchContract: "runnable" | "not_runnable" = "not_runnable";
  let runtime: RuntimeIncarnation | undefined = undefined;
  let wakeLease:
    | { lease_id: string; actor_id: string; issued_by_session: string; issued_at: string; expires_at: string; spawned_pid?: number; spawned_at?: string; pid_liveness?: PidLiveness }
    | null = null;

  if (dir.ok) {
    record = dir.record;
    if (record !== null) {
      launchContract = client.actorDirectory.contractReadiness(record);

      // A live (leased) session is a running runtime incarnation that outranks
      // any outstanding wake lease. Workload is observation-based: a session
      // holding an unacked claim classifies as processing (active); otherwise
      // idle.
      const presence = await client.sessions.getPresence(record.profile.actor_id);
      if (presence.online && presence.active_sessions.length > 0) {
        const sessionId = presence.active_sessions[0]!;
        const workload = await workloadForSession(
          client.taskClaim,
          record.profile.actor_id,
          sessionId
        );
        runtime = runtimeFromSession(sessionId, workload);
      }

      if (runtime === undefined) {
        // Reconcile first: an outstanding wake lease whose spawned process died
        // without registering is a failed activation -> clear it, then read the
        // (possibly gone) lease and classify from offline. A live or un-probed
        // lease is retained, with its pid_liveness surfaced in the payload.
        const reconcile = await reconcileWakeLease(
          client,
          record.profile.actor_id
        );
        if (!reconcile.cleared) {
          const lease = await client.wakeLease.get(record.profile.actor_id);
          if (lease !== null) {
            wakeLease = { ...lease, pid_liveness: reconcile.pid_liveness };
          }
        }
      } else {
        // Runtime attached: read any lease for observability but classify by
        // the runtime (which outranks a stale lease).
        wakeLease = await client.wakeLease.get(record.profile.actor_id);
      }
    }
  }

  const classification = classifyPresence({
    actor: record?.profile,
    launch_contract: launchContract,
    runtime,
    ...(wakeLease ? { wake_lease_id: wakeLease.lease_id } : {}),
  });

  if (!classification.ok) {
    return toolResult(
      {
        status: "error",
        error: classification.error,
        actor_id: params.actor_id,
        launch_contract: launchContract,
        wake_lease: wakeLease,
      },
      true
    );
  }

  return toolResult({
    status: "ok",
    actor_id: classification.actor_id,
    presence: classification.presence,
    launch_contract: launchContract,
    wake_lease: wakeLease,
    runtime: runtime,
  });
}