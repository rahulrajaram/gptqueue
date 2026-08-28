import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import {
  classifyPresence,
  type RuntimeIncarnation,
} from "../../core/actor-presence.js";

export const actorStatusSchema = z.object({
  actor_id: z.string().min(1).describe("Durable actor identity to inspect"),
});

/**
 * Derive a runtime incarnation from a live session, if one exists. The report
 * phase defaults a leased but un-observed runtime to workload "idle": this
 * slice has no workload tracking, so any live leased session is treated as
 * ready to accept work rather than processing. The lease id is the session id
 * (the session's Redis lease key is keyed on the session id).
 */
function runtimeFromSession(sessionId: string): RuntimeIncarnation {
  return Object.freeze({
    incarnation_id: sessionId,
    session_id: sessionId,
    lease_id: sessionId,
    workload: "idle" as const,
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
    | { lease_id: string; actor_id: string; issued_by_session: string; issued_at: string; expires_at: string }
    | null = null;

  if (dir.ok) {
    record = dir.record;
    if (record !== null) {
      launchContract = client.actorDirectory.contractReadiness(record);

      // A live (leased) session is a running runtime incarnation that outranks
      // any outstanding wake lease.
      const presence = await client.sessions.getPresence(record.profile.actor_id);
      if (presence.online && presence.active_sessions.length > 0) {
        runtime = runtimeFromSession(presence.active_sessions[0]!);
      }

      wakeLease = await client.wakeLease.get(record.profile.actor_id);
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