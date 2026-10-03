import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import { classifyPresence, type RuntimeIncarnation } from "../../core/actor-presence.js";
import { sessionTag } from "../../core/session-tag.js";
import {
  assemblePresenceInput,
  type AssembledWakeLease,
} from "./presence-input.js";

// Public projection. actor_status is readable by any participant, even before
// registration, and a session id is a bearer credential, so each one becomes
// its tag here; field names and shape stay. Presence assembly, and the
// send_message wake gate that shares it, keep the raw values.

const publicRuntime = (
  runtime: RuntimeIncarnation | undefined
): RuntimeIncarnation | undefined =>
  runtime === undefined
    ? undefined
    : Object.freeze({
        ...runtime,
        incarnation_id: sessionTag(runtime.incarnation_id),
        ...(runtime.session_id === undefined ? {} : { session_id: sessionTag(runtime.session_id) }),
        ...(runtime.lease_id === undefined ? {} : { lease_id: sessionTag(runtime.lease_id) }),
      });

/** The waking sender's session is another participant's bearer too. */
const publicWakeLease = (lease: AssembledWakeLease | null): AssembledWakeLease | null =>
  lease === null
    ? null
    : Object.freeze({ ...lease, issued_by_session: sessionTag(lease.issued_by_session) });

export const actorStatusSchema = z.object({
  actor_id: z.string().min(1).describe("Durable actor identity to inspect"),
});

export async function actorStatus(
  client: RedisClient,
  params: z.infer<typeof actorStatusSchema>
) {
  // A read, like custody_status: usable before registration; never throws for
  // domain failures.
  const dir = await client.actorDirectory.get(params.actor_id);
  if (!dir.ok) {
    // An unreadable record is reported as such, not as an unknown actor.
    return toolResult(
      {
        status: "error",
        error: { code: dir.error.code, message: dir.error.message },
        actor_id: params.actor_id,
      },
      true
    );
  }

  let record = null;
  let launchContract: "runnable" | "not_runnable" = "not_runnable";
  let runtime = undefined;
  let wakeLease: AssembledWakeLease | null = null;

  if (dir.record !== null) {
    record = dir.record;
    // Presence assembly (runtime incarnation + wake-lease reconcile ordering)
    // is shared byte-for-byte with send_message's wake gate.
    const assembled = await assemblePresenceInput(client, dir.record);
    launchContract = assembled.launch_contract;
    runtime = assembled.runtime;
    wakeLease = assembled.wake_lease;
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
        wake_lease: publicWakeLease(wakeLease),
      },
      true
    );
  }

  return toolResult({
    status: "ok",
    actor_id: classification.actor_id,
    presence: classification.presence,
    launch_contract: launchContract,
    wake_lease: publicWakeLease(wakeLease),
    runtime: publicRuntime(runtime),
  });
}