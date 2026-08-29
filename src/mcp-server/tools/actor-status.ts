import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import { classifyPresence } from "../../core/actor-presence.js";
import {
  assemblePresenceInput,
  type AssembledWakeLease,
} from "./presence-input.js";

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

  let record = null;
  let launchContract: "runnable" | "not_runnable" = "not_runnable";
  let runtime = undefined;
  let wakeLease: AssembledWakeLease | null = null;

  if (dir.ok && dir.record !== null) {
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