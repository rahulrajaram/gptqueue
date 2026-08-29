import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import type { RedisClient } from "../redis-client.js";
import type { QueueMessage } from "../types.js";
import { ensureSessionBinding } from "./session-binding.js";
import { toolResult, type ToolPayload } from "../tool-result.js";
import {
  classifyPresence,
  type RuntimeIncarnation,
} from "../../core/actor-presence.js";
import type { WakeLease } from "../../core/wake-lease.js";
import { dispatchLaunch } from "../launcher.js";
import { workloadForSession } from "./workload-for-session.js";
import { reconcileWakeLease } from "./reconcile-wake-lease.js";

export const sendMessageSchema = z.object({
  session_id: z
    .string()
    .optional()
    .describe(
      "Optional session_id returned by register_agent. Required when the transport does not preserve process-local registration state."
    ),
  to: z.string().describe("Target agent name"),
  content: z.string().describe("Message content"),
  type: z
    .enum(["task", "result", "status", "error", "ping"])
    .default("task")
    .describe("Message type"),
  metadata: z
    .record(z.string(), z.unknown())
    .optional()
    .describe("Optional metadata"),
  in_reply_to: z
    .string()
    .optional()
    .describe("Message ID this is replying to"),
  idempotency_key: z
    .string()
    .min(1)
    .max(128)
    .optional()
    .describe(
      "Caller-generated key for retry safety. Reusing the same key for the same sender returns the original message_id without enqueueing a duplicate."
    ),
});

/**
 * Default wake-lease TTL (whole seconds) when launching an offline durable
 * actor. Bounded well inside WakeLeaseStore's 1..3600 range. 60s is a sane
 * default: it covers a realistic launch/register span while the TTL's expiry
 * independently returns an activation that never came up to offline.
 */
const WAKE_LEASE_SECONDS = 60;

/**
 * Additive `wake` field appended to a send result ONLY for `wake_if_offline`
 * durable-actor recipients. All other recipients produce byte-identical send
 * results to pre-wake behavior.
 */
export type SendWakeResult =
  | Readonly<{ status: "wake_dispatched"; lease_id: string; pid?: number }>
  | Readonly<{
      status: "launch_failed";
      lease_id: string;
      error_message: string;
    }>
  | Readonly<{ status: "wake_coalesced"; lease_id: string }>
  | Readonly<{ status: "wake_error"; error_message: string }>;

/**
 * Derive a runtime incarnation from a live session and its observed workload.
 * Mirrors actor-status.ts exactly: workload is observation-based (a session
 * holding an unacked claim is "processing"), and the lease id is the session
 * id.
 */
async function runtimeFromSession(
  claims: RedisClient["taskClaim"],
  actorId: string,
  sessionId: string
): Promise<RuntimeIncarnation> {
  const workload = await workloadForSession(claims, actorId, sessionId);
  return Object.freeze({
    incarnation_id: sessionId,
    session_id: sessionId,
    lease_id: sessionId,
    workload,
  });
}

/**
 * Best-effort wake of an offline `wake_if_offline` durable actor, run AFTER the
 * message persist has been awaited (see the persist-before-wake invariant in
 * sendMessage). Returns the additive `wake` payload, or undefined when the
 * recipient is not wake-eligible or is already active/idle.
 *
 * Any unexpected error is caught and surfaced as `status: "wake_error"` so the
 * send itself can never fail because of wake handling.
 */
async function maybeWake(
  client: RedisClient,
  to: string
): Promise<SendWakeResult | undefined> {
  try {
    // Presence-gated: only durable `wake_if_offline` actor records are
    // wake-eligible. Plain agents and store_only actors resolve to no wake.
    const dir = await client.actorDirectory.get(to);
    if (
      !dir.ok ||
      dir.record === null ||
      dir.record.profile.activation_policy.mode !== "wake_if_offline"
    ) {
      return undefined;
    }
    const record = dir.record;

    const launchContract = client.actorDirectory.contractReadiness(record);

    // Assemble presence exactly like actor-status.ts: runtime from live
    // sessions with workload "idle", plus any outstanding wake lease.
    let runtime: RuntimeIncarnation | undefined;
    const presence = await client.sessions.getPresence(record.profile.actor_id);
    if (presence.online && presence.active_sessions.length > 0) {
      runtime = await runtimeFromSession(
        client.taskClaim,
        record.profile.actor_id,
        presence.active_sessions[0]!
      );
    }

    let wakeLease: WakeLease | null = null;
    if (runtime === undefined) {
      // Reconcile the outstanding lease against process liveness before
      // classifying: a dead-pid lease is a failed activation and is cleared,
      // so the actor classifies from offline and re-wake becomes possible. A
      // live/un-probed lease is retained so an in-flight start coalesces.
      const reconcile = await reconcileWakeLease(client, record.profile.actor_id);
      if (!reconcile.cleared) {
        wakeLease = await client.wakeLease.get(record.profile.actor_id);
      }
    }

    const classification = classifyPresence({
      actor: record.profile,
      launch_contract: launchContract,
      runtime,
      ...(wakeLease ? { wake_lease_id: wakeLease.lease_id } : {}),
    });

    if (!classification.ok) return undefined;

    switch (classification.presence) {
      // Runtime-attached (or offline-but-not-launchable) recipients keep
      // existing behavior: no wake field at all.
      case "active":
      case "idle":
      case "offline_store_only":
      case "unavailable":
        return undefined;

      // An activation is already in flight for this actor: coalesce onto the
      // outstanding lease; we must not spawn a second runtime.
      case "starting":
        if (wakeLease !== null) {
          return { status: "wake_coalesced", lease_id: wakeLease.lease_id };
        }
        return undefined;

      case "offline_launchable": {
        const acquired = await client.wakeLease.acquire({
          actor_id: record.profile.actor_id,
          issued_by_session: client.sessionId ?? "unknown",
          lease_seconds: WAKE_LEASE_SECONDS,
          now: new Date().toISOString(),
        });
        if (!acquired.ok) {
          return {
            status: "wake_error",
            error_message: acquired.error.message,
          };
        }

        // A concurrent controller already owns the activation in flight.
        if (acquired.coalesced) {
          return {
            status: "wake_coalesced",
            lease_id: acquired.lease.lease_id,
          };
        }

        // We own the new lease: dispatch this actor's runtime launch.
        if (record.launch === null) {
          return {
            status: "wake_error",
            error_message: "launch contract missing for wake_if_offline actor",
          };
        }
        const launched = await dispatchLaunch(record.launch);
        if (launched.dispatched) {
          // Best-effort spawn evidence: record the dispatched pid on the wake
          // lease so observers (actor_status) can surface it. A failure here
          // must never affect the send result.
          if (launched.pid !== undefined) {
            try {
              await client.wakeLease.attachSpawn({
                actor_id: record.profile.actor_id,
                lease_id: acquired.lease.lease_id,
                pid: launched.pid,
                spawned_at: new Date().toISOString(),
              });
            } catch {
              // best-effort; launch and send are unaffected
            }
          }
          return {
            status: "wake_dispatched",
            lease_id: acquired.lease.lease_id,
            pid: launched.pid,
          };
        }
        // Launch failed; the message is already persisted and recoverable, and
        // the lease TTL will expire the activation back to offline.
        return {
          status: "launch_failed",
          lease_id: acquired.lease.lease_id,
          error_message:
            launched.error?.message ?? "launch failed for unknown reason",
        };
      }

      default:
        return undefined;
    }
  } catch (error) {
    return {
      status: "wake_error",
      error_message: error instanceof Error ? error.message : String(error),
    };
  }
}

export async function sendMessage(
  client: RedisClient,
  params: z.infer<typeof sendMessageSchema>
) {
  await ensureSessionBinding(client, params.session_id);

  const message: QueueMessage = {
    id: uuidv4(),
    from: client.requireRegistered(),
    to: params.to,
    timestamp: new Date().toISOString(),
    type: params.type,
    payload: {
      content: params.content,
      metadata: params.metadata,
      in_reply_to: params.in_reply_to,
    },
  };

  const outcome = params.idempotency_key
    ? await client.sendMessageIdempotent(message, params.idempotency_key)
    : { status: (await client.sendMessage(message)) ? "sent" as const : "full" as const, messageId: message.id };

  if (outcome.status === "full") {
    return toolResult({
      status: "error",
      error: {
        code: "QUEUE_FULL",
        message: "Queue full after retries",
        retryable: true,
      },
      to: params.to,
    }, true);
  }

  const payload: ToolPayload = {
    status: outcome.status,
    message_id: outcome.messageId,
    to: params.to,
    deduplicated: outcome.status === "duplicate",
  };

  // persist-before-wake INVARIANT: the mailbox RPUSH above was awaited before
  // any wake attempt, so a launched-but-failed activation can never lose an
  // already-persisted, recoverable message. Waking is purely additive.
  const wake = await maybeWake(client, params.to);
  if (wake !== undefined) {
    payload.wake = wake;
  }

  return toolResult(payload);
}
