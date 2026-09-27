import { z } from "zod";
import { v4 as uuidv4 } from "uuid";
import type { RedisClient } from "../redis-client.js";
import type { QueueMessage } from "../types.js";
import { bindSession, ensureSessionBinding } from "./session-binding.js";
import { toolResult, type ToolPayload } from "../tool-result.js";
import {
  classifyPresence,
  type RuntimePresenceState,
} from "../../core/actor-presence.js";
import type { ActorDirectoryRecord } from "../../core/actor-directory.js";
import type { WakeLease } from "../../core/wake-lease.js";
import { dispatchLaunch } from "../launcher.js";
import {
  assemblePresenceInput,
  type AssembledPresence,
} from "./presence-input.js";

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
 * Wake policy gate: only a durable `wake_if_offline` actor record is
 * wake-eligible; plain agents and store_only actors get no wake. Pure: it
 * performs no presence assembly or lease work.
 */
export function wakeEligible(
  record: ActorDirectoryRecord | null
): { readonly record: ActorDirectoryRecord } | undefined {
  if (record === null || record.profile.activation_policy.mode !== "wake_if_offline") {
    return undefined;
  }
  return Object.freeze({ record });
}

/**
 * The wake decision for one classified presence state, discriminated so an
 * unknown presence is a COMPILE error (`assertNever`) rather than a silent
 * no-wake. A new presence state added to `RuntimePresenceState` must be mapped
 * here before it can ever be dispatched.
 */
export type WakePresenceDecision =
  | Readonly<{ kind: "no_wake" }>
  | Readonly<{ kind: "coalesce" }>
  | Readonly<{ kind: "launch" }>;

const assertNever = (value: never): never => {
  throw new Error(`Unhandled wake decision: ${JSON.stringify(value)}`);
};

/**
 * Map a classified presence state to the wake action. Exhaustive over every
 * member of `RuntimePresenceState` — the union is the source of the
 * `default: never` guarantee. `hasWakeLease` drives the starting -> coalesce
 * vs no-wake split (starting without an observed lease is a degenerate case
 * that keeps pre-wake behavior).
 */
export function wakeDecisionForPresence(
  presence: RuntimePresenceState,
  hasWakeLease: boolean
): WakePresenceDecision {
  switch (presence) {
    // Runtime-attached (or offline-but-not-launchable) recipients get no wake.
    case "active":
    case "idle":
    case "offline_store_only":
    case "unavailable":
      return Object.freeze({ kind: "no_wake" });
    // An activation is already in flight; coalesce when a lease is observed.
    case "starting":
      return hasWakeLease
        ? Object.freeze({ kind: "coalesce" })
        : Object.freeze({ kind: "no_wake" });
    case "offline_launchable":
      return Object.freeze({ kind: "launch" });
    default:
      return assertNever(presence);
  }
}

/** Outcome of the atomic acquire-or-coalesce on the wake lease store. */
type AcquireOutcome =
  | Readonly<{ kind: "owned"; lease: WakeLease }>
  | Readonly<{ kind: "coalesced"; lease_id: string }>
  | Readonly<{ kind: "error"; error_message: string }>;

/**
 * Atomically acquire this actor's wake lease, or learn that a concurrent
 * controller already owns the in-flight activation (coalesce onto it). Never
 * throws: a store error is returned as `error`.
 */
async function acquireOrCoalesce(
  client: Pick<RedisClient, "wakeLease" | "sessionId">,
  record: ActorDirectoryRecord
): Promise<AcquireOutcome> {
  const acquired = await client.wakeLease.acquire({
    actor_id: record.profile.actor_id,
    issued_by_session: client.sessionId ?? "unknown",
    lease_seconds: WAKE_LEASE_SECONDS,
    now: new Date().toISOString(),
  });
  if (!acquired.ok) {
    return { kind: "error", error_message: acquired.error.message };
  }
  if (acquired.coalesced) {
    return { kind: "coalesced", lease_id: acquired.lease.lease_id };
  }
  return { kind: "owned", lease: acquired.lease };
}

/**
 * Dispatch the actor's runtime launch now that WE own the lease, attaching
 * best-effort spawn evidence to it. `launch !== null` is guaranteed for a
 * `wake_if_offline` record admitted through the tool, but a legacy
 * inconsistent record (launch:null direct on db) is handled loudly rather
 * than dispatching a null. Never throws.
 */
async function shipLaunch(
  client: Pick<RedisClient, "wakeLease">,
  record: ActorDirectoryRecord,
  lease: WakeLease
): Promise<
  | Readonly<{ status: "wake_dispatched"; lease_id: string; pid?: number }>
  | Readonly<{
      status: "launch_failed";
      lease_id: string;
      error_message: string;
    }>
  | Readonly<{ status: "wake_error"; error_message: string }>
> {
  if (record.launch === null) {
    return {
      status: "wake_error",
      error_message: "launch contract missing for wake_if_offline actor",
    };
  }
  const launched = await dispatchLaunch(record.launch);
  if (launched.dispatched) {
    // Best-effort spawn evidence: record the dispatched pid on the wake lease
    // so observers (actor_status) can surface it. A failure here must never
    // affect the send result.
    if (launched.pid !== undefined) {
      try {
        await client.wakeLease.attachSpawn({
          actor_id: record.profile.actor_id,
          lease_id: lease.lease_id,
          pid: launched.pid,
          spawned_at: new Date().toISOString(),
        });
      } catch {
        // best-effort; launch and send are unaffected
      }
    }
    return {
      status: "wake_dispatched",
      lease_id: lease.lease_id,
      pid: launched.pid,
    };
  }
  // Launch failed; the message is already persisted and recoverable, and the
  // lease TTL will expire the activation back to offline.
  return {
    status: "launch_failed",
    lease_id: lease.lease_id,
    error_message:
      launched.error?.message ?? "launch failed for unknown reason",
  };
}

/**
 * Execute the wake for an `offline_launchable` actor: acquire/coalesce the
 * lease, then (if we own it) ship the launch. A pure linear composition of
 * `acquireOrCoalesce` + `shipLaunch`.
 */
async function wakeOfflineLaunchable(
  client: Pick<RedisClient, "wakeLease" | "sessionId">,
  record: ActorDirectoryRecord
): Promise<SendWakeResult> {
  const acquire = await acquireOrCoalesce(client, record);
  switch (acquire.kind) {
    case "owned":
      return shipLaunch(client, record, acquire.lease);
    case "coalesced":
      return { status: "wake_coalesced", lease_id: acquire.lease_id };
    case "error":
      return { status: "wake_error", error_message: acquire.error_message };
    default:
      return assertNever(acquire);
  }
}

/**
 * Best-effort wake of an offline `wake_if_offline` durable actor, run AFTER the
 * message persist has been awaited (see the persist-before-wake invariant in
 * sendMessage). Returns the additive `wake` payload, or undefined when the
 * recipient is not wake-eligible or is already active/idle.
 *
 * `record` is the directory record resolved before the persist; reusing it
 * saves a second directory read on every durable-actor send.
 *
 * Shallow linear composition: gate eligibility -> assemble presence -> classify
 * -> dispatch the classified result. Presence dispatch is exhaustively handled
 * by `wakeDecisionForPresence` (a new presence state is a compile error, never
 * a silent no-wake).
 *
 * Any unexpected error is caught and surfaced as `status: "wake_error"` so the
 * send itself can never fail because of wake handling.
 */
async function maybeWake(
  client: RedisClient,
  resolved: ActorDirectoryRecord | null
): Promise<SendWakeResult | undefined> {
  try {
    const gate = wakeEligible(resolved);
    if (gate === undefined) return undefined;
    const { record } = gate;

    // Presence assembly is shared byte-for-byte with actor-status.ts.
    const presence: AssembledPresence = await assemblePresenceInput(client, record);

    const classification = classifyPresence({
      actor: record.profile,
      launch_contract: presence.launch_contract,
      runtime: presence.runtime,
      ...(presence.wake_lease
        ? { wake_lease_id: presence.wake_lease.lease_id }
        : {}),
    });

    if (!classification.ok) return undefined;

    const decision = wakeDecisionForPresence(
      classification.presence,
      presence.wake_lease !== null
    );
    switch (decision.kind) {
      case "no_wake":
        return undefined;
      case "coalesce":
        return presence.wake_lease !== null
          ? {
              status: "wake_coalesced",
              lease_id: presence.wake_lease.lease_id,
            }
          : undefined;
      case "launch":
        return wakeOfflineLaunchable(client, record);
      default:
        return assertNever(decision);
    }
  } catch (error) {
    return {
      status: "wake_error",
      error_message: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * Frozen typed error result for an unresolvable recipient. Shape matches the
 * repo's typed-tool-error convention (`status`/`error`/`isError`), consistent
 * with QUEUE_FULL and the durable-actor gate errors.
 */
function unknownRecipient(to: string) {
  return Object.freeze(
    toolResult(
      {
        status: "error",
        error: {
          code: "unknown_recipient",
          message: `recipient '${to}' is not a registered agent or durable actor; no mailbox or queue data was created`,
        },
        to,
      },
      true
    )
  );
}

/**
 * Resolve-then-push recipient validation (review-mandated H3): BEFORE any
 * mailbox push, resolve `to` as (a) a durable actor-directory record, or
 * (b) a REGISTERED agent via the canonical registry. Anything else is an
 * unknown recipient and is rejected with a frozen typed error WITHOUT
 * creating any queue (`gptq:q:<to>`) or metadata (`gptq:meta:<to>`) keys.
 * Both lookups run in one round trip; the directory result keeps precedence.
 */
async function resolveRecipient(
  client: RedisClient,
  to: string
): Promise<
  | { ok: true; record: ActorDirectoryRecord | null }
  | { ok: false; error: { code: string; message: string } }
> {
  const [dir, registered] = await Promise.all([
    client.actorDirectory.get(to),
    client.sessions.resolveRegistered(to),
  ]);
  if (!dir.ok) {
    // Fail closed on a corrupt/unreadable directory: do not create keys.
    return { ok: false, error: { code: dir.error.code, message: dir.error.message } };
  }
  if (dir.record !== null) {
    return { ok: true, record: dir.record }; // durable actor path
  }
  if (registered) {
    return { ok: true, record: null }; // registered (plain) agent path
  }
  return { ok: false, error: { code: "unknown_recipient", message: "" } };
}

export async function sendMessage(
  client: RedisClient,
  params: z.infer<typeof sendMessageSchema>
) {
  // Recipient first: an unknown recipient is reported as such even to a
  // caller that is not registered itself (lifecycle invariant H8).
  await ensureSessionBinding(client, params.session_id);

  const resolved = await resolveRecipient(client, params.to);
  if (!resolved.ok) {
    if (resolved.error.code === "unknown_recipient") {
      return unknownRecipient(params.to);
    }
    return toolResult(
      {
        status: "error",
        error: { code: resolved.error.code, message: resolved.error.message },
        to: params.to,
      },
      true
    );
  }

  const { agent: sender } = await bindSession(client, params.session_id);

  const message: QueueMessage = {
    id: uuidv4(),
    from: sender,
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
  const wake = await maybeWake(client, resolved.record);
  if (wake !== undefined) {
    payload.wake = wake;
  }

  return toolResult(payload);
}
