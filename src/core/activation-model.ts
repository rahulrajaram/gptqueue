/**
 * REFERENCE MODEL — NOT wired into production delivery.
 *
 * This module is a pure reference model for single-flight activation and
 * batch delivery. It returns effect intents; it performs no Redis, process, or
 * network work. It is consumed ONLY by tests (tests/activation-model.test.ts)
 * and is deliberately NOT wired into production.
 *
 * Production wake/claim/acknowledge/recover delivery lives in:
 *   - src/mcp-server/tools/send-message.ts (maybeWake and its decomposed
 *     helpers)
 *   - src/mcp-server/tools/reconcile-wake-lease.ts (pid-liveness reconcile)
 *   - src/core/wake-lease.ts + src/mcp-server/lua/wake-lease-*.lua (lease
 *     acquire/clear/attach)
 *   - src/core/task-claim-store.ts + claims-*.lua (claim/ack/recover)
 *
 * This model is neither used as a driver nor asserted to match the Lua-backed
 * implementation above. WIRING IT INTO PRODUCTION IS A DELIBERATE FUTURE
 * DECISION; until then the tests/reference-boundary.test.ts grep-guard keeps
 * the boundary honest (no src file outside this module may import it, so a
 * silent rewiring cannot happen without touching that test).
 */

export type ActivationPhase =
  | "offline"
  | "starting"
  | "online"
  | "activation_unknown";
export type DeliveryMode = "wake_if_offline" | "store_only";

export interface QueuedTask {
  readonly task_id: string;
  readonly delivery: DeliveryMode;
}

export interface BatchClaim {
  readonly claim_id: string;
  readonly runtime_id: string;
  readonly tasks: readonly QueuedTask[];
}

export interface ActivationState {
  readonly actor_id: string;
  readonly phase: ActivationPhase;
  readonly pending_tasks: readonly QueuedTask[];
  readonly acknowledged_task_ids: readonly string[];
  readonly acknowledged_claims: readonly Readonly<{
    claim_id: string;
    runtime_id: string;
  }>[];
  readonly wake_lease_id?: string;
  readonly active_runtime_id?: string;
  readonly claim?: BatchClaim;
}

export type ActivationEvent =
  | Readonly<{
      type: "task_queued";
      task: QueuedTask;
      wake_lease_id?: string;
    }>
  | Readonly<{
      type: "runtime_ready";
      wake_lease_id: string;
      runtime_id: string;
    }>
  | Readonly<{ type: "claim_batch"; claim_id: string; runtime_id: string }>
  | Readonly<{
      type: "acknowledge_batch";
      claim_id: string;
      runtime_id: string;
    }>
  | Readonly<{ type: "runtime_stopped"; runtime_id: string }>
  | Readonly<{
      type: "claim_expired";
      claim_id: string;
      wake_lease_id: string;
    }>
  | Readonly<{ type: "activation_outcome_unknown"; wake_lease_id: string }>
  | Readonly<{
      type: "activation_reconciled";
      wake_lease_id: string;
      outcome: "offline" | "online";
      runtime_id?: string;
    }>
  | Readonly<{ type: "retry_activation"; wake_lease_id: string }>;

export type ActivationEffect =
  | Readonly<{ type: "request_wake"; actor_id: string; wake_lease_id: string }>
  | Readonly<{ type: "notify_runtime"; actor_id: string; runtime_id: string }>
  | Readonly<{ type: "deliver_batch"; actor_id: string; claim: BatchClaim }>;

export type ActivationTransition =
  | Readonly<{
      ok: true;
      state: ActivationState;
      effects: readonly ActivationEffect[];
    }>
  | Readonly<{
      ok: false;
      state: ActivationState;
      error: Readonly<{
        code: "invalid_event" | "invalid_identity" | "wake_lease_required";
        message: string;
      }>;
    }>;

const freezeTasks = (tasks: readonly QueuedTask[]): readonly QueuedTask[] =>
  Object.freeze(tasks.map((task) => Object.freeze({ ...task })));

const freezeState = (state: ActivationState): ActivationState =>
  Object.freeze({
    ...state,
    pending_tasks: freezeTasks(state.pending_tasks),
    acknowledged_task_ids: Object.freeze([...state.acknowledged_task_ids]),
    acknowledged_claims: Object.freeze(
      state.acknowledged_claims.map((claim) => Object.freeze({ ...claim }))
    ),
    claim:
      state.claim === undefined
        ? undefined
        : Object.freeze({ ...state.claim, tasks: freezeTasks(state.claim.tasks) }),
  });

const update = (
  state: ActivationState,
  patch: Partial<ActivationState>
): ActivationState => freezeState({ ...state, ...patch });

const ok = (
  state: ActivationState,
  effects: readonly ActivationEffect[] = []
): ActivationTransition =>
  Object.freeze({ ok: true, state, effects: Object.freeze([...effects]) });

const invalid = (
  state: ActivationState,
  code: "invalid_event" | "invalid_identity" | "wake_lease_required",
  message: string
): ActivationTransition =>
  Object.freeze({
    ok: false,
    state,
    error: Object.freeze({ code, message }),
  });

const taskExists = (state: ActivationState, taskId: string): boolean =>
  state.pending_tasks.some((task) => task.task_id === taskId) ||
  state.claim?.tasks.some((task) => task.task_id === taskId) === true ||
  state.acknowledged_task_ids.includes(taskId);

const hasWakeableTask = (state: ActivationState): boolean =>
  state.pending_tasks.some((task) => task.delivery === "wake_if_offline");

const requestWake = (
  state: ActivationState,
  wakeLeaseId: string
): ActivationTransition =>
  ok(freezeState({ ...state, phase: "starting", wake_lease_id: wakeLeaseId }), [
    {
      type: "request_wake",
      actor_id: state.actor_id,
      wake_lease_id: wakeLeaseId,
    },
  ]);

const assertNever = (event: never): never => {
  throw new Error(`Unhandled activation event: ${JSON.stringify(event)}`);
};

export const createActivationState = (actorId: string): ActivationState =>
  freezeState({
    actor_id: actorId,
    phase: "offline",
    pending_tasks: [],
    acknowledged_task_ids: [],
    acknowledged_claims: [],
  });

/** Reduce one event into a new immutable state and controller effect intents. */
export const transitionActivation = (
  state: ActivationState,
  event: ActivationEvent
): ActivationTransition => {
  switch (event.type) {
    case "task_queued": {
      if (taskExists(state, event.task.task_id)) return ok(state);
      const queued = update(state, {
        pending_tasks: [...state.pending_tasks, event.task],
      });

      if (state.phase === "online") {
        if (state.active_runtime_id === undefined) {
          return invalid(state, "invalid_identity", "online actor has no runtime");
        }
        return ok(queued, [
          {
            type: "notify_runtime",
            actor_id: state.actor_id,
            runtime_id: state.active_runtime_id,
          },
        ]);
      }

      if (
        state.phase !== "offline" ||
        state.claim !== undefined ||
        event.task.delivery === "store_only"
      ) {
        return ok(queued);
      }
      return event.wake_lease_id === undefined
        ? invalid(
            state,
            "wake_lease_required",
            "wake_if_offline requires a controller-issued wake lease"
          )
        : requestWake(queued, event.wake_lease_id);
    }

    case "runtime_ready":
      return state.phase === "starting" &&
        state.wake_lease_id === event.wake_lease_id
        ? ok(
            update(state, {
              phase: "online",
              wake_lease_id: undefined,
              active_runtime_id: event.runtime_id,
            })
          )
        : invalid(
            state,
            "invalid_event",
            "runtime readiness must match the starting wake lease"
          );

    case "claim_batch": {
      if (
        state.phase !== "online" ||
        state.active_runtime_id !== event.runtime_id ||
        state.claim !== undefined ||
        state.pending_tasks.length === 0
      ) {
        return invalid(
          state,
          "invalid_event",
          "only the active runtime may claim a non-empty unclaimed batch"
        );
      }
      const claim = Object.freeze({
        claim_id: event.claim_id,
        runtime_id: event.runtime_id,
        tasks: freezeTasks(state.pending_tasks),
      });
      return ok(update(state, { pending_tasks: [], claim }), [
        { type: "deliver_batch", actor_id: state.actor_id, claim },
      ]);
    }

    case "acknowledge_batch": {
      const prior = state.acknowledged_claims.find(
        (claim) => claim.claim_id === event.claim_id
      );
      if (prior !== undefined) {
        return prior.runtime_id === event.runtime_id
          ? ok(state)
          : invalid(
              state,
              "invalid_identity",
              "duplicate acknowledgement must match the original runtime"
            );
      }
      if (
        state.claim?.claim_id !== event.claim_id ||
        state.claim.runtime_id !== event.runtime_id
      ) {
        return invalid(
          state,
          "invalid_identity",
          "acknowledgement must match the active claim and runtime"
        );
      }
      return ok(
        update(state, {
          claim: undefined,
          acknowledged_task_ids: [
            ...state.acknowledged_task_ids,
            ...state.claim.tasks.map((task) => task.task_id),
          ],
          acknowledged_claims: [
            ...state.acknowledged_claims,
            { claim_id: event.claim_id, runtime_id: event.runtime_id },
          ],
        })
      );
    }

    case "runtime_stopped":
      return state.phase === "online" &&
        state.active_runtime_id === event.runtime_id
        ? ok(update(state, { phase: "offline", active_runtime_id: undefined }))
        : invalid(
            state,
            "invalid_identity",
            "only the active runtime may report that it stopped"
          );

    case "claim_expired": {
      if (state.claim?.claim_id !== event.claim_id) {
        return invalid(
          state,
          "invalid_identity",
          "claim expiry must match the outstanding claim"
        );
      }
      const recovered = update(state, {
        claim: undefined,
        pending_tasks: [...state.claim.tasks, ...state.pending_tasks],
      });
      return recovered.phase === "offline" && hasWakeableTask(recovered)
        ? requestWake(recovered, event.wake_lease_id)
        : ok(recovered);
    }

    case "activation_outcome_unknown":
      return state.phase === "starting" &&
        state.wake_lease_id === event.wake_lease_id
        ? ok(update(state, { phase: "activation_unknown" }))
        : invalid(
            state,
            "invalid_event",
            "unknown activation must match the starting wake lease"
          );

    case "activation_reconciled":
      if (
        state.phase !== "activation_unknown" ||
        state.wake_lease_id !== event.wake_lease_id
      ) {
        return invalid(
          state,
          "invalid_event",
          "reconciliation must match the unresolved wake lease"
        );
      } else if (event.outcome === "offline") {
        return ok(update(state, { phase: "offline", wake_lease_id: undefined }));
      } else if (event.runtime_id !== undefined) {
        return ok(
          update(state, {
            phase: "online",
            wake_lease_id: undefined,
            active_runtime_id: event.runtime_id,
          })
        );
      }
      return invalid(
        state,
        "invalid_identity",
        "online reconciliation requires the observed runtime identity"
      );

    case "retry_activation":
      return state.phase === "offline" &&
        state.claim === undefined &&
        hasWakeableTask(state)
        ? requestWake(state, event.wake_lease_id)
        : invalid(
            state,
            "invalid_event",
            "retry requires an offline actor with unclaimed wakeable work"
          );

    default:
      return assertNever(event);
  }
};
