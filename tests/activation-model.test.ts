import { describe, expect, it } from "vitest";
import {
  createActivationState,
  transitionActivation,
  type ActivationEffect,
  type ActivationEvent,
  type ActivationState,
  type ActivationTransition,
  type QueuedTask,
} from "../src/core/activation-model.js";

const task = (
  id: string,
  delivery: QueuedTask["delivery"] = "wake_if_offline"
): QueuedTask => ({ task_id: id, delivery });

const apply = (
  result: ActivationTransition
): Readonly<{
  state: ActivationState;
  effects: readonly ActivationEffect[];
}> => {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error(result.error.message);
  return result;
};

const reduce = (
  state: ActivationState,
  ...events: readonly ActivationEvent[]
): ActivationState =>
  events.reduce(
    (current, event) => apply(transitionActivation(current, event)).state,
    state
  );

const onlineWithClaim = (
  actorId = "agent",
  runtimeId = "runtime-1"
): ActivationState =>
  reduce(
    createActivationState(actorId),
    {
      type: "task_queued",
      task: task("work"),
      wake_lease_id: "wake-1",
    },
    { type: "runtime_ready", wake_lease_id: "wake-1", runtime_id: runtimeId },
    { type: "claim_batch", claim_id: "claim-1", runtime_id: runtimeId }
  );

describe("activation reference model", () => {
  it("coalesces ten offline tasks into one wake request", () => {
    let state = createActivationState("metabuilder");
    const effects: ActivationEffect[] = [];

    for (let index = 0; index < 10; index += 1) {
      const result = apply(
        transitionActivation(state, {
          type: "task_queued",
          task: task(`task-${index}`),
          wake_lease_id: `lease-${index}`,
        })
      );
      state = result.state;
      effects.push(...result.effects);
    }

    expect(state).toMatchObject({ phase: "starting", wake_lease_id: "lease-0" });
    expect(state.pending_tasks).toHaveLength(10);
    expect(effects).toEqual([
      {
        type: "request_wake",
        actor_id: "metabuilder",
        wake_lease_id: "lease-0",
      },
    ]);
  });

  it("includes startup arrivals in the first claimed batch", () => {
    const online = reduce(
      createActivationState("cultivar"),
      {
        type: "task_queued",
        task: task("before-start"),
        wake_lease_id: "wake-1",
      },
      {
        type: "task_queued",
        task: task("during-start"),
        wake_lease_id: "unused-wake",
      },
      { type: "runtime_ready", wake_lease_id: "wake-1", runtime_id: "pi-1" }
    );
    const claimed = apply(
      transitionActivation(online, {
        type: "claim_batch",
        claim_id: "claim-1",
        runtime_id: "pi-1",
      })
    );

    expect(claimed.state.claim?.tasks.map(({ task_id }) => task_id)).toEqual([
      "before-start",
      "during-start",
    ]);
    expect(claimed.effects[0]?.type).toBe("deliver_batch");
  });

  it("stores work without waking when store_only is requested", () => {
    const result = apply(
      transitionActivation(createActivationState("offline-agent"), {
        type: "task_queued",
        task: task("later", "store_only"),
      })
    );

    expect(result.state.phase).toBe("offline");
    expect(result.state.pending_tasks).toEqual([task("later", "store_only")]);
    expect(result.effects).toEqual([]);
  });

  it("notifies rather than re-waking an online runtime", () => {
    const online = reduce(
      createActivationState("online-agent"),
      {
        type: "task_queued",
        task: task("initial"),
        wake_lease_id: "wake-1",
      },
      {
        type: "runtime_ready",
        wake_lease_id: "wake-1",
        runtime_id: "codex-1",
      }
    );
    const next = apply(
      transitionActivation(online, {
        type: "task_queued",
        task: task("next"),
        wake_lease_id: "must-not-be-used",
      })
    );

    expect(next.effects).toEqual([
      {
        type: "notify_runtime",
        actor_id: "online-agent",
        runtime_id: "codex-1",
      },
    ]);
  });

  it("recovers a crashed claim and requests one replacement wake", () => {
    const stopped = apply(
      transitionActivation(onlineWithClaim("recovering-agent", "pi-crashed"), {
        type: "runtime_stopped",
        runtime_id: "pi-crashed",
      })
    ).state;
    const recovered = apply(
      transitionActivation(stopped, {
        type: "claim_expired",
        claim_id: "claim-1",
        wake_lease_id: "wake-2",
      })
    );

    expect(recovered.state).toMatchObject({ phase: "starting", claim: undefined });
    expect(recovered.state.pending_tasks).toEqual([task("work")]);
    expect(recovered.effects[0]).toMatchObject({
      type: "request_wake",
      wake_lease_id: "wake-2",
    });
  });

  it("treats duplicate acknowledgement as an idempotent no-op", () => {
    const acknowledged = apply(
      transitionActivation(onlineWithClaim(), {
        type: "acknowledge_batch",
        claim_id: "claim-1",
        runtime_id: "runtime-1",
      })
    ).state;
    const duplicate = apply(
      transitionActivation(acknowledged, {
        type: "acknowledge_batch",
        claim_id: "claim-1",
        runtime_id: "runtime-1",
      })
    );

    expect(duplicate.state).toEqual(acknowledged);
    expect(duplicate.state.acknowledged_task_ids).toEqual(["work"]);
    expect(duplicate.effects).toEqual([]);
  });

  it("rejects duplicate acknowledgement from a different runtime", () => {
    const acknowledged = apply(
      transitionActivation(onlineWithClaim(), {
        type: "acknowledge_batch",
        claim_id: "claim-1",
        runtime_id: "runtime-1",
      })
    ).state;
    const intruder = transitionActivation(acknowledged, {
      type: "acknowledge_batch",
      claim_id: "claim-1",
      runtime_id: "other-runtime",
    });

    expect(intruder.ok).toBe(false);
    if (!intruder.ok) expect(intruder.error.code).toBe("invalid_identity");
    expect(intruder.state).toBe(acknowledged);
  });

  it("preserves unknown activation without issuing a blind retry", () => {
    const unknown = reduce(
      createActivationState("uncertain-agent"),
      {
        type: "task_queued",
        task: task("uncertain-work"),
        wake_lease_id: "wake-unknown",
      },
      {
        type: "activation_outcome_unknown",
        wake_lease_id: "wake-unknown",
      }
    );
    const arrival = apply(
      transitionActivation(unknown, {
        type: "task_queued",
        task: task("arrived-while-unknown"),
        wake_lease_id: "must-not-be-used",
      })
    );

    expect(arrival.state.phase).toBe("activation_unknown");
    expect(arrival.state.pending_tasks).toHaveLength(2);
    expect(arrival.effects).toEqual([]);
  });

  it("requires reconciliation before an unknown activation can be retried", () => {
    const unknown = reduce(
      createActivationState("reconcile-agent"),
      {
        type: "task_queued",
        task: task("work"),
        wake_lease_id: "wake-1",
      },
      { type: "activation_outcome_unknown", wake_lease_id: "wake-1" }
    );
    expect(
      transitionActivation(unknown, {
        type: "retry_activation",
        wake_lease_id: "wake-2",
      }).ok
    ).toBe(false);

    const reconciled = reduce(unknown, {
      type: "activation_reconciled",
      wake_lease_id: "wake-1",
      outcome: "offline",
    });
    const retry = apply(
      transitionActivation(reconciled, {
        type: "retry_activation",
        wake_lease_id: "wake-2",
      })
    );

    expect(retry.state.phase).toBe("starting");
    expect(retry.effects[0]).toMatchObject({ wake_lease_id: "wake-2" });
  });

  it("rejects a second runtime from claiming the active actor", () => {
    const online = reduce(
      createActivationState("single-runtime-agent"),
      {
        type: "task_queued",
        task: task("work"),
        wake_lease_id: "wake-1",
      },
      {
        type: "runtime_ready",
        wake_lease_id: "wake-1",
        runtime_id: "owner-runtime",
      }
    );
    const intruder = transitionActivation(online, {
      type: "claim_batch",
      claim_id: "intruder-claim",
      runtime_id: "other-runtime",
    });

    expect(intruder.ok).toBe(false);
    if (!intruder.ok) expect(intruder.error.code).toBe("invalid_event");
    expect(intruder.state).toBe(online);
  });

  it("does not mutate earlier state snapshots", () => {
    const initial = createActivationState("immutable-agent");
    const queued = apply(
      transitionActivation(initial, {
        type: "task_queued",
        task: task("work"),
        wake_lease_id: "wake-1",
      })
    ).state;

    expect(Object.isFrozen(initial)).toBe(true);
    expect(Object.isFrozen(queued.pending_tasks)).toBe(true);
    expect(initial).toEqual(createActivationState("immutable-agent"));
  });
});
