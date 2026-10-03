import { describe, expect, it } from "vitest";
import {
  admitHandoffRecord,
  createCustody,
  transitionCustody,
  type CustodianIdentity,
  type CustodyRecord,
  type CustodyTransitionResult,
  type HandoffRecordV1,
} from "../src/core/custody-model.js";

const LEASE = "2030-01-01T00:00:00.000Z";

const worktree = (): CustodyRecord["worktree"] => ({
  worktree_path: "/srv/worktrees/agent",
  repo_head: "abc123",
  tree_fingerprint: "fp-1",
});

const custodian = (
  overrides: Partial<CustodianIdentity> = {}
): CustodianIdentity => ({
  actor_name: "origin-agent",
  session_id: "session-1",
  ...overrides,
});

const handoff = (
  overrides: Partial<HandoffRecordV1> = {}
): HandoffRecordV1 => ({
  schema_version: 1,
  authored_by: "origin",
  repo_head: "abc123",
  tracked_tree_state: "clean",
  untracked_inventory: [],
  unfinished_work: "wrap up the batch",
  hazards: [],
  next_step: "commit the batch",
  ...overrides,
});

const apply = (result: CustodyTransitionResult): CustodyRecord => {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error(result.error.message);
  return result.state;
};

const heldBy = (
  claimOn: CustodyRecord,
  who: CustodianIdentity = custodian(),
  lease = LEASE
): CustodyRecord =>
  apply(
    transitionCustody(claimOn, {
      type: "claim",
      custodian: who,
      lease_expires_at: lease,
    })
  );

describe("handoff record admission", () => {
  it("admits a valid handoff deeply frozen", () => {
    const result = admitHandoffRecord(
      handoff({
        tracked_tree_state: "dirty",
        untracked_inventory: ["wip.sh", "notes.md"],
        hazards: ["generated dist/"],
      })
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(Object.isFrozen(result.handoff)).toBe(true);
    expect(Object.isFrozen(result.handoff.untracked_inventory)).toBe(true);
    expect(Object.isFrozen(result.handoff.hazards)).toBe(true);
    expect(() =>
      (result.handoff.untracked_inventory as string[]).push("mutated")
    ).toThrow();
    expect(result.handoff).toEqual(
      handoff({
        tracked_tree_state: "dirty",
        untracked_inventory: ["wip.sh", "notes.md"],
        hazards: ["generated dist/"],
      })
    );
  });

  it("rejects a wrong or missing schema_version as invalid_version", () => {
    for (const candidate of [
      handoff({ schema_version: 2 }),
      {},
      "not-an-object",
      null,
    ]) {
      const result = admitHandoffRecord(candidate);
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected invalid_version");
      expect(result.error.code).toBe("invalid_version");
    }
  });

  it("rejects an empty or non-string repo_head as missing_head", () => {
    for (const repo_head of ["", 42, undefined]) {
      const result = admitHandoffRecord(handoff({ repo_head } as HandoffRecordV1));
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected missing_head");
      expect(result.error.code).toBe("missing_head");
    }
  });

  it("rejects an unknown tracked_tree_state as invalid_tree_state", () => {
    const result = admitHandoffRecord(
      handoff({ tracked_tree_state: "partially_dirty" } as HandoffRecordV1)
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected invalid_tree_state");
    expect(result.error.code).toBe("invalid_tree_state");
  });

  it("rejects a dirty tree with no inventory as dirty_tree_without_inventory", () => {
    for (const untracked_inventory of [undefined, [], null]) {
      const result = admitHandoffRecord(
        handoff({
          tracked_tree_state: "dirty",
          untracked_inventory,
        } as HandoffRecordV1)
      );
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected dirty_tree_without_inventory");
      expect(result.error.code).toBe("dirty_tree_without_inventory");
    }
  });

  it("rejects a malformed inventory as invalid_inventory", () => {
    for (const untracked_inventory of [
      "oops",
      5,
      ["ok", ""],
      ["ok", 7],
    ]) {
      const result = admitHandoffRecord(
        handoff({ untracked_inventory } as HandoffRecordV1)
      );
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected invalid_inventory");
      expect(result.error.code).toBe("invalid_inventory");
    }
  });

  it("rejects malformed hazards as invalid_inventory", () => {
    for (const hazards of ["oops", [5]]) {
      const result = admitHandoffRecord(handoff({ hazards } as HandoffRecordV1));
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected invalid_inventory");
      expect(result.error.code).toBe("invalid_inventory");
    }
  });

  it("defaults a missing hazards field to an empty array", () => {
    const result = admitHandoffRecord(handoff());
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.handoff.hazards).toEqual([]);
  });

  it("rejects missing unfinished_work as missing_unfinished_work", () => {
    for (const unfinished_work of ["", 0]) {
      const result = admitHandoffRecord(
        handoff({ unfinished_work } as HandoffRecordV1)
      );
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected missing_unfinished_work");
      expect(result.error.code).toBe("missing_unfinished_work");
    }
  });

  it("rejects an unknown authored_by as invalid_authored_by", () => {
    const result = admitHandoffRecord(
      handoff({ authored_by: "god" } as HandoffRecordV1)
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected invalid_authored_by");
    expect(result.error.code).toBe("invalid_authored_by");
  });

  it("rejects missing next_step as missing_next_step", () => {
    const result = admitHandoffRecord(handoff({ next_step: "" }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected missing_next_step");
    expect(result.error.code).toBe("missing_next_step");
  });
});

describe("custody transitions", () => {
  it("claims from unowned as a fresh initial hold", () => {
    const held = heldBy(createCustody(worktree()));

    expect(held.state).toBe("held");
    expect(held.mode).toBe("initial");
    expect(held.custodian).toEqual(custodian());
    expect(held.lease_expires_at).toBe(LEASE);
    expect(held.handoff).toBeUndefined();
  });

  it("claims from released as a graceful_handoff retaining handoff provenance", () => {
    const released = apply(
      transitionCustody(heldBy(createCustody(worktree())), {
        type: "release",
        handoff: handoff(),
      })
    );
    const reclaimed = apply(
      transitionCustody(released, {
        type: "claim",
        custodian: custodian({ actor_name: "successor-agent" }),
        lease_expires_at: LEASE,
      })
    );

    expect(reclaimed.mode).toBe("graceful_handoff");
    expect(reclaimed.custodian?.actor_name).toBe("successor-agent");
    expect(reclaimed.handoff).toEqual(handoff());
  });

  it("rejects a claim from held as already_held naming the custodian", () => {
    const held = heldBy(createCustody(worktree()), custodian({ actor_name: "current-holder" }));

    const result = transitionCustody(held, {
      type: "claim",
      custodian: custodian({ actor_name: "intruder" }),
      lease_expires_at: LEASE,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected already_held");
    expect(result.error.code).toBe("already_held");
    expect(result.error.message).toContain("current-holder");
  });

  it("rejects a claim from forfeited as forfeited_requires_takeover", () => {
    const forfeited = apply(
      transitionCustody(heldBy(createCustody(worktree())), {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      })
    );

    const result = transitionCustody(forfeited, {
      type: "claim",
      custodian: custodian(),
      lease_expires_at: LEASE,
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected forfeited_requires_takeover");
    expect(result.error.code).toBe("forfeited_requires_takeover");
  });

  it("assumes a forfeited worktree and clears the dead session's attribution", () => {
    const forfeited = apply(
      transitionCustody(heldBy(createCustody(worktree())), {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      })
    );

    const assumed = apply(
      transitionCustody(forfeited, {
        type: "assume",
        custodian: custodian({ actor_name: "successor-agent" }),
        lease_expires_at: LEASE,
        inventory: ["recovered.json"],
      })
    );

    expect(assumed.state).toBe("held");
    expect(assumed.mode).toBe("successor_takeover");
    expect(assumed.custodian).toEqual(custodian({ actor_name: "successor-agent" }));
    expect(assumed.worktree).toEqual(worktree());
  });

  it("rejects an assume without a non-empty inventory as takeover_requires_inventory", () => {
    const forfeited = apply(
      transitionCustody(heldBy(createCustody(worktree())), {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      })
    );

    for (const inventory of [[], ["ok", ""], [""]]) {
      const result = transitionCustody(forfeited, {
        type: "assume",
        custodian: custodian(),
        lease_expires_at: LEASE,
        inventory,
      });

      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected takeover_requires_inventory");
      expect(result.error.code).toBe("takeover_requires_inventory");
    }
  });

  it("rejects an assume from non-forfeited states as takeover_requires_forfeited", () => {
    const unowned = createCustody(worktree());
    const held = heldBy(unowned);
    const released = apply(
      transitionCustody(held, { type: "release", handoff: handoff() })
    );

    for (const state of [unowned, held, released]) {
      const result = transitionCustody(state, {
        type: "assume",
        custodian: custodian(),
        lease_expires_at: LEASE,
        inventory: ["recovered.json"],
      });

      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected takeover_requires_forfeited");
      expect(result.error.code).toBe("takeover_requires_forfeited");
    }
  });

  it("rejects an invalid custodian or missing lease as invalid_custodian", () => {
    const unowned = createCustody(worktree());

    const badCustodian = transitionCustody(unowned, {
      type: "claim",
      custodian: { actor_name: "", session_id: "s" },
      lease_expires_at: LEASE,
    });
    expect(badCustodian.ok).toBe(false);
    if (badCustodian.ok) throw new Error("expected invalid_custodian");
    expect(badCustodian.error.code).toBe("invalid_custodian");

    const missingLease = transitionCustody(unowned, {
      type: "claim",
      custodian: custodian(),
      lease_expires_at: "",
    });
    expect(missingLease.ok).toBe(false);
    if (missingLease.ok) throw new Error("expected invalid_custodian");
    expect(missingLease.error.code).toBe("invalid_custodian");
  });

  it("releases a held worktree storing the admitted handoff and clearing the custodian", () => {
    const held = heldBy(createCustody(worktree()));
    const released = apply(
      transitionCustody(held, {
        type: "release",
        handoff: handoff({
          tracked_tree_state: "dirty",
          untracked_inventory: ["wip.sh"],
          unfinished_work: "finish refactor",
        }),
      })
    );

    expect(released.state).toBe("released");
    expect(released.custodian).toBeUndefined();
    expect(released.mode).toBeUndefined();
    expect(released.lease_expires_at).toBeUndefined();
    expect(released.handoff).toEqual(
      handoff({
        tracked_tree_state: "dirty",
        untracked_inventory: ["wip.sh"],
        unfinished_work: "finish refactor",
      })
    );
  });

  it("propagates handoff admission errors on release", () => {
    const held = heldBy(createCustody(worktree()));

    const result = transitionCustody(held, {
      type: "release",
      handoff: handoff({ repo_head: "" }),
    });

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected missing_head");
    expect(result.error.code).toBe("missing_head");
  });

  it("rejects a release from non-held states as not_held", () => {
    const unowned = createCustody(worktree());
    const released = apply(
      transitionCustody(heldBy(unowned), { type: "release", handoff: handoff() })
    );
    const forfeited = apply(
      transitionCustody(heldBy(unowned), {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      })
    );

    for (const state of [unowned, released, forfeited]) {
      const result = transitionCustody(state, { type: "release", handoff: handoff() });

      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected not_held");
      expect(result.error.code).toBe("not_held");
    }
  });

  it("leaves a held worktree unchanged when the lease has not expired", () => {
    const held = heldBy(createCustody(worktree()));

    const result = transitionCustody(held, {
      type: "expire",
      now: "2029-12-31T00:00:00.000Z",
    });

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.state).toBe(held);
    expect(result.state.state).toBe("held");

    const atExpiry = transitionCustody(held, { type: "expire", now: LEASE });
    expect(atExpiry.ok).toBe(true);
    if (!atExpiry.ok) throw new Error(atExpiry.error.message);
    expect(atExpiry.state).toBe(held);
  });

  it("forfeits an expired held worktree clearing the custodian but keeping provenance", () => {
    const released = apply(
      transitionCustody(heldBy(createCustody(worktree())), {
        type: "release",
        handoff: handoff(),
      })
    );
    const reclaimedHeld = heldBy(released, custodian({ actor_name: "second-agent" }));
    const result = apply(
      transitionCustody(reclaimedHeld, {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      })
    );

    expect(result.state).toBe("forfeited");
    expect(result.custodian).toBeUndefined();
    expect(result.mode).toBeUndefined();
    expect(result.lease_expires_at).toBeUndefined();
    expect(result.worktree).toEqual(worktree());
    expect(result.handoff).toEqual(handoff());
  });

  it("rejects an unparseable expiry timestamp as invalid_timestamp", () => {
    const held = heldBy(createCustody(worktree()));

    for (const now of ["not-a-date", "2030-13-40T00:00:00.000Z"]) {
      const result = transitionCustody(held, { type: "expire", now });

      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected invalid_timestamp");
      expect(result.error.code).toBe("invalid_timestamp");
    }
  });

  it("leaves released and unowned worktrees unchanged on expiry", () => {
    const unowned = createCustody(worktree());
    const released = apply(
      transitionCustody(heldBy(unowned), { type: "release", handoff: handoff() })
    );

    for (const state of [unowned, released]) {
      const result = transitionCustody(state, {
        type: "expire",
        now: "2031-01-01T00:00:00.000Z",
      });

      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error(result.error.message);
      expect(result.state).toBe(state);
    }
  });

  it("never mutates an input record across transitions", () => {
    const initial = createCustody(worktree());
    const held = heldBy(initial);

    expect(Object.isFrozen(initial)).toBe(true);
    expect(Object.isFrozen(held)).toBe(true);
    expect(Object.isFrozen(initial.worktree)).toBe(true);
    expect(initial).toEqual(createCustody(worktree()));
    expect(held.state).toBe("held");
  });
});
