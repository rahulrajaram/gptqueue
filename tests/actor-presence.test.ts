import { describe, expect, it } from "vitest";
import {
  admitActorProfile,
  classifyPresence,
  type ActivationPolicy,
  type DurableActorProfile,
  type LaunchContractReadiness,
  type PresenceInput,
  type RuntimeIncarnation,
} from "../src/core/actor-presence.js";

const policy = (mode: ActivationPolicy["mode"]): ActivationPolicy => ({
  mode,
});

const profile = (overrides: Partial<DurableActorProfile> = {}): DurableActorProfile => ({
  actor_id: "actor-1",
  alias: "metabuilder",
  capabilities: ["build"],
  workspace_root: "/workspace",
  working_directory: "/workspace/actor",
  runtime: "pi",
  activation_policy: policy("wake_if_offline"),
  max_concurrency: 1,
  ...overrides,
});

const runtime = (overrides: Partial<RuntimeIncarnation> = {}): RuntimeIncarnation => ({
  incarnation_id: "incarnation-1",
  lease_id: "lease-1",
  workload: "processing",
  ...overrides,
});

const input = (overrides: Partial<PresenceInput> = {}): PresenceInput => ({
  actor: profile(),
  launch_contract: "runnable",
  runtime: undefined,
  ...overrides,
});

describe("actor presence classification", () => {
  it("classifies a registered offline launchable actor as offline_launchable", () => {
    const result = classifyPresence(
      input({ actor: profile({ activation_policy: policy("wake_if_offline") }) })
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.actor_id).toBe("actor-1");
    expect(result.presence).toBe("offline_launchable");
  });

  it("rejects an unknown recipient without implying any created state", () => {
    const result = classifyPresence(input({ actor: undefined }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected unknown_recipient");
    expect(result.error.code).toBe("unknown_recipient");
    expect(result.error.message).toContain("no mailbox or queue data was created");
  });

  it("classifies a store_only actor with a runnable contract as offline_store_only", () => {
    const result = classifyPresence(
      input({ actor: profile({ activation_policy: policy("store_only") }) })
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.presence).toBe("offline_store_only");
  });

  it("classifies a not_runnable contract as unavailable regardless of policy", () => {
    for (const mode of ["wake_if_offline", "store_only"] as const) {
      const result = classifyPresence(
        input({
          actor: profile({ activation_policy: policy(mode) }),
          launch_contract: "not_runnable" satisfies LaunchContractReadiness,
        })
      );

      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error(result.error.message);
      expect(result.presence).toBe("unavailable");
    }
  });

  it("classifies a leased runtime by its workload", () => {
    const processing = classifyPresence(input({ runtime: runtime() }));
    expect(processing.ok).toBe(true);
    if (!processing.ok) throw new Error(processing.error.message);
    expect(processing.presence).toBe("active");

    const idle = classifyPresence(
      input({ runtime: runtime({ workload: "idle" }) })
    );
    expect(idle.ok).toBe(true);
    if (!idle.ok) throw new Error(idle.error.message);
    expect(idle.presence).toBe("idle");
  });

  it("classifies an outstanding wake lease without a runtime as starting", () => {
    const result = classifyPresence(input({ wake_lease_id: "wake-1" }));

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.presence).toBe("starting");
  });

  it("ignores a runtime observation without a lease_id and falls back to offline", () => {
    const result = classifyPresence(
      input({ runtime: runtime({ lease_id: undefined }) })
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.presence).toBe("offline_launchable");
  });

  it("prefers a leased runtime over an outstanding wake lease", () => {
    const shared = { wake_lease_id: "wake-1" } as const;

    const processing = classifyPresence(
      input({ ...shared, runtime: runtime() })
    );
    expect(processing.ok).toBe(true);
    if (!processing.ok) throw new Error(processing.error.message);
    expect(processing.presence).toBe("active");

    const idle = classifyPresence(
      input({ ...shared, runtime: runtime({ workload: "idle" }) })
    );
    expect(idle.ok).toBe(true);
    if (!idle.ok) throw new Error(idle.error.message);
    expect(idle.presence).toBe("idle");
  });

  it("prefers an outstanding wake lease over a non-runnable contract", () => {
    const result = classifyPresence(
      input({ launch_contract: "not_runnable", wake_lease_id: "wake-1" })
    );

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.presence).toBe("starting");
  });

  it("does not mutate the presence input", () => {
    const actor = profile({ run_directory: "/state/actor-1/run" });
    const observation = runtime({ session_id: "session-1" });
    const original: PresenceInput = {
      actor,
      launch_contract: "runnable",
      runtime: observation,
      wake_lease_id: "wake-1",
    };
    const snapshot = structuredClone({ actor, observation, original });

    const result = classifyPresence(original);
    expect(result.ok).toBe(true);

    expect(structuredClone(original)).toEqual(snapshot.original);
    expect(structuredClone(actor)).toEqual(snapshot.actor);
    expect(structuredClone(observation)).toEqual(snapshot.observation);
  });
});

describe("durable actor profile admission", () => {
  it("rejects an empty actor_id as invalid_identity", () => {
    const result = admitActorProfile(profile({ actor_id: "" }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected invalid_identity");
    expect(result.error.code).toBe("invalid_identity");
  });

  it("rejects an empty alias as invalid_identity", () => {
    const result = admitActorProfile(profile({ alias: "" }));

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected invalid_identity");
    expect(result.error.code).toBe("invalid_identity");
  });

  it("rejects an unknown policy mode as invalid_policy", () => {
    const result = admitActorProfile(
      profile({
        activation_policy: { mode: "wake_on_arrival" } as unknown as ActivationPolicy,
      })
    );

    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected invalid_policy");
    expect(result.error.code).toBe("invalid_policy");
  });

  it("rejects identities outside the [A-Za-z0-9._-]{1,64} charset as invalid_identity_charset", () => {
    expect(
      admitActorProfile(profile({ actor_id: "has space" })).ok
    ).toBe(false);
    if (admitActorProfile(profile({ actor_id: "has space" })).ok) {
      throw new Error("expected invalid_identity_charset");
    }
    expect(
      admitActorProfile(profile({ actor_id: "has space" }))
    ).toMatchObject({ ok: false, error: { code: "invalid_identity_charset" } });

    // alias rejects embedded path separators / injection-special chars.
    expect(
      admitActorProfile(profile({ alias: "../../etc" }))
    ).toMatchObject({ ok: false, error: { code: "invalid_identity_charset" } });

    // 65 chars exceeds the length bound.
    expect(
      admitActorProfile(profile({ actor_id: "a".repeat(65) }))
    ).toMatchObject({ ok: false, error: { code: "invalid_identity_charset" } });
  });

  it("admits identities using the full allowed charset", () => {
    const result = admitActorProfile(profile({ actor_id: "A.b_c-9", alias: "x.y_z-1" }));
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    expect(result.profile.actor_id).toBe("A.b_c-9");
  });

  it("rejects invalid max_concurrency as invalid_concurrency", () => {
    for (const max_concurrency of [0, 1.5]) {
      const result = admitActorProfile(profile({ max_concurrency }));

      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("expected invalid_concurrency");
      expect(result.error.code).toBe("invalid_concurrency");
    }
  });

  it("admits a valid profile deeply frozen", () => {
    const result = admitActorProfile(profile());

    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error(result.error.message);
    const { profile: admitted } = result;
    expect(Object.isFrozen(admitted)).toBe(true);
    expect(Object.isFrozen(admitted.capabilities)).toBe(true);
    expect(Object.isFrozen(admitted.activation_policy)).toBe(true);
  });
});

describe("delivery mode versus activation policy", () => {
  it("keeps a store_only delivery from changing a launchable actor's presence", () => {
    const presence = classifyPresence(input());
    expect(presence.ok).toBe(true);
    if (!presence.ok) throw new Error(presence.error.message);
    expect(presence.presence).toBe("offline_launchable");
  });

  it("classifies a store_only-policy actor as offline_store_only independent of delivery mode", () => {
    const presence = classifyPresence(
      input({ actor: profile({ activation_policy: policy("store_only") }) })
    );

    expect(presence.ok).toBe(true);
    if (!presence.ok) throw new Error(presence.error.message);
    expect(presence.presence).toBe("offline_store_only");
  });
});
