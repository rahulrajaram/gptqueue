import { describe, it, expect } from "vitest";
import type { ActorDirectoryRecord } from "../src/core/actor-directory.js";
import {
  wakeEligible,
  wakeDecisionForPresence,
  type WakePresenceDecision,
} from "../src/mcp-server/tools/send-message.js";
import type { RuntimePresenceState } from "../src/core/actor-presence.js";

/**
 * Focused unit tests for the decomposed maybeWake functions. These exercise the
 * pure decision table and the eligibility gate WITHOUT duplicating the e2e
 * coverage in tests/wake-e2e.test.ts / activation.integration.test.ts (which
 * pin the full wire behavior against a live redis/db15).
 */

type ActivationPolicyMode = "wake_if_offline" | "store_only";

function makeRecord(
  mode: ActivationPolicyMode,
  actorId = "act"
): ActorDirectoryRecord {
  return {
    profile: {
      actor_id: actorId,
      alias: actorId,
      capabilities: [],
      workspace_root: "/workspace",
      working_directory: "/workspace",
      runtime: "node",
      activation_policy: { mode },
      max_concurrency: 1,
    },
    launch: mode === "wake_if_offline" ? { command: "node", args: [] } : null,
    registered_by: "sess",
    registered_at: "2030-01-01T00:00:00.000Z",
  };
}

describe("wakeEligible (wake policy gate)", () => {
  it("returns undefined when there is no directory record (plain agent)", () => {
    expect(wakeEligible(null)).toBeUndefined();
  });

  it("returns undefined for a store_only durable actor", () => {
    expect(wakeEligible(makeRecord("store_only"))).toBeUndefined();
  });

  it("returns the record for a wake_if_offline durable actor", () => {
    const record = makeRecord("wake_if_offline", "wakee");
    expect(wakeEligible(record)).toEqual({ record });
  });
});

describe("wakeDecisionForPresence (exhaustive presence -> wake mapping)", () => {
  const cases: ReadonlyArray<{
    presence: RuntimePresenceState;
    hasWakeLease: boolean;
    expected: WakePresenceDecision;
  }> = [
    { presence: "active", hasWakeLease: true, expected: { kind: "no_wake" } },
    { presence: "active", hasWakeLease: false, expected: { kind: "no_wake" } },
    { presence: "idle", hasWakeLease: true, expected: { kind: "no_wake" } },
    { presence: "idle", hasWakeLease: false, expected: { kind: "no_wake" } },
    { presence: "offline_store_only", hasWakeLease: true, expected: { kind: "no_wake" } },
    { presence: "offline_store_only", hasWakeLease: false, expected: { kind: "no_wake" } },
    { presence: "unavailable", hasWakeLease: true, expected: { kind: "no_wake" } },
    { presence: "unavailable", hasWakeLease: false, expected: { kind: "no_wake" } },
    { presence: "starting", hasWakeLease: true, expected: { kind: "coalesce" } },
    { presence: "starting", hasWakeLease: false, expected: { kind: "no_wake" } },
    { presence: "offline_launchable", hasWakeLease: false, expected: { kind: "launch" } },
    { presence: "offline_launchable", hasWakeLease: true, expected: { kind: "launch" } },
  ];

  for (const c of cases) {
    it(`maps ${c.presence}${c.hasWakeLease ? " (lease)" : " (no lease)"} -> ${c.expected.kind}`, () => {
      expect(wakeDecisionForPresence(c.presence, c.hasWakeLease)).toEqual(
        c.expected
      );
    });
  }
});
