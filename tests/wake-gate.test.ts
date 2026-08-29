import { describe, it, expect } from "vitest";
import type { ActorDirectoryRecord } from "../src/core/actor-directory.js";
import {
  gateWakeEligibility,
  wakeDecisionForPresence,
  type WakePresenceDecision,
} from "../src/mcp-server/tools/send-message.js";
import type { RuntimePresenceState } from "../src/core/actor-presence.js";
import type { ActorGetResult } from "../src/core/actor-directory.js";

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

type DirectoryOnlyClient = Parameters<typeof gateWakeEligibility>[0];

function clientWithDirResult(
  result: ActorGetResult
): DirectoryOnlyClient {
  return { actorDirectory: { get: async () => result } } as DirectoryOnlyClient;
}

describe("gateWakeEligibility (wake gate rejection path)", () => {
  it("returns undefined on a corrupt/unreadable directory", async () => {
    const client = clientWithDirResult({
      ok: false,
      error: { code: "store_corrupt", message: "boom" },
    });
    await expect(gateWakeEligibility(client, "x")).resolves.toBeUndefined();
  });

  it("returns undefined when there is no directory record (plain agent)", async () => {
    const client = clientWithDirResult({ ok: true, record: null });
    await expect(gateWakeEligibility(client, "plain")).resolves.toBeUndefined();
  });

  it("returns undefined for a store_only durable actor", async () => {
    const client = clientWithDirResult({ ok: true, record: makeRecord("store_only") });
    await expect(gateWakeEligibility(client, "storeonly")).resolves.toBeUndefined();
  });

  it("returns the record for a wake_if_offline durable actor", async () => {
    const record = makeRecord("wake_if_offline", "wakee");
    const client = clientWithDirResult({ ok: true, record });
    await expect(gateWakeEligibility(client, "wakee")).resolves.toEqual({
      record,
    });
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
