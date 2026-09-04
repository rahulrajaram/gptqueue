/**
 * Focused communication-lifecycle integration suite.
 *
 * Drives the same deterministic scenario engine the MetaBuilder
 * communication-lifecycle-v1 harness cites as evidence actions
 * (harness/lifecycle/run-scenario.mjs) and asserts the summarized
 * invariants: H1-H4 topology, H5 idempotency, H6/H7 continuity,
 * H8 cleanup, H9 backpressure — three consecutive bounded rounds each.
 *
 * The engine spawns its own isolated fixtures: an in-sandbox-style
 * `redis-server` on a private Unix socket under /tmp (never db0, never the
 * live server) and a `dist/transports/http.js` child listening on its own
 * Unix socket. No TCP, no live server contact.
 */

import { describe, it, expect } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

// Several integration scenarios intentionally wait out leases and blocking
// receives; the default vitest timeout is far too tight for them.
const SCENARIO_TIMEOUT = 600_000;

const RUNNER = "harness/lifecycle/run-scenario.mjs";

async function runScenario(scenario: string, rounds = 3) {
  const { stdout } = await execFileAsync(process.execPath, [RUNNER, scenario, "--rounds", String(rounds)], {
    cwd: process.cwd(),
    timeout: SCENARIO_TIMEOUT - 5_000,
  });
  const trimmed = stdout.trim();
  const newlineIdx = trimmed.lastIndexOf("\n");
  const summaryLine = newlineIdx === -1 ? trimmed : trimmed.slice(newlineIdx + 1);
  return JSON.parse(summaryLine) as {
    scenario: string;
    rounds: number;
    roundsResults: Array<Record<string, unknown>>;
    ok: boolean;
  };
}

describe("communication lifecycle (H1-H9, deterministic UDS fixtures)", () => {
  it(
    "topology: registration, readiness, 1→1, 1→N fan-out, and the 3×2 matrix deliver exactly across three consecutive rounds",
    async () => {
      const summary = await runScenario("topology");
      expect(summary.ok).toBe(true);
      expect(summary.rounds).toBe(3);
      expect(summary.roundsResults).toHaveLength(3);
      for (const round of summary.roundsResults) {
        expect(round["readinessBarrier"]).toBe("passed");
        expect(round["distinctSessionIds"]).toBe(6);
        const oneToOne = round["oneToOne"] as Record<string, number>;
        expect(oneToOne["edges"]).toBe(1);
        expect(oneToOne["delivered"]).toBe(1);
        const oneToMany = round["oneToMany"] as Record<string, number>;
        expect(oneToMany["sendCalls"]).toBe(oneToMany["edges"]);
        expect(oneToMany["delivered"]).toBe(2);
        expect(oneToMany["bystanderDeliveries"]).toBe(0);
        const matrix = round["matrix"] as Record<string, number>;
        expect(matrix["edges"]).toBe(6);
        expect(matrix["delivered"]).toBe(6);
        expect(matrix["missing"]).toBe(0);
        expect(matrix["duplicateLogicalDeliveries"]).toBe(0);
        expect(matrix["crossDeliveries"]).toBe(0);
      }
    },
    SCENARIO_TIMEOUT
  );

  it(
    "idempotency: same key never duplicates a logical delivery; distinct keys stay distinct",
    async () => {
      const summary = await runScenario("idempotency");
      expect(summary.ok).toBe(true);
      for (const round of summary.roundsResults) {
        const sameKey = round["sameKeyRetry"] as Record<string, unknown>;
        expect(sameKey["result"]).toBe("duplicate");
        expect(sameKey["originalMessageIdReturned"]).toBe(true);
        expect(sameKey["logicalDeliveries"]).toBe(1);
        const distinct = round["distinctKey"] as Record<string, unknown>;
        expect(distinct["result"]).toBe("sent");
        expect(distinct["distinctMessageId"]).toBe(true);
        expect(distinct["logicalDeliveries"]).toBe(1);
      }
    },
    SCENARIO_TIMEOUT
  );

  it(
    "continuity: a fresh transport re-binds by session_id without re-registering, and a stale transport recovers through the documented 404",
    async () => {
      const summary = await runScenario("continuity");
      expect(summary.ok).toBe(true);
      for (const round of summary.roundsResults) {
        const rebind = round["statelessRebind"] as Record<string, unknown>;
        expect(rebind["queuedThenReceived"]).toBe(true);
        expect(rebind["reRegistered"]).toBe(false);
        expect(rebind["registryEntriesForReceiver"]).toBe(1);
        const stale = round["staleTransport"] as Record<string, unknown>;
        expect(stale["refusalStatus"]).toBe(404);
        expect(stale["reinitialized"]).toBe(true);
        expect(stale["postRecoveryDelivery"]).toBe(true);
      }
    },
    SCENARIO_TIMEOUT
  );

  it(
    "cleanup: graceful close preserves the mailbox while terminal unregister removes state, and health returns to baseline",
    async () => {
      const summary = await runScenario("cleanup");
      expect(summary.ok).toBe(true);
      for (const round of summary.roundsResults) {
        const graceful = round["gracefulClose"] as Record<string, unknown>;
        expect(graceful["status"]).toBe("session_closed");
        expect(graceful["mailboxPreserved"]).toBe(true);
        expect(graceful["rebindRefused"]).toBe("SESSION_UNAVAILABLE");
        expect(graceful["registrationKept"]).toBe(true);
        expect(graceful["leaseRefreshStopped"]).toBe(true);
        const terminal = round["terminalUnregister"] as Record<string, unknown>;
        expect(terminal["listed"]).toBe(false);
        expect(terminal["rebindRefused"]).toBe("SESSION_UNAVAILABLE");
        expect(terminal["sendRefused"]).toBe("unknown_recipient");
        expect(terminal["mailboxRemoved"]).toBe(true);
        expect(round["healthBaseline"]).toBe(0);
      }
    },
    SCENARIO_TIMEOUT
  );

  it(
    "backpressure: the bounded refusal is typed and observable, and no queued message is silently lost",
    async () => {
      const summary = await runScenario("backpressure");
      expect(summary.ok).toBe(true);
      for (const round of summary.roundsResults) {
        expect(round["queueBound"]).toBe(3);
        const refusal = round["typedRefusal"] as Record<string, unknown>;
        expect(refusal["code"]).toBe("QUEUE_FULL");
        expect(refusal["retryable"]).toBe(true);
        expect(refusal["silentLoss"]).toBe(0);
        const pre = round["preRefusalDeliveries"] as Record<string, unknown>;
        expect(pre["expected"]).toBe(3);
        expect(pre["received"]).toBe(3);
        expect(pre["intact"]).toBe(true);
        expect(round["postDrainDelivery"]).toBe(true);
      }
    },
    SCENARIO_TIMEOUT
  );
});
