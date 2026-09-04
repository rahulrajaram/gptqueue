#!/usr/bin/env node
/**
 * CLI entry point for the communication-lifecycle scenarios.
 *
 * Usage: node harness/lifecycle/run-scenario.mjs <scenario> [--rounds N]
 *
 * Prints one JSON summary object on stdout and exits 0 when every round
 * passed; exits 1 with the failure reason on stderr otherwise. The summary is
 * the evidence artifact the communication-lifecycle-v1 Harness Module cites.
 */

import { runTopologyScenario, runIdempotencyScenario, runContinuityScenario, runCleanupScenario, runBackpressureScenario, runConcurrentInterleavingScenario, runRestartDurabilityScenario } from "./scenarios.mjs";

const SCENARIOS = {
  topology: runTopologyScenario,
  idempotency: runIdempotencyScenario,
  continuity: runContinuityScenario,
  cleanup: runCleanupScenario,
  backpressure: runBackpressureScenario,
  concurrent: runConcurrentInterleavingScenario,
  restart: runRestartDurabilityScenario,
};

function parseArgs(argv) {
  const [scenario, ...rest] = argv;
  if (!scenario || !SCENARIOS[scenario]) {
    console.error(`usage: run-scenario.mjs <${Object.keys(SCENARIOS).join("|")}> [--rounds N]`);
    process.exit(2);
  }
  let rounds = 3;
  const idx = rest.indexOf("--rounds");
  if (idx !== -1) {
    const value = parseInt(rest[idx + 1], 10);
    if (!Number.isInteger(value) || value < 1 || value > 10) {
      console.error("--rounds must be an integer between 1 and 10");
      process.exit(2);
    }
    rounds = value;
  }
  return { scenario, rounds };
}

const { scenario, rounds } = parseArgs(process.argv.slice(2));
try {
  const summary = await SCENARIOS[scenario](rounds);
  process.stdout.write(JSON.stringify(summary) + "\n");
  process.exit(0);
} catch (error) {
  console.error(String(error instanceof Error ? error.stack : error));
  process.exit(1);
}
