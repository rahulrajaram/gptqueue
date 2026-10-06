/** LT-0..LT-4 for the codex runtime; see latency.ts for hops, env switches and targets. */
import { describe, expect, it } from "vitest";
import { assertTargets, measureRuntime } from "./latency.js";

describe("codex latency outside the agent loop", () => {
  it("records every hop (LT-0) and reports p50/p95/p99 (LT-1..LT-4)", async () => {
    const result = await measureRuntime("codex");
    for (const summary of Object.values(result.hops)) expect(Number.isFinite(summary.p95)).toBe(true);
    assertTargets(result);
  }, 120_000);
});
