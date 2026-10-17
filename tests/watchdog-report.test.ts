/** W5 (quiet journal) and W6 (status view): pure formatting and health evaluation. */
import { describe, expect, it } from "vitest";
import { evaluateWatchdogStatus, formatWatchdogOutput, parseWatchdogArgs } from "../src/core/watchdog-report.js";
import type { WatchdogReport, WatchdogResult } from "../src/core/watchdog-shell.js";

const AT = "2030-01-01T00:00:00.000Z";
const report = (overrides: Partial<WatchdogReport> = {}): WatchdogReport => ({
  at: AT,
  leads: [
    { lead: "a", state: { kind: "waiting", on: "w", for: "t", subject: "s", since: AT }, sw4rm_state: "WAITING" },
    { lead: "b", state: { kind: "idle", since: AT }, sw4rm_state: "RUNNABLE" },
    { lead: "c", state: { kind: "stalled", assignment: "t", from: "f", subject: "s", since: AT }, sw4rm_state: "WAITING" },
    { lead: "d", state: { kind: "running", since: AT }, sw4rm_state: "RUNNING" },
  ],
  actions: [], deferred: 0, uncorrelated: 2, rate_limited: 0, dry_run: false, operator_alerts: { status: "disabled" },
  ...overrides,
});
const ok = (r: WatchdogReport): WatchdogResult => ({ exitCode: 0, output: r });

describe("W5: quiet journal", () => {
  it("prints one compact line for a quiet pass", () => {
    expect(formatWatchdogOutput(ok(report()), false)).toBe(`gptqueue watchdog: ok at=${AT} leads=4 waiting=1 stalled=1 idle=1 uncorrelated=2`);
  });

  it.each([
    ["an action", { actions: [{ kind: "remind", rule: 1, to: "w", fire_key: "k", text: "x", status: "done" }] }],
    ["deferred work", { deferred: 1 }],
    ["rate limited work", { rate_limited: 1 }],
  ] as const)("prints the full JSON report when there is %s", (_label, overrides) => {
    const r = report(overrides as Partial<WatchdogReport>);
    expect(formatWatchdogOutput(ok(r), false)).toBe(JSON.stringify(r));
  });

  it("prints full JSON for errors and when --json forces it", () => {
    expect(formatWatchdogOutput(ok(report()), true)).toBe(JSON.stringify(report()));
    const failed: WatchdogResult = { exitCode: 1, output: { error: "redis_unavailable", reason: "x" } };
    expect(formatWatchdogOutput(failed, false)).toBe(JSON.stringify(failed.output));
  });

  it("parses --json and --status", () => {
    expect(parseWatchdogArgs(["--once", "--json"], {})).toMatchObject({ ok: true, args: { mode: "once", json: true } });
    expect(parseWatchdogArgs(["--status"], {})).toMatchObject({ ok: true, args: { mode: "status" } });
    expect(parseWatchdogArgs(["--status", "--once"], {}).ok).toBe(false);
    expect(parseWatchdogArgs([], {}).ok).toBe(false);
  });
});

describe("W3 (health honesty): status view", () => {
  const now = new Date(Date.parse(AT) + 60_000);
  const substantive = (overrides: Record<string, unknown> = {}) =>
    JSON.stringify({ at: AT, exit: 0, leads: 4, actions: 0, failed: 0, rate_limited: 0, deferred: 0, uncorrelated: 2, ...overrides });
  const skip = JSON.stringify({ at: new Date(Date.parse(AT) + 30_000).toISOString(), exit: 0, skipped: true });

  it("is healthy when the last substantive pass is fresh, exited 0 and had no failed actions", () => {
    expect(evaluateWatchdogStatus(substantive(), null, now)).toMatchObject({ exitCode: 0, status: { state: "healthy", healthy: true, last_pass: { exit: 0 } } });
  });

  it("is degraded (exit 1) when the last substantive pass had failed actions", () => {
    expect(evaluateWatchdogStatus(substantive({ failed: 2 }), null, now)).toMatchObject({ exitCode: 1, status: { state: "degraded", healthy: false } });
  });

  it("a skipped pass after a failed pass stays unhealthy, and the skip is shown separately", () => {
    const r = evaluateWatchdogStatus(substantive({ exit: 1, error: "pass_deadline" }), skip, now);
    expect(r).toMatchObject({ exitCode: 1, status: { state: "unhealthy", last_skip: { skipped: true } } });
    expect(r.status.reason).toContain("exited 1");
  });

  it.each([
    ["no pass recorded", null, "no substantive watchdog pass recorded"],
    ["a stale pass", substantive({ at: new Date(Date.parse(AT) - 180_001 + 60_000).toISOString() }), "older than 3 minutes"],
    ["an unreadable record", "{nope", "unreadable"],
  ])("is unhealthy for %s, with a reason", (_label, raw, reason) => {
    const r = evaluateWatchdogStatus(raw, null, now);
    expect(r.exitCode).toBe(1);
    expect(r.status.state).toBe("unhealthy");
    expect(r.status.reason).toContain(reason);
  });
});
