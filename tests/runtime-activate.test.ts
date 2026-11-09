// f-2916: bounded listener readiness wait for activation (scripts/lib/runtime-activate.mjs).
// Offline: health, time and journal effects are injected fakes — no live
// service is restarted. Covers the controls who-tests-the-harness requires:
// bad, late and unreachable health; rollback with journal capture; pointer
// preconditions; and never inferring the new release's health from the old
// release's logs (the journal window starts at the new restart).
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
// @ts-expect-error untyped .mjs tooling module
import * as ra from "../scripts/lib/runtime-activate.mjs";
// @ts-expect-error untyped .mjs tooling module
import * as rr from "../scripts/lib/runtime-release.mjs";

const scratch = mkdtempSync(join(tmpdir(), "gptq-f2916-"));
const newPrefix = () => {
  const prefix = join(scratch, `prefix-${Math.random().toString(36).slice(2)}`);
  mkdirSync(prefix, { recursive: true });
  mkdirSync(join(prefix, "releases"), { recursive: true });
  return prefix;
};
/** Plant a release directory with a verify.json so activate()/status() accept it. */
const plantRelease = (prefix: string, id: string) => {
  const dir = join(prefix, "releases", id);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "verify.json"), JSON.stringify({ ok: true, at: "test" }));
  return dir;
};
/** Fake clock: advances `stepMs` per now() call after the first. */
const fakeClock = (stepMs: number) => { let t = 1_000_000; const now = () => { const v = t; t += stepMs; return v; }; return { now }; };
const noSleep = () => Promise.resolve();

/** Effect recorder with a scripted health sequence. */
const makeEffects = (healthScript: Array<{ statusCode: number | null; body: string }>, journalText = "nov 09 … listening on 8101\n") => {
  const calls = { restarts: [] as string[], journals: [] as { service: string; since: string }[], healthProbes: 0 };
  return {
    calls,
    restart: (service: string) => { calls.restarts.push(service); return { restartedAt: `T${calls.restarts.length}` }; },
    journal: (service: string, since: string) => { calls.journals.push({ service, since }); return { ok: true, text: journalText }; },
    unitFacts: () => ({ ok: true, text: "ExecStart=/node <data-dir>/gptqueue/current/dist/transports/http.js" }),
    preflight: () => ({ ok: true }),
    fetchHealth: async () => { calls.healthProbes += 1; return healthScript[Math.min(healthScript.length - 1, calls.healthProbes - 1)]; },
  };
};

describe("evaluateHealth (pure decision core)", () => {
  it("healthy only on HTTP 200 with status ok", () => {
    expect(ra.evaluateHealth({ statusCode: 200, body: '{"status":"ok","sessions":0}' })).toEqual({ ok: true, reason: "ok" });
  });
  it("connection failure (the f-2916 false-rollback mode) is unreachable", () => {
    expect(ra.evaluateHealth({ statusCode: null, body: "ECONNREFUSED" })).toEqual({ ok: false, reason: "unreachable" });
  });
  it("non-200, non-json and non-ok statuses each fail closed", () => {
    expect(ra.evaluateHealth({ statusCode: 503, body: '{}' }).reason).toBe("http_503");
    expect(ra.evaluateHealth({ statusCode: 200, body: 'not json' }).reason).toBe("invalid_json");
    expect(ra.evaluateHealth({ statusCode: 200, body: '{"status":"error"}' }).reason).toBe('status_"error"');
    expect(ra.evaluateHealth({ statusCode: 200, body: '{}' }).reason).toBe("status_null");
  });
});

describe("pollUntilHealthy (bounded loop)", () => {
  const base = { sleep: noSleep, intervalMs: 250, deadlineMs: 15_000 };

  it("succeeds on first probe", async () => {
    const { now } = fakeClock(0);
    const r = await ra.pollUntilHealthy({ ...base, now, startAtMs: now(), probe: async () => ({ statusCode: 200, body: '{"status":"ok"}' }) });
    expect(r).toMatchObject({ ok: true, attempts: 1, lastReason: "ok" });
  });
  it("keeps polling through pre-readiness connection failures and recovers (the core fix)", async () => {
    let n = 0;
    const { now } = fakeClock(300);
    const r = await ra.pollUntilHealthy({
      ...base, now, startAtMs: now(),
      probe: async () => { n += 1; return n < 4 ? { statusCode: null, body: "refused" } : { statusCode: 200, body: '{"status":"ok"}' }; },
    });
    expect(r).toMatchObject({ ok: true, attempts: 4, lastReason: "ok" });
  });
  it("gives up when the deadline expires while unhealthy", async () => {
    const { now } = fakeClock(2_000); // each poll advances the clock 2s; deadline 5s
    const r = await ra.pollUntilHealthy({ ...base, now, deadlineMs: 5_000, startAtMs: now(), probe: async () => ({ statusCode: 503, body: "{}" }) });
    expect(r.ok).toBe(false);
    expect(r.lastReason).toBe("http_503");
    expect(r.elapsedMs).toBeGreaterThanOrEqual(0);
  });
  it("late health inside the deadline still succeeds", async () => {
    let n = 0;
    const { now } = fakeClock(500);
    const r = await ra.pollUntilHealthy({
      ...base, now, startAtMs: now(),
      probe: async () => { n += 1; return n === 10 ? { statusCode: 200, body: '{"status":"ok"}' } : { statusCode: null, body: "refused" }; },
    });
    expect(r).toMatchObject({ ok: true, attempts: 10 });
  });
  it("a healthy verdict that arrives after the deadline fails as late, not ok", async () => {
    let probed = false;
    const now = () => (probed ? 3_000 : 0); // the probe itself crosses the deadline
    const r = await ra.pollUntilHealthy({
      ...base, now, deadlineMs: 2_000, startAtMs: 0,
      probe: async () => { probed = true; return { statusCode: 200, body: '{"status":"ok"}' }; },
    });
    expect(r).toMatchObject({ ok: false, lastReason: "late_ok_after_deadline", attempts: 1, elapsedMs: 3_000 });
  });
  it("a blocking probe that exhausts the deadline terminates on the next pre-check", async () => {
    const { now } = fakeClock(3_000);
    let n = 0;
    const r = await ra.pollUntilHealthy({
      ...base, now, deadlineMs: 4_000, startAtMs: now(),
      probe: async () => { n += 1; return { statusCode: null, body: "refused" }; },
    });
    expect(r).toMatchObject({ ok: false, attempts: 1, lastReason: "unreachable" });
  });
  it("rejects intervalMs >= deadlineMs, which would allow a single pre-readiness probe", async () => {
    const probe = async () => ({ statusCode: 200, body: '{"status":"ok"}' });
    const { now } = fakeClock(100);
    await expect(ra.pollUntilHealthy({ ...base, now, startAtMs: now(), probe, deadlineMs: 250, intervalMs: 250 }))
      .rejects.toMatchObject({ code: "bad_argument" });
  });
  it("rejects NaN or non-positive deadline/interval instead of looping forever", async () => {
    const probe = async () => ({ statusCode: 200, body: '{"status":"ok"}' });
    const { now } = fakeClock(100);
    await expect(ra.pollUntilHealthy({ ...base, now, startAtMs: now(), probe, deadlineMs: NaN }))
      .rejects.toMatchObject({ code: "bad_argument" });
    await expect(ra.pollUntilHealthy({ ...base, now, startAtMs: now(), probe, intervalMs: Number("abc") }))
      .rejects.toMatchObject({ code: "bad_argument" });
    await expect(ra.pollUntilHealthy({ ...base, now, startAtMs: now(), probe, deadlineMs: 0 }))
      .rejects.toMatchObject({ code: "bad_argument" });
  });
});

describe("activateWithReadiness", () => {
  const goodScript = [{ statusCode: 200, body: '{"status":"ok"}' }];

  it("happy path: activates, restarts once, waits for health, captures the new release's journal window", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects(goodScript);
    const { now } = fakeClock(100);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "http://x/health", effects, now, sleep: noSleep });
    expect(result.activation).toEqual({ current: "new", previous: "old", changed: true });
    expect(result.readiness).toMatchObject({ ok: true, lastReason: "ok" });
    expect(result.rollback).toBeUndefined();
    expect(effects.calls.restarts).toEqual(["mcp-gptqueue.service"]);
    // Journal window is keyed to the NEW restart, not the activation clock.
    expect(effects.calls.journals).toEqual([{ service: "mcp-gptqueue.service", since: "T1" }]);
    expect(rr.status(prefix).current).toBe("new");
  });

  it("unreachable-then-healthy health no longer causes a false rollback", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects([
      { statusCode: null, body: "refused" },
      { statusCode: null, body: "refused" },
      { statusCode: 200, body: '{"status":"ok"}' },
    ]);
    const { now } = fakeClock(300);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep });
    expect(result.readiness).toMatchObject({ ok: true, attempts: 3 });
    expect(result.rollback).toBeUndefined();
    expect(rr.status(prefix).current).toBe("new");
  });

  it("deadline expiry rolls the pointer back and restarts the previous release", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    const { now } = fakeClock(1_000);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 1_000 });
    expect(result.readiness.ok).toBe(false);
    expect(result.readiness.lastReason).toBe("unreachable");
    expect(result.error).toBe("readiness_deadline");
    // Rollback happened and restarted the old release exactly once more.
    expect(result.rollback).toMatchObject({ current: "old", previous: "new", restart: { ok: true } });
    expect(effects.calls.restarts.length).toBe(2);
    expect(rr.status(prefix).current).toBe("old");
    // The failed attempt's journal was still captured before rollback.
    expect(effects.calls.journals).toHaveLength(1);
  });

  it("readiness failure with a concurrently moved pointer reports instead of rolling back over it", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new"); plantRelease(prefix, "other");
    rr.activate(prefix, "old");
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    const realFetch = effects.fetchHealth;
    effects.fetchHealth = async (...a) => { const r = await realFetch(...a); rr.activate(prefix, "other"); return r; };
    const { now } = fakeClock(1_000);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 4_000 });
    expect(result.error).toBe("concurrent_pointer_change");
    expect(result.rollback).toBeUndefined();
    expect(rr.status(prefix).current).toBe("other");
  });
  it("already-current activation reports unhealthy listener as a failure, not success", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "new");
    rr.activate(prefix, "new");
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    const { now } = fakeClock(1_000);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 500 });
    expect(result.error).toBe("already_current_unhealthy");
    expect(result.readiness.ok).toBe(false);
    expect(result.rollback).toBeUndefined(); // nothing was activated; no rollback authority
  });
  it("environment preflight failure aborts before any pointer move", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects(goodScript);
    effects.preflight = () => { throw new (rr.ReleaseError ?? Error)("bad_environment", "XDG_RUNTIME_DIR is not set"); };
    await expect(ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects }))
      .rejects.toThrow(/XDG_RUNTIME_DIR/);
    expect(rr.status(prefix).current).toBe("old"); // pointer untouched
    expect(effects.calls.restarts).toEqual([]);
  });
  it("restart failure rolls back and reports restart_failed without probing", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects(goodScript);
    effects.restart = () => { throw new Error("unit not found"); };
    const { now } = fakeClock(100);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep });
    expect(result.error).toBe("restart_failed");
    expect(result.readiness.ok).toBe(false);
    expect(effects.calls.healthProbes).toBe(0);
    expect(rr.status(prefix).current).toBe("old");
  });

  it("rejects intervalMs >= deadlineMs before any pointer move", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects([{ statusCode: 200, body: '{"status":"ok"}' }]);
    await expect(ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, deadlineMs: 250, intervalMs: 250 }))
      .rejects.toMatchObject({ code: "bad_argument" });
    expect(rr.status(prefix).current).toBe("old"); // pointer untouched
    expect(effects.calls.restarts).toEqual([]);
  });
  it("refuses to activate on top of an unexpected current pointer", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "a"); plantRelease(prefix, "b"); plantRelease(prefix, "new");
    rr.activate(prefix, "a");
    const effects = makeEffects(goodScript);
    await expect(ra.activateWithReadiness({ prefix, id: "new", expectCurrent: "b", healthUrl: "u", effects }))
      .rejects.toMatchObject({ code: "unexpected_current" });
    expect(rr.status(prefix).current).toBe("a");
    expect(effects.calls.restarts).toEqual([]);
  });

  it("refuses an unknown release id and a no-op activation of the current release", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "new");
    rr.activate(prefix, "new");
    const effects = makeEffects(goodScript);
    const { now } = fakeClock(100);
    await expect(ra.activateWithReadiness({ prefix, id: "missing", healthUrl: "u", effects }))
      .rejects.toMatchObject({ code: "bad_argument" });
    const noop = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep });
    expect(noop.readiness).toMatchObject({ ok: true }); // health probed, not assumed
    expect(noop.error).toBeUndefined();
    expect(effects.calls.restarts).toEqual([]);
  });

  it("a missing previous pointer during rollback is reported, not thrown, with readiness evidence intact", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "new");
    // current is unset, so activation sets current without a previous pointer.
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    const { now } = fakeClock(1_000);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 500 });
    expect(result.error).toBe("readiness_deadline");
    expect(result.readiness.ok).toBe(false);
    expect(result.journal.ok).toBe(true);
    expect(result.rollback).toMatchObject({ failed: true, error: expect.stringContaining("no_previous") });
  });
  it("rollback restart failure still reports the pointer rollback", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    let restarts = 0;
    effects.restart = () => { restarts += 1; if (restarts === 2) throw new Error("second restart failed"); return { restartedAt: `T${restarts}` }; };
    const { now } = fakeClock(1_000);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 500 });
    expect(result.rollback).toMatchObject({ current: "old", previous: "new", restart: { ok: false, error: "second restart failed" } });
    expect(rr.status(prefix).current).toBe("old");
  });

  it("flags a concurrent pointer change during the readiness wait without stomping it", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new"); plantRelease(prefix, "other");
    rr.activate(prefix, "old");
    const effects = makeEffects(goodScript);
    const realFetch = effects.fetchHealth;
    effects.fetchHealth = async (...a) => { const r = await realFetch(...a); rr.activate(prefix, "other"); return r; };
    const { now } = fakeClock(100);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep });
    expect(result.error).toBe("concurrent_pointer_change");
    expect(result.concurrent).toEqual({ expected: "new", actual: "other" });
    expect(result.rollback).toBeUndefined(); // the concurrent actor owns the pointer
    expect(rr.status(prefix).current).toBe("other");
  });
  it("rolls back when the pointer is cleared (not moved) during a failed readiness wait", async () => {
    const prefix = newPrefix();
    plantRelease(prefix, "old"); plantRelease(prefix, "new");
    rr.activate(prefix, "old");
    const effects = makeEffects([{ statusCode: null, body: "refused" }]);
    const realFetch = effects.fetchHealth;
    effects.fetchHealth = async (...a) => { const r = await realFetch(...a); rmSync(join(prefix, "current")); return r; };
    const { now } = fakeClock(100);
    const result = await ra.activateWithReadiness({ prefix, id: "new", healthUrl: "u", effects, now, sleep: noSleep, deadlineMs: 500 });
    expect(result.error).toBe("readiness_deadline");
    expect(result.concurrent).toBeUndefined(); // a cleared pointer has no owner to defer to
    expect(result.readiness.attempts).toBeGreaterThan(0); // the clear happened during a real probe
    expect(result.rollback).toMatchObject({ current: "old", restart: { ok: true }, pointerClearedDuringReadiness: true });
    expect(result.rollback.previous).toBeUndefined(); // the cleared pointer cannot become previous
    expect(rr.status(prefix).current).toBe("old");
  });
});

describe("releaseProvenance", () => {
  it("reads verify.json when present and undefined otherwise", () => {
    const prefix = newPrefix();
    plantRelease(prefix, "r1");
    expect(ra.releaseProvenance(prefix, "r1")).toEqual({ ok: true, at: "test" });
    expect(ra.releaseProvenance(prefix, "missing")).toBeUndefined();
  });
});

