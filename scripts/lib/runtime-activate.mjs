// f-2916: bounded listener readiness wait for runtime activation.
//
// The old deployment path restarted the systemd service and fetched /health
// immediately; systemctl's return precedes listener readiness, so a healthy
// new release was falsely rolled back. This module makes activation a
// first-class operation:
//
//   1. refuse unless the current pointer still names `expectCurrent`
//      (no activation on top of an unexpected concurrent change);
//   2. swap the pointer, restart the service, then poll health until
//      HTTP 200 with status "ok" within a declared deadline;
//   3. capture the NEW release's own startup journal for the window
//      [restart, healthy-or-deadline] — never the old release's logs;
//   4. roll back and restart the previous release when readiness fails.
//
// The decision core (health evaluation and the polling loop) is pure over
// injected effects so tests can exercise bad, late and unreachable health
// without a live service.
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ReleaseError, activate, rollback, status } from "./runtime-release.mjs";

export const DEFAULT_DEADLINE_MS = 15_000;
export const DEFAULT_INTERVAL_MS = 250;

/**
 * A single health observation. statusCode null means the request itself
 * failed (connection refused, reset, timeout) — the exact failure mode of
 * the pre-readiness probe that caused the false rollback.
 * @returns {{ ok: boolean, reason: string }}
 */
export function evaluateHealth({ statusCode, body }) {
  if (statusCode === null || statusCode === undefined) return { ok: false, reason: "unreachable" };
  if (statusCode !== 200) return { ok: false, reason: `http_${statusCode}` };
  let payload;
  try {
    payload = typeof body === "string" ? JSON.parse(body) : body;
  } catch {
    return { ok: false, reason: "invalid_json" };
  }
  if (!payload || typeof payload !== "object" || payload.status !== "ok") {
    return { ok: false, reason: `status_${JSON.stringify(payload?.status ?? null)}` };
  }
  return { ok: true, reason: "ok" };
}

/**
 * Poll `probe()` until it reports a healthy observation or the deadline
 * (measured from `startAtMs`) expires. Pure over injected time and sleep.
 * @returns {{ ok: boolean, attempts: number, elapsedMs: number, lastReason: string }}
 */
export async function pollUntilHealthy({ probe, startAtMs, now = Date.now, sleep = setTimeout, deadlineMs = DEFAULT_DEADLINE_MS, intervalMs = DEFAULT_INTERVAL_MS }) {
  if (!Number.isFinite(deadlineMs) || deadlineMs <= 0) throw new ReleaseError("bad_argument", `deadlineMs must be a positive finite number, got ${deadlineMs}`);
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new ReleaseError("bad_argument", `intervalMs must be a positive finite number, got ${intervalMs}`);
  let attempts = 0;
  let lastReason = "not_probed";
  for (;;) {
    // Check the deadline before probing too, so a probe that blocks past the
    // deadline cannot extend the wait or turn a late verdict into success.
    if (now() - startAtMs > deadlineMs) {
      return { ok: false, attempts, elapsedMs: now() - startAtMs, lastReason: lastReason === "ok" ? "late_ok_after_deadline" : lastReason };
    }
    attempts += 1;
    const observation = await probe();
    const verdict = evaluateHealth(observation);
    lastReason = verdict.reason;
    if (verdict.ok) {
      const late = now() - startAtMs > deadlineMs;
      return { ok: !late, attempts, elapsedMs: now() - startAtMs, lastReason: late ? "late_ok_after_deadline" : "ok" };
    }
    if (now() - startAtMs + intervalMs > deadlineMs) {
      return { ok: false, attempts, elapsedMs: now() - startAtMs, lastReason };
    }
    await sleep(intervalMs);
  }
}

/** systemd/journalctl effect bundle; overridable for tests. */
export const systemEffects = Object.freeze({
  restart: (service) => {
    const r = spawnSync("systemctl", ["--user", "restart", service], { encoding: "utf8" });
    if (r.status !== 0) throw new ReleaseError("restart_failed", `systemctl --user restart ${service} failed: ${r.stderr.trim()}`);
    return { restartedAt: new Date().toISOString() };
  },
  journal: (service, sinceIso) => {
    const r = spawnSync("journalctl", ["--user", "-u", service, "--since", sinceIso, "--no-pager", "-o", "short-iso"], { encoding: "utf8" });
    // A journal read failing is evidence loss, not deployment failure.
    return r.status === 0 ? { ok: true, text: r.stdout } : { ok: false, error: r.stderr.trim() };
  },
  fetchHealth: async (url) => {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(2_000) });
      return { statusCode: response.status, body: await response.text() };
    } catch (error) {
      return { statusCode: null, body: String(error?.cause ?? error?.message ?? error) };
    }
  },
});

/**
 * Activate release `id` with a bounded readiness wait and journal capture.
 * Every effect is injectable; production callers pass `systemEffects`.
 * @param {{
 *   prefix: string, id: string,
 *   expectCurrent?: string, service?: string, healthUrl: string,
 *   deadlineMs?: number, intervalMs?: number,
 *   effects?: typeof systemEffects,
 *   now?: () => number, sleep?: (ms: number) => Promise<unknown>,
 * }} options
 * @returns the activation record: activation, readiness, journal and — on
 *   failure — the rollback record. Frozen.
 */
export async function activateWithReadiness(options) {
  const {
    prefix, id, expectCurrent, service = "mcp-gptqueue.service", healthUrl,
    deadlineMs = DEFAULT_DEADLINE_MS, intervalMs = DEFAULT_INTERVAL_MS,
    effects = systemEffects, now = Date.now, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  } = options;
  if (!Number.isFinite(deadlineMs) || deadlineMs <= 0) throw new ReleaseError("bad_argument", `deadlineMs must be a positive finite number, got ${deadlineMs}`);
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new ReleaseError("bad_argument", `intervalMs must be a positive finite number, got ${intervalMs}`);
  const before = status(prefix);
  if (expectCurrent !== undefined && before.current !== expectCurrent) {
    throw new ReleaseError("unexpected_current", `current is ${before.current ?? "unset"}, expected ${expectCurrent}; refusing to activate ${id}`);
  }
  if (!before.releases.some((r) => r.id === id)) {
    throw new ReleaseError("bad_argument", `release ${id} is not present under ${prefix}`);
  }
  const activation = activate(prefix, id);
  if (!activation.changed) {
    return Object.freeze({ activation, readiness: { ok: true, skipped: "already_current", reason: "ok", attempts: 0 }, journal: undefined, rollback: undefined });
  }
  const startWall = new Date();
  const startAtMs = now();
  let restart;
  try {
    restart = effects.restart(service);
  } catch (error) {
    // The restart never happened, so there is no new-release window; the
    // journal starts before the failed attempt to capture that attempt itself
    // and is labeled as such — never presented as the new release's own log.
    const journal = { ...effects.journal(service, startWall.toISOString()), window: "pre_restart_attempt" };
    const rolledBack = rollbackAndRestart(prefix, service, effects);
    return Object.freeze({ activation, restart: { ok: false, error: error.message }, readiness: { ok: false, attempts: 0, lastReason: "restart_failed", elapsedMs: now() - startAtMs }, journal, rollback: rolledBack, error: "restart_failed" });
  }
  const readiness = await pollUntilHealthy({
    probe: () => effects.fetchHealth(healthUrl),
    startAtMs, now, sleep, deadlineMs, intervalMs,
  });
  // The journal window starts at the restart, so every line belongs to the
  // release the pointer named at restart time — the NEW release — never the
  // old one's listener.
  const journal = { ...effects.journal(service, restart.restartedAt), window: "restart_to_outcome" };
  if (!readiness.ok) {
    const rolledBack = rollbackAndRestart(prefix, service, effects);
    return Object.freeze({ activation, restart, readiness, journal, rollback: rolledBack, error: "readiness_deadline" });
  }
  const after = status(prefix);
  if (after.current !== id) {
    // Another activation moved the pointer during our readiness wait. That
    // actor owns the pointer now; rolling back would fight it, so report the
    // inconsistency with evidence instead of mutating further.
    return Object.freeze({
      activation, restart, readiness, journal,
      concurrent: { expected: id, actual: after.current },
      error: "concurrent_pointer_change",
    });
  }
  return Object.freeze({ activation, restart, readiness, journal, rollback: undefined });
}

/** Roll the pointer back and restart so the previous release serves again. */
function rollbackAndRestart(prefix, service, effects) {
  let rolledBack;
  try {
    rolledBack = rollback(prefix);
  } catch (error) {
    // Report the failed rollback alongside the readiness evidence instead of
    // throwing it away; the caller still learns why activation failed.
    return Object.freeze({ failed: true, error: `${error.code}: ${error.message}`, restart: { ok: false, error: `${error.code}: ${error.message}` } });
  }
  try {
    effects.restart(service);
  } catch (error) {
    return Object.freeze({ ...rolledBack, restart: { ok: false, error: error.message } });
  }
  return Object.freeze({ ...rolledBack, restart: { ok: true } });
}

/** The release id a release's verify.json was recorded for, as provenance text. */
export function releaseProvenance(prefix, id) {
  try {
    return JSON.parse(readFileSync(join(prefix, "releases", id, "verify.json"), "utf8"));
  } catch {
    return undefined;
  }
}
