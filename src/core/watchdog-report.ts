/**
 * Watchdog CLI presentation (W5, W6), kept pure for tests: argument parsing,
 * the quiet journal line, and health evaluation of the last-pass record.
 */
import { homedir } from "node:os";
import { join } from "node:path";
import type { WatchdogResult } from "./watchdog-shell.js";

export type CliMode = "once" | "status";
export type CliArgs = Readonly<{ mode: CliMode; configPath: string; dryRun: boolean; redisUrl: string; json: boolean }>;
export type CliParse = Readonly<{ ok: true; args: CliArgs }> | Readonly<{ ok: false; reason: string }>;

export const configHome = (env: NodeJS.ProcessEnv): string => env.XDG_CONFIG_HOME || join(homedir(), ".config");
export const stateHome = (env: NodeJS.ProcessEnv): string => env.XDG_STATE_HOME || join(homedir(), ".local", "state");

/** Pure argument parsing; defaults from the environment. Exactly one of --once or --status. */
export const parseWatchdogArgs = (argv: readonly string[], env: NodeJS.ProcessEnv): CliParse => {
  const modes = new Set<CliMode>();
  let dryRun = false;
  let json = false;
  let configPath = join(configHome(env), "gptqueue", "watchdog.json");
  let redisUrl = env.REDIS_URL || "redis://127.0.0.1:6379";
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case "--once": modes.add("once"); break;
      case "--status": modes.add("status"); break;
      case "--dry-run": dryRun = true; break;
      case "--json": json = true; break;
      case "--config":
      case "--redis-url": {
        const value = argv[i + 1];
        if (value === undefined || value.startsWith("--")) return { ok: false, reason: `${arg} needs a value` };
        if (arg === "--config") configPath = value; else redisUrl = value;
        i += 1;
        break;
      }
      default: return { ok: false, reason: `unknown argument ${String(arg)}` };
    }
  }
  if (modes.size !== 1) return { ok: false, reason: "pass exactly one of --once (one pass) or --status (last pass health)" };
  const [mode] = [...modes] as [CliMode];
  return { ok: true, args: { mode, configPath, dryRun, redisUrl, json } };
};

/**
 * W5: a quiet pass (no actions, errors, deferred or rate-limited work) is one
 * compact journal line; anything else, or --json, is the full JSON report.
 */
export const formatWatchdogOutput = (result: WatchdogResult, forceJson: boolean): string => {
  const output = result.output;
  if (forceJson || result.exitCode !== 0 || !("leads" in output)) return JSON.stringify(output);
  if (output.actions.length > 0 || output.deferred > 0 || output.rate_limited > 0) return JSON.stringify(output);
  const count = (kind: string) => output.leads.filter((l) => l.state.kind === kind).length;
  return `gptqueue watchdog: ok at=${output.at} leads=${output.leads.length} waiting=${count("waiting")} stalled=${count("stalled")} idle=${count("idle")} uncorrelated=${output.uncorrelated}`;
};

/** A pass older than this is stale for --status. */
export const STATUS_MAX_AGE_MS = 3 * 60 * 1000;

export type HealthState = "healthy" | "degraded" | "unhealthy";
export type WatchdogStatus = Readonly<{
  exitCode: 0 | 1;
  status: Readonly<{ state: HealthState; healthy: boolean; reason: string | null; last_pass: unknown; last_skip: unknown }>;
}>;

const parseRecord = (raw: string | null): Record<string, unknown> | "unreadable" | null => {
  if (raw === null) return null;
  try {
    const value = JSON.parse(raw) as unknown;
    return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : "unreadable";
  } catch {
    return "unreadable";
  }
};

/**
 * W3/W6: pure health from the last SUBSTANTIVE pass (skipped passes never count).
 * Healthy: under 3 minutes old, exit 0 and no failed actions. Failed actions are
 * `degraded`; anything else wrong is `unhealthy`. The last skip is shown separately.
 */
export const evaluateWatchdogStatus = (rawSubstantive: string | null, rawLast: string | null, now: Date): WatchdogStatus => {
  const last = parseRecord(rawLast);
  const lastSkip = last !== null && last !== "unreadable" && last.skipped === true ? last : null;
  const verdict = (state: HealthState, reason: string | null, lastPass: unknown): WatchdogStatus =>
    ({ exitCode: state === "healthy" ? 0 : 1, status: { state, healthy: state === "healthy", reason, last_pass: lastPass, last_skip: lastSkip } });
  const record = parseRecord(rawSubstantive);
  if (record === null) return verdict("unhealthy", "no substantive watchdog pass recorded (gptq:watchdog:last-substantive-pass is absent or expired)", null);
  if (record === "unreadable") return verdict("unhealthy", "the last substantive pass record is unreadable", null);
  const at = typeof record.at === "string" ? Date.parse(record.at) : Number.NaN;
  if (Number.isNaN(at)) return verdict("unhealthy", "the last substantive pass record is unreadable (no valid at)", record);
  const age = now.getTime() - at;
  if (age > STATUS_MAX_AGE_MS) return verdict("unhealthy", `the last substantive pass is older than 3 minutes (${Math.round(age / 1000)} s)`, record);
  if (record.exit !== 0) return verdict("unhealthy", `the last substantive pass exited ${String(record.exit)}`, record);
  if (typeof record.failed === "number" && record.failed > 0) return verdict("degraded", `the last substantive pass had ${record.failed} failed action(s)`, record);
  return verdict("healthy", null, record);
};
