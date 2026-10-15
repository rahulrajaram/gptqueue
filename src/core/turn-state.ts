/**
 * Turn state: whether an agent's runtime is inside a turn (`running`) or
 * between turns (`idle`), as its hooks report it. The model makes no call for
 * this; hooks write it as a side effect (UserPromptSubmit -> running, an
 * allowed Stop -> idle).
 *
 * Hooks can run asynchronously and land out of order, so a report older than
 * the stored state is ignored, and a repeated phase from the same session keeps
 * its original `since` (an agent idle for an hour stays "idle since" the start
 * of that hour). The watchdog reads this together with the message ledger.
 */
import type { Redis } from "ioredis";
import { SESSION_KEYS } from "./keys.js";

export type TurnPhase = "running" | "idle";
export type TurnRuntime = "claude" | "codex" | "pi";

export interface TurnState {
  readonly agent: string;
  readonly phase: TurnPhase;
  /** ISO time the current phase began (phase duration). */
  readonly since: string;
  /** ISO time of the last accepted report; reports older than this are stale (S3). */
  readonly reported_at: string;
  readonly runtime: TurnRuntime;
  /** Native runtime session (Claude session id, Codex thread id, ...). */
  readonly session_id: string;
  readonly turn_id: string | null;
}

export interface TurnReport {
  readonly phase: TurnPhase;
  /** ISO time of the hook event. */
  readonly at: string;
  readonly runtime: TurnRuntime;
  readonly session_id: string;
  readonly turn_id?: string | null;
}

/** Seven days: long enough for a weekend-idle agent, short enough that a retired name's state expires. */
export const TURN_STATE_TTL_SECONDS = 7 * 24 * 60 * 60;

const PHASES: ReadonlySet<string> = new Set<TurnPhase>(["running", "idle"]);
const RUNTIMES: ReadonlySet<string> = new Set<TurnRuntime>(["claude", "codex", "pi"]);
const isText = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const isTime = (value: unknown): value is string => isText(value) && !Number.isNaN(Date.parse(value));

/** Admit a stored or reported state; null when any field is malformed. */
export const admitTurnState = (value: unknown): TurnState | null => {
  if (typeof value !== "object" || value === null) return null;
  const v = value as Record<string, unknown>;
  if (!isText(v.agent) || !PHASES.has(v.phase as string) || !isTime(v.since) || !RUNTIMES.has(v.runtime as string) || !isText(v.session_id)) return null;
  if (v.turn_id !== null && v.turn_id !== undefined && !isText(v.turn_id)) return null;
  // Legacy records (before reported_at existed) were last reported at their since.
  if (v.reported_at !== undefined && !isTime(v.reported_at)) return null;
  const reportedAt = (v.reported_at as string | undefined) ?? v.since;
  return Object.freeze({
    agent: v.agent, phase: v.phase as TurnPhase, since: v.since, reported_at: reportedAt, runtime: v.runtime as TurnRuntime,
    session_id: v.session_id, turn_id: (v.turn_id as string | null | undefined) ?? null,
  });
};

export type TurnApply =
  | Readonly<{ kind: "changed"; state: TurnState }>
  /** Same phase and session: only reported_at advances (still a write). */
  | Readonly<{ kind: "refreshed"; state: TurnState }>
  | Readonly<{ kind: "stale" }>
  | Readonly<{ kind: "invalid" }>;

/** Pure: what a report does to the current state. */
export const applyTurnReport = (current: TurnState | null, agent: string, report: TurnReport): TurnApply => {
  const next = admitTurnState({ agent, phase: report.phase, since: report.at, reported_at: report.at, runtime: report.runtime, session_id: report.session_id, turn_id: report.turn_id ?? null });
  if (next === null) return { kind: "invalid" };
  if (current === null) return { kind: "changed", state: next };
  if (Date.parse(report.at) < Date.parse(current.reported_at)) return { kind: "stale" };
  if (current.phase === next.phase && current.session_id === next.session_id && current.runtime === next.runtime) {
    return { kind: "refreshed", state: Object.freeze({ ...current, reported_at: report.at }) };
  }
  return { kind: "changed", state: next };
};

/** Write `ARGV[2]` only if the stored value still equals `ARGV[1]` ("" = absent). */
const COMPARE_AND_SET = `
local current = redis.call('GET', KEYS[1])
if (current or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
return 1
`;

export class TurnStateStore {
  constructor(private readonly redis: Redis) {}

  /** The stored state, or null when absent or malformed (a malformed record is overwritten by the next report). */
  async read(agent: string): Promise<TurnState | null> {
    const raw = await this.redis.get(SESSION_KEYS.turnState(agent));
    return raw === null ? null : parse(raw);
  }

  /** Apply a hook report; a lost race re-reads once. Returns what was decided. */
  async report(agent: string, report: TurnReport): Promise<TurnApply> {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const key = SESSION_KEYS.turnState(agent);
      const raw = await this.redis.get(key);
      const decided = applyTurnReport(raw === null ? null : parse(raw), agent, report);
      if (decided.kind !== "changed" && decided.kind !== "refreshed") return decided;
      const written = await this.redis.eval(COMPARE_AND_SET, 1, key, raw ?? "", JSON.stringify(decided.state), TURN_STATE_TTL_SECONDS);
      if (written === 1) return decided;
    }
    return { kind: "stale" };
  }
}

const parse = (raw: string): TurnState | null => {
  try {
    return admitTurnState(JSON.parse(raw));
  } catch {
    return null;
  }
};
