/**
 * Claude Code hook for gptqueue: SessionStart context, a Stop gate, and turn
 * state for the loop adapter.
 *   gptqueue-claude-hook session-start | stop | user-prompt-submit     (hook JSON on stdin)
 *
 * - Turn state: user-prompt-submit records the session's agent as running and
 *   prints nothing; an allowed stop records it as idle. Only a confirmed
 *   identity is recorded (a guessed name may be another session's).
 *
 * - Acts on the inbox this session's MCP server registered (ID-2), falling
 *   back to the re-derived name only when no record exists, and says so.
 * - Counts unclaimed mail and this session's claimed-but-unacked tasks
 *   separately (AC-4) and fails closed but visible: a Redis error blocks the
 *   stop once with the error instead of reporting "no mail".
 * - stop_hook_active loop protection: while re-entered, block again only when
 *   new mail arrived (inbox-events id moved) since the previous block, so
 *   mail that arrives meanwhile is never skipped; otherwise allow with a
 *   visible systemMessage.
 * - Wake path (WK-1): when the inbox is empty and no background watcher is
 *   running for this session (pidfile), block once to have the model start it.
 *   GPTQ_CLAUDE_WATCH=off disables that reminder.
 */
import { readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { ensureRuntimeDir, watcherPidPath, watcherRunning } from "./session-record.js";
import { confirmLegacyLiveness, isConfirmed, probeClient, probeInbox, recordWaitMs, resolveIdentity, waitForIdentity, type Identity, type ProbeResult } from "./inbox-probe.js";
import { sessionTag } from "../core/session-tag.js";
import { quoteAgent } from "./session-record.js";
import { writeFileSync } from "node:fs";
import { claudeSeatStartupContext } from "../integrations/agent-tracking.js";
import { TurnStateStore, type TurnPhase } from "../core/turn-state.js";

export type HookEvent = "session-start" | "stop";
export type StopReason = "mail" | "error" | "watch";
export type PriorStop = Readonly<{ reason: StopReason; event_id: string | null }> | null;

export type HookInput = Readonly<{
  event: HookEvent;
  identity: Identity;
  probe: ProbeResult;
  stopHookActive: boolean;
  prior: PriorStop;
  watcherRunning: boolean;
  watchPolicy: "require" | "off";
  watchCommand: string;
  /** Seat card or refusal from the agent-tracking plugin; absent when the launcher named no seat. */
  seat?: string | null;
}>;

export type HookDecision =
  | Readonly<{ kind: "context"; text: string }>
  | Readonly<{ kind: "block"; reason: string; record: Exclude<PriorStop, null> }>
  | Readonly<{ kind: "allow"; systemMessage: string | null }>;

const CONSUME = "Consume with claim_tasks (max_batch up to 16), act on each task, then acknowledge_tasks with the claim_id. Do not use receive_message: it is an unacknowledged pop.";

const identityText = (id: Identity): string => {
  switch (id.source) {
    // SEC-F5: the session id is a bearer credential; hook text lands in model context and transcripts.
    case "session_record": return `agent ${quoteAgent(id.agent)} (gptqueue session ${id.gptqueue_session_id === null ? "unknown" : sessionTag(id.gptqueue_session_id)}, as registered by this session's gptqueue MCP server)`;
    case "legacy_env": return `agent ${quoteAgent(id.agent)} (${id.note})`;
    case "env":
    case "derived": return `agent ${quoteAgent(id.agent)} (UNCONFIRMED: ${id.note}; source ${id.source})`;
    default: { const unreachable: never = id.source; throw new Error(String(unreachable)); }
  }
};

export const countsText = (probe: Extract<ProbeResult, { ok: true }>): string => {
  const c = probe.counts;
  const mine = c.claimed_by_session === null ? "unknown (session not identified)" : String(c.claimed_by_session);
  const expired = c.expired_by_others > 0 ? ` (${c.expired_by_others} of them in EXPIRED claims: claim_tasks recovers them now)` : "";
  return `${c.unclaimed} unclaimed; claimed by this session but not acknowledged: ${mine}; claimed by other sessions: ${c.claimed_by_others}${expired}`;
};

/** Work this session must handle before stopping. */
export const actionable = (probe: Extract<ProbeResult, { ok: true }>): number =>
  probe.counts.unclaimed + (probe.counts.claimed_by_session ?? 0) + probe.counts.expired_by_others;

/** The seat text follows the inbox context; without a seat the context is unchanged. */
const withSeat = (text: string, seat: string | null | undefined): string => (seat ? `${text}\n\n${seat}` : text);

export const decideHook = (input: HookInput): HookDecision => {
  const { identity, probe } = input;
  const watchText = `To be woken when mail arrives while idle, run this with run_in_background (its exit wakes you; restart it after every wake): ${input.watchCommand}`;
  switch (input.event) {
    case "session-start": {
      if (!isConfirmed(identity)) {
        // The guessed name may be another session's (same directory): report no counts for it.
        return { kind: "context", text: withSeat(`gptqueue: this session's agent name is UNCONFIRMED (${identity.note}). Its gptqueue MCP server registers the session itself; call get_queue_status or claim_tasks through it to see your own inbox. Use a distinct idempotency_key per recipient on send_message (keys are global). ${input.watchPolicy === "off" ? "" : watchText}`.trim(), input.seat) };
      }
      const inbox = probe.ok
        ? `Inbox: ${countsText(probe)}.${actionable(probe) > 0 ? ` ${CONSUME}` : ""}`
        : `Inbox check FAILED (${probe.code}: ${probe.message}); gptqueue state is unknown, do not assume the inbox is empty.`;
      return { kind: "context", text: withSeat(`gptqueue: this session is registered as ${identityText(identity)} (no register_agent call needed). ${inbox} Use a distinct idempotency_key per recipient on send_message (keys are global). ${input.watchPolicy === "off" ? "" : watchText}`.trim(), input.seat) };
    }
    case "stop": {
      if (!probe.ok) {
        if (input.stopHookActive && input.prior?.reason === "error") {
          return { kind: "allow", systemMessage: `gptqueue inbox check for ${quoteAgent(identity.agent)} still failing (${probe.code}); stopping without inbox confirmation.` };
        }
        return { kind: "block", record: { reason: "error", event_id: null }, reason: `gptqueue inbox check for ${identityText(identity)} FAILED (${probe.code}: ${probe.message}). The inbox state is unknown, not empty. Tell the user gptqueue is unreachable, or retry with get_queue_status / claim_tasks, then stop.` };
      }
      if (!isConfirmed(identity)) {
        // Never present a guessed name's counts as this session's own.
        return { kind: "allow", systemMessage: `gptqueue: this session's agent name is UNCONFIRMED (${identity.note}); not gating on ${quoteAgent(identity.agent)}, which may be another session's inbox. Check this session's inbox with get_queue_status or claim_tasks.` };
      }
      const pending = actionable(probe);
      const latest = probe.counts.latest_event_id;
      if (pending > 0) {
        const newMail = input.prior === null || input.prior.reason !== "mail" || latest !== input.prior.event_id;
        if (input.stopHookActive && !newMail) {
          return { kind: "allow", systemMessage: `gptqueue: ${quoteAgent(identity.agent)} still has ${countsText(probe)}; not re-blocking (no new mail since the last reminder).` };
        }
        return { kind: "block", record: { reason: "mail", event_id: latest }, reason: `gptqueue inbox for ${identityText(identity)}: ${countsText(probe)}. ${CONSUME} Acknowledge or renew claims you already hold. If none need action now, say so and stop.` };
      }
      // One watch reminder per stop chain. A re-entered stop skips it only when
      // this chain already showed it, or the earlier block was not ours (no
      // record). After our own mail or error block it must still fire: the
      // watcher often exits during that very turn, because the mail woke it.
      const remindedThisChain = input.stopHookActive && (input.prior === null || input.prior.reason === "watch");
      if (input.watchPolicy === "require" && !input.watcherRunning && !remindedThisChain) {
        return { kind: "block", record: { reason: "watch", event_id: latest }, reason: `gptqueue: inbox for ${identityText(identity)} is empty, but no background inbox watcher is running for this session, so mail arriving while you are idle will not wake you. ${watchText} Then stop.` };
      }
      return { kind: "allow", systemMessage: null };
    }
    default: {
      const unreachable: never = input.event;
      throw new Error(`unknown hook event ${String(unreachable)}`);
    }
  }
};

const shellQuote = (value: string): string => `'${value.replace(/'/g, `'\\''`)}'`;

/** Name-free: the watcher resolves this session's registered name itself from the session record. */
export const watchCommandFor = (binDir: string, claudeSessionId: string): string =>
  `${shellQuote(join(binDir, "gptqueue-inbox-peek"))} --watch --session ${shellQuote(claudeSessionId)}`;

type RawHookJson = { session_id?: unknown; cwd?: unknown; stop_hook_active?: unknown };

const readPrior = (path: string): PriorStop => {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as { reason?: unknown; event_id?: unknown };
    const reason = parsed.reason;
    return reason === "mail" || reason === "error" || reason === "watch"
      ? { reason, event_id: typeof parsed.event_id === "string" ? parsed.event_id : null } : null;
  } catch { return null; }
};

export const renderDecision = (event: HookEvent, decision: HookDecision): string | null => {
  switch (decision.kind) {
    case "context": return JSON.stringify({ hookSpecificOutput: { hookEventName: event === "session-start" ? "SessionStart" : "Stop", additionalContext: decision.text } });
    case "block": return JSON.stringify({ decision: "block", reason: decision.reason });
    case "allow": return decision.systemMessage === null ? null : JSON.stringify({ systemMessage: decision.systemMessage });
    default: { const unreachable: never = decision; throw new Error(String(unreachable)); }
  }
};

/** Record a confirmed session's turn phase. Best effort: a turn-state failure never blocks a prompt or a stop. */
const recordTurn = async (identity: Identity, phase: TurnPhase, claudeSession: string, env: NodeJS.ProcessEnv): Promise<void> => {
  if (!isConfirmed(identity) || claudeSession === "unknown") return;
  const redis = probeClient(env.REDIS_URL);
  try {
    await new TurnStateStore(redis).report(identity.agent, { phase, at: new Date().toISOString(), runtime: "claude", session_id: claudeSession });
  } catch {
    // turn state is advisory input to the watchdog
  } finally {
    redis.disconnect();
  }
};

export const runClaudeHook = async (argv: readonly string[], stdin: string, binDir: string, env: NodeJS.ProcessEnv = process.env): Promise<{ stdout: string | null; exitCode: number }> => {
  const event = argv[0];
  if (event === "user-prompt-submit") {
    let raw: RawHookJson = {};
    try { raw = JSON.parse(stdin || "{}") as RawHookJson; } catch { /* nothing to record */ }
    const session = typeof raw.session_id === "string" && raw.session_id ? raw.session_id : "unknown";
    await recordTurn(resolveIdentity(typeof raw.cwd === "string" ? raw.cwd : undefined, env), "running", session, env);
    return { stdout: null, exitCode: 0 };
  }
  if (event !== "session-start" && event !== "stop") return { stdout: null, exitCode: 2 };
  let json: RawHookJson = {};
  try { json = JSON.parse(stdin || "{}") as RawHookJson; } catch { /* treat as empty input; still report inbox state */ }
  const claudeSession = typeof json.session_id === "string" && json.session_id ? json.session_id : "unknown";
  const cwd = typeof json.cwd === "string" ? json.cwd : undefined;
  // SessionStart can run before the MCP server has registered; wait briefly for its record.
  const resolved = event === "session-start" ? await waitForIdentity(cwd, recordWaitMs(env, 2000), env) : resolveIdentity(cwd, env);
  const redis = probeClient(env.REDIS_URL);
  const { identity, probe, seat } = await (async () => {
    const checked = await confirmLegacyLiveness(redis, resolved, env);
    const inbox = await probeInbox(redis, checked.agent, checked.gptqueue_session_id);
    // Seat binding (agent tracking) happens here, in the hook, so the model never makes a tool call for it.
    const seatText = event === "session-start"
      ? await claudeSeatStartupContext({ env, cwd: cwd ?? process.cwd(), session_id: claudeSession, claude: checked.claude, redis })
          .catch(() => "gptqueue seat: binding FAILED; this session does NOT hold a seat. Tell the operator.")
      : null;
    return { identity: checked, probe: inbox, seat: seatText };
  })().finally(() => redis.disconnect());

  let dir: string | null = null;
  try { dir = ensureRuntimeDir(env); } catch { /* unsafe dir: no state, no watcher detection */ }
  const statePath = dir ? join(dir, `stop-${claudeSession.replace(/[^A-Za-z0-9._-]/g, "_")}.json`) : null;
  const decision = decideHook({
    event,
    identity,
    probe,
    stopHookActive: json.stop_hook_active === true,
    prior: statePath ? readPrior(statePath) : null,
    watcherRunning: dir ? watcherRunning(watcherPidPath(dir, claudeSession), identity.agent, identity.claude) : false,
    watchPolicy: env.GPTQ_CLAUDE_WATCH === "off" ? "off" : "require",
    watchCommand: watchCommandFor(binDir, claudeSession),
    seat,
  });
  if (statePath && event === "stop") {
    if (decision.kind === "block") writeFileSync(statePath, JSON.stringify(decision.record), { mode: 0o600 });
    else rmSync(statePath, { force: true });
  }
  if (event === "stop" && decision.kind === "allow") await recordTurn(identity, "idle", claudeSession, env);
  return { stdout: renderDecision(event, decision), exitCode: 0 };
};

/**
 * BC-F6: the directory the hook was invoked from, NOT its realpath: under an
 * installed runtime that is <prefix>/current/bin, which survives release
 * switches and prunes, whereas the realpath pins releases/<id>/bin.
 */
export const hookBinDir = (scriptPath: string): string => dirname(resolve(scriptPath));
