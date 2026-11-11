/**
 * Claude Code hook for gptqueue: SessionStart context, a Stop gate, and turn
 * state for the loop adapter.
 *   gptqueue-claude-hook session-start | stop | user-prompt-submit | pre-tool-use   (hook JSON on stdin)
 *
 * - Headless worker sessions (worker-mode.ts, f-1965): every event is a no-op
 *   apart from a one-line SessionStart context; no Redis connection is opened.
 * - pre-tool-use (opt-in AskUserQuestion guard, f-1915): denies
 *   AskUserQuestion once per (session, latest inbox event) while this session
 *   has actionable mail, so an operator question does not stall lead mail.
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
import { chmodSync, lstatSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { ensureRuntimeDir, watcherPidPath, watcherRunning } from "./session-record.js";
import { confirmLegacyLiveness, isConfirmed, probeClient, probeInbox, recordWaitMs, resolveIdentity, waitForIdentity, type Identity, type ProbeResult } from "./inbox-probe.js";
import { sessionTag } from "../core/session-tag.js";
import { quoteAgent } from "./session-record.js";
import { writeFileSync } from "node:fs";
import { claudeSeatStartupContext } from "../integrations/agent-tracking.js";
import { TurnStateStore, type TurnPhase } from "../core/turn-state.js";
import { isWorkerSession, WORKER_SESSION_CONTEXT } from "./worker-mode.js";

export type HookEvent = "session-start" | "stop";
export type StopReason = "mail" | "error" | "watch";
/**
 * What one stop chain (a stop and the stops re-entered after its blocks) has
 * already shown. It accumulates across the chain's blocks, so each reminder
 * fires at most once per chain and mail re-blocks only for new mail.
 */
export type ChainHistory = Readonly<{ error: boolean; watch: boolean; mail: boolean; mail_event_id: string | null }>;
export type PriorStop = Readonly<{ reason: StopReason; event_id: string | null; chain?: ChainHistory }> | null;

const EMPTY_CHAIN: ChainHistory = Object.freeze({ error: false, watch: false, mail: false, mail_event_id: null });
/** A record written before chain history existed describes only its own block. */
const historyOf = (prior: Exclude<PriorStop, null>): ChainHistory => prior.chain ?? Object.freeze({
  error: prior.reason === "error", watch: prior.reason === "watch",
  mail: prior.reason === "mail", mail_event_id: prior.reason === "mail" ? prior.event_id : null });
const blockRecord = (reason: StopReason, eventId: string | null, chain: ChainHistory): Exclude<PriorStop, null> => Object.freeze({
  reason, event_id: eventId, chain: Object.freeze({
    error: chain.error || reason === "error", watch: chain.watch || reason === "watch",
    mail: chain.mail || reason === "mail", mail_event_id: reason === "mail" ? eventId : chain.mail_event_id }) });

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
      // A fresh stop starts a new chain; a re-entered one continues the recorded chain.
      const chain = input.stopHookActive && input.prior !== null ? historyOf(input.prior) : EMPTY_CHAIN;
      if (!probe.ok) {
        if (chain.error) {
          return { kind: "allow", systemMessage: `gptqueue inbox check for ${quoteAgent(identity.agent)} still failing (${probe.code}); stopping without inbox confirmation.` };
        }
        return { kind: "block", record: blockRecord("error", null, chain), reason: `gptqueue inbox check for ${identityText(identity)} FAILED (${probe.code}: ${probe.message}). The inbox state is unknown, not empty. Tell the user gptqueue is unreachable, or retry with get_queue_status / claim_tasks, then stop.` };
      }
      if (!isConfirmed(identity)) {
        // Never present a guessed name's counts as this session's own.
        return { kind: "allow", systemMessage: `gptqueue: this session's agent name is UNCONFIRMED (${identity.note}); not gating on ${quoteAgent(identity.agent)}, which may be another session's inbox. Check this session's inbox with get_queue_status or claim_tasks.` };
      }
      const pending = actionable(probe);
      const latest = probe.counts.latest_event_id;
      if (pending > 0) {
        const newMail = !chain.mail || latest !== chain.mail_event_id;
        if (input.stopHookActive && !newMail) {
          return { kind: "allow", systemMessage: `gptqueue: ${quoteAgent(identity.agent)} still has ${countsText(probe)}; not re-blocking (no new mail since the last reminder).` };
        }
        return { kind: "block", record: blockRecord("mail", latest, chain), reason: `gptqueue inbox for ${identityText(identity)}: ${countsText(probe)}. ${CONSUME} Acknowledge or renew claims you already hold. If none need action now, say so and stop.` };
      }
      // One watch reminder per stop chain. A re-entered stop skips it when this
      // chain already showed it, or the earlier block was not ours (no record).
      // After our own mail or error block it must still fire: the watcher often
      // exits during that very turn, because the mail woke it.
      const remindedThisChain = input.stopHookActive && (input.prior === null || chain.watch);
      if (input.watchPolicy === "require" && !input.watcherRunning && !remindedThisChain) {
        return { kind: "block", record: blockRecord("watch", latest, chain), reason: `gptqueue: inbox for ${identityText(identity)} is empty, but no background inbox watcher is running for this session, so mail arriving while you are idle will not wake you. ${watchText} Then stop.` };
      }
      return { kind: "allow", systemMessage: null };
    }
    default: {
      const unreachable: never = input.event;
      throw new Error(`unknown hook event ${String(unreachable)}`);
    }
  }
};

/** The AskUserQuestion denial recorded for a session: the inbox event it was raised for. */
export type PriorAsk = Readonly<{ event_id: string | null }> | null;

export type AskInput = Readonly<{ identity: Identity; probe: ProbeResult; prior: PriorAsk }>;

export type AskDecision =
  | Readonly<{ kind: "deny"; reason: string; record: Exclude<PriorAsk, null> }>
  /** No permission decision: Claude Code's normal flow (never "allow", which would bypass permission prompts). */
  | Readonly<{ kind: "pass"; systemMessage: string | null; clearRecord: boolean }>;

/**
 * Pure AskUserQuestion guard decision. Never blocks on gptqueue trouble or a
 * guessed identity; denies once per latest inbox event while mail is actionable.
 */
export const decideAsk = (input: AskInput): AskDecision => {
  const { identity, probe, prior } = input;
  if (!isConfirmed(identity)) return { kind: "pass", systemMessage: null, clearRecord: false };
  switch (probe.ok) {
    case false:
      return { kind: "pass", clearRecord: false, systemMessage: `gptqueue inbox check for ${quoteAgent(identity.agent)} FAILED (${probe.code}); AskUserQuestion not gated, inbox state unknown.` };
    case true: {
      if (actionable(probe) === 0) return { kind: "pass", systemMessage: null, clearRecord: true };
      const latest = probe.counts.latest_event_id;
      if (prior !== null && prior.event_id === latest) return { kind: "pass", systemMessage: null, clearRecord: false };
      return {
        kind: "deny", record: Object.freeze({ event_id: latest }),
        reason: `gptqueue inbox for ${identityText(identity)}: ${countsText(probe)}. AskUserQuestion blocks this session until the operator answers, so this mail and any lead waiting on it stall. Claim and handle it first (${CONSUME}), then ask. Asking again without new mail goes through: this guard does not repeat.`,
      };
    }
    default: { const unreachable: never = probe; throw new Error(String(unreachable)); }
  }
};

export const renderAsk = (decision: AskDecision): string | null => {
  switch (decision.kind) {
    case "deny": return JSON.stringify({ hookSpecificOutput: { hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: decision.reason } });
    case "pass": return decision.systemMessage === null ? null : JSON.stringify({ systemMessage: decision.systemMessage });
    default: { const unreachable: never = decision; throw new Error(String(unreachable)); }
  }
};

const shellQuote = (value: string): string => `'${value.replace(/'/g, `'\\''`)}'`;

/**
 * Name-free: the watcher resolves this session's registered name itself from the
 * session record. The Stop gate's own actionable rule is every unclaimed
 * message (plus this session's claims and expired claims), so the watcher it
 * recommends must match that: a plain `--watch` wakes on any unclaimed mail.
 * `--wakes-only` is narrower than the Stop gate and would silently strand a
 * result/status the session still owes a reply on (field report, statica,
 * 2026-11-11: a READY packet sat 9.3 h). It stays available for dispatchers
 * that deliberately want the narrower predicate.
 */
export const watchCommandFor = (binDir: string, claudeSessionId: string): string =>
  `${shellQuote(join(binDir, "gptqueue-inbox-peek"))} --watch --session ${shellQuote(claudeSessionId)}`;

type RawHookJson = { session_id?: unknown; cwd?: unknown; stop_hook_active?: unknown; tool_name?: unknown };

const parseHookJson = (stdin: string): RawHookJson => {
  try {
    const parsed: unknown = JSON.parse(stdin || "{}");
    return typeof parsed === "object" && parsed !== null ? parsed as RawHookJson : {};
  } catch { return {}; }
};
const sessionFileTag = (claudeSession: string): string => claudeSession.replace(/[^A-Za-z0-9._-]/g, "_");

const readPriorAsk = (path: string): PriorAsk => {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as { event_id?: unknown };
    return { event_id: typeof parsed.event_id === "string" ? parsed.event_id : null };
  } catch { return null; }
};

const readChain = (raw: unknown): ChainHistory | undefined => {
  if (typeof raw !== "object" || raw === null) return undefined;
  const c = raw as Record<string, unknown>;
  return typeof c.error === "boolean" && typeof c.watch === "boolean" && typeof c.mail === "boolean"
    ? Object.freeze({ error: c.error, watch: c.watch, mail: c.mail, mail_event_id: typeof c.mail_event_id === "string" ? c.mail_event_id : null })
    : undefined;
};

const readPrior = (path: string): PriorStop => {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as { reason?: unknown; event_id?: unknown; chain?: unknown };
    const reason = parsed.reason;
    if (reason !== "mail" && reason !== "error" && reason !== "watch") return null;
    const chain = readChain(parsed.chain);
    return { reason, event_id: typeof parsed.event_id === "string" ? parsed.event_id : null, ...(chain ? { chain } : {}) };
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

export type CliEvent = HookEvent | "user-prompt-submit" | "pre-tool-use";
export const CLI_EVENTS: readonly CliEvent[] = Object.freeze(["session-start", "stop", "user-prompt-submit", "pre-tool-use"]);
const isCliEvent = (value: string | undefined): value is CliEvent => CLI_EVENTS.includes(value as CliEvent);

/** Worker mode: no Redis, no state, no records; only SessionStart says why. */
const workerOutput = (event: CliEvent): string | null => {
  switch (event) {
    case "session-start": return JSON.stringify({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: WORKER_SESSION_CONTEXT } });
    case "stop":
    case "user-prompt-submit":
    case "pre-tool-use": return null;
    default: { const unreachable: never = event; throw new Error(String(unreachable)); }
  }
};

const ASK_GUARD_INACTIVE = (why: string): string =>
  JSON.stringify({ systemMessage: `gptqueue AskUserQuestion guard inactive (${why}); not gating this question.` });

/**
 * Persist a denial record before the denial is emitted (F1): a deny that
 * cannot be recorded would repeat for the same mail. Refuses an existing
 * path that is not a regular file owned by this user; forces mode 0600.
 */
const persistDenial = (path: string, record: Exclude<PriorAsk, null>): boolean => {
  try {
    const existing = (() => { try { return lstatSync(path); } catch { return null; } })();
    if (existing !== null && (!existing.isFile() || (process.getuid !== undefined && existing.uid !== process.getuid()))) return false;
    writeFileSync(path, JSON.stringify(record), { mode: 0o600 });
    chmodSync(path, 0o600);
    return readPriorAsk(path)?.event_id === record.event_id;
  } catch { return false; }
};

/** A session id usable as a state-file key: non-empty and unchanged by sanitizing (no collisions). */
const usableSession = (raw: unknown): string | null =>
  typeof raw === "string" && raw !== "" && raw !== "unknown" && sessionFileTag(raw) === raw ? raw : null;

const runAskGuard = async (stdin: string, env: NodeJS.ProcessEnv): Promise<{ stdout: string | null; exitCode: number }> => {
  const json = parseHookJson(stdin);
  if (json.tool_name !== "AskUserQuestion") return { stdout: null, exitCode: 0 };
  // F1: fail open unless a denial can be recorded per session.
  const claudeSession = usableSession(json.session_id);
  if (claudeSession === null) return { stdout: ASK_GUARD_INACTIVE("no usable session_id"), exitCode: 0 };
  let dir: string | null = null;
  try { dir = ensureRuntimeDir(env); } catch { /* unsafe dir */ }
  if (dir === null) return { stdout: ASK_GUARD_INACTIVE("runtime dir unavailable or unsafe"), exitCode: 0 };
  const statePath = join(dir, `ask-${claudeSession}.json`);
  const resolved = resolveIdentity(typeof json.cwd === "string" ? json.cwd : undefined, env);
  // A guessed name may be another session's inbox: never gate on it, and skip Redis entirely.
  if (!isConfirmed(resolved)) return { stdout: null, exitCode: 0 };
  const redis = probeClient(env.REDIS_URL);
  const { identity, probe } = await (async () => {
    const checked = await confirmLegacyLiveness(redis, resolved, env);
    return { identity: checked, probe: await probeInbox(redis, checked.agent, checked.gptqueue_session_id) };
  })().finally(() => redis.disconnect());
  const decision = decideAsk({ identity, probe, prior: readPriorAsk(statePath) });
  switch (decision.kind) {
    case "deny":
      return persistDenial(statePath, decision.record)
        ? { stdout: renderAsk(decision), exitCode: 0 }
        : { stdout: ASK_GUARD_INACTIVE("could not record the denial"), exitCode: 0 };
    case "pass":
      if (decision.clearRecord) { try { rmSync(statePath, { force: true }); } catch { /* stale record only re-allows */ } }
      return { stdout: renderAsk(decision), exitCode: 0 };
    default: { const unreachable: never = decision; throw new Error(String(unreachable)); }
  }
};

export const runClaudeHook = async (argv: readonly string[], stdin: string, binDir: string, env: NodeJS.ProcessEnv = process.env): Promise<{ stdout: string | null; exitCode: number }> => {
  const event = argv[0];
  if (!isCliEvent(event)) return { stdout: null, exitCode: 2 };
  if (isWorkerSession(env)) return { stdout: workerOutput(event), exitCode: 0 };
  if (event === "pre-tool-use") return runAskGuard(stdin, env);
  if (event === "user-prompt-submit") {
    let raw: RawHookJson = {};
    try { raw = JSON.parse(stdin || "{}") as RawHookJson; } catch { /* nothing to record */ }
    const session = typeof raw.session_id === "string" && raw.session_id ? raw.session_id : "unknown";
    await recordTurn(resolveIdentity(typeof raw.cwd === "string" ? raw.cwd : undefined, env), "running", session, env);
    return { stdout: null, exitCode: 0 };
  }
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
  const statePath = dir ? join(dir, `stop-${sessionFileTag(claudeSession)}.json`) : null;
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
