/**
 * Codex Stop gate (loop adapter S1). Codex runs Stop hooks synchronously at
 * the end of every turn, inside the shared app-server daemon, for every
 * thread. So:
 * - an unbound thread (no runtime-mailbox mapping) exits at once with `{}`;
 * - a Redis failure while resolving the binding fails open with `{}`, because
 *   blocking would hit every thread on the daemon, gptqueue-bound or not;
 * - for a bound thread with pending mail it blocks once per new mail event.
 *   Codex then continues the same turn with the reason as a new prompt, and the
 *   next Stop carries stop_hook_active=true (probe-verified on 0.159.0). Mail
 *   that arrives meanwhile blocks again; otherwise the stop is allowed with a
 *   visible message, so a legitimately blocked agent never spins;
 * - an allowed stop records the agent as idle (turn state).
 */
import type { Redis } from "ioredis";
import { z } from "zod";
import { probeInbox, type ProbeResult } from "../claude/inbox-probe.js";
import { SESSION_KEYS } from "../core/keys.js";
import { TurnStateStore } from "../core/turn-state.js";

export type PriorBlock = Readonly<{ reason: "mail" | "error"; event_id: string | null }> | null;

export type CodexStopInput = Readonly<{
  agent: string;
  probe: ProbeResult;
  stopHookActive: boolean;
  prior: PriorBlock;
}>;

export type CodexStopDecision =
  | Readonly<{ kind: "block"; reason: string; record: Exclude<PriorBlock, null> }>
  | Readonly<{ kind: "allow"; systemMessage: string | null; idle: boolean }>;

/** How long a block's mail event id is remembered for the block-once rule. */
export const STOP_GATE_TTL_SECONDS = 24 * 60 * 60;

const CONSUME = "Consume with claim_tasks (max_batch up to 16), act on each task, then acknowledge_tasks with the claim_id.";

export const decideCodexStop = (input: CodexStopInput): CodexStopDecision => {
  const who = `gptqueue inbox for agent ${JSON.stringify(input.agent)}`;
  const { probe } = input;
  if (!probe.ok) {
    if (input.stopHookActive && input.prior?.reason === "error") {
      return { kind: "allow", systemMessage: `${who}: inbox check still failing (${probe.code}); stopping without inbox confirmation.`, idle: false };
    }
    return {
      kind: "block",
      record: { reason: "error", event_id: null },
      reason: `${who}: the inbox check FAILED (${probe.code}: ${probe.message}). The inbox state is unknown, not empty. Retry with get_queue_status or claim_tasks, or say that gptqueue is unreachable, then stop.`,
    };
  }
  const c = probe.counts;
  // This hook cannot attribute claims to the thread's own sidecar session, so
  // only unclaimed and expired-claim mail gates the stop.
  const pending = c.unclaimed + c.expired_by_others;
  if (pending === 0) return { kind: "allow", systemMessage: null, idle: true };
  const newMail = input.prior === null || input.prior.reason !== "mail" || c.latest_event_id !== input.prior.event_id;
  if (input.stopHookActive && !newMail) {
    return { kind: "allow", systemMessage: `${who} still has ${pending} pending message(s); not re-blocking (no new mail since the last reminder).`, idle: true };
  }
  const expired = c.expired_by_others > 0 ? ` and ${c.expired_by_others} in expired claims (claim_tasks recovers them)` : "";
  return {
    kind: "block",
    record: { reason: "mail", event_id: c.latest_event_id },
    reason: `${who}: ${c.unclaimed} unclaimed message(s)${expired}. ${CONSUME} If none need action now, say so and stop.`,
  };
};

const mappingSchema = z.object({ agent: z.string().min(1), working_directory: z.string() }).passthrough();
const priorSchema = z.object({ reason: z.enum(["mail", "error"]), event_id: z.string().nullable() });

const parseJson = <T>(raw: string | null, schema: z.ZodType<T>): T | null => {
  if (raw === null) return null;
  try {
    const parsed = schema.safeParse(JSON.parse(raw));
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
};

export type CodexStopEvent = Readonly<{ session_id: string; stop_hook_active?: boolean; turn_id?: string }>;

/** End-to-end budget for one Stop (mapping, prior, probe and bookkeeping); well inside the installed 15 s hook timeout. */
export const STOP_GATE_DEADLINE_MS = 4_000;

/**
 * Run the gate for one Stop event; returns the hook's stdout JSON. Never
 * throws. The whole path runs under one deadline (S1): per-command timeouts
 * alone let several slow replies add up past the hook timeout on the shared
 * daemon. On expiry the client is disconnected and the stop fails open ("{}").
 */
export const runCodexStop = async (
  event: CodexStopEvent, redis: Redis, now: () => Date = () => new Date(), deadlineMs: number = STOP_GATE_DEADLINE_MS,
): Promise<string> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<string>((resolveExpired) => {
    timer = setTimeout(() => {
      try { redis.disconnect(); } catch { /* already gone */ }
      resolveExpired("{}");
    }, deadlineMs);
  });
  try {
    return await Promise.race([gate(event, redis, now).catch(() => "{}"), expired]);
  } finally {
    clearTimeout(timer);
  }
};

const gate = async (event: CodexStopEvent, redis: Redis, now: () => Date): Promise<string> => {
  let agent: string | null;
  try {
    agent = parseJson(await redis.get(SESSION_KEYS.runtimeMailbox("codex", event.session_id)), mappingSchema)?.agent ?? null;
  } catch {
    return "{}"; // cannot tell whether this thread is gptqueue's: never block an unrelated thread
  }
  if (agent === null) {
    // Redis answered but this thread has no runtime-mailbox mapping: an unbound
    // thread (the common case) is indistinguishable here from the gate probing
    // a different Redis than the sidecar (a mis-probe, R1), so emit one
    // observable stderr event; the stop still fails open.
    try {
      process.stderr.write(`${JSON.stringify({ event: "codex_stop_gate_no_mapping", runtime: "codex", session_id: event.session_id })}\n`);
    } catch { /* stderr is best effort */ }
    return "{}";
  }

  const priorKey = SESSION_KEYS.stopGate("codex", event.session_id);
  const prior = await redis.get(priorKey).then((raw) => parseJson(raw, priorSchema), () => null);
  const decision = decideCodexStop({ agent, probe: await probeInbox(redis, agent, null), stopHookActive: event.stop_hook_active === true, prior });
  switch (decision.kind) {
    case "block":
      try {
        await redis.set(priorKey, JSON.stringify(decision.record), "EX", STOP_GATE_TTL_SECONDS);
      } catch {
        // F2: an unrecorded block would re-block on every continued Stop (no block-once memory), so allow instead.
        return JSON.stringify({ systemMessage: `gptqueue inbox for agent ${JSON.stringify(agent)}: a reminder was due but could not be recorded, so this stop is allowed. Check the inbox with claim_tasks before stopping.` });
      }
      return JSON.stringify({ decision: "block", reason: decision.reason });
    case "allow": {
      try {
        await redis.del(priorKey);
        if (decision.idle) {
          await new TurnStateStore(redis).report(agent, {
            phase: "idle", at: now().toISOString(), runtime: "codex", session_id: event.session_id, turn_id: event.turn_id ?? null,
          });
        }
      } catch {
        // Bookkeeping failed after deciding: the stop and its message still stand.
      }
      return JSON.stringify(decision.systemMessage === null ? {} : { systemMessage: decision.systemMessage });
    }
    default: {
      const unreachable: never = decision;
      throw new Error(`unhandled stop decision ${JSON.stringify(unreachable)}`);
    }
  }
};
