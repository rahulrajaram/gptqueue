import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { CodexSocketClient } from "./codex-socket.js";
import type { CodexRpcClient } from "./codex-runtime.js";
import type { RuntimeTools } from "./runtime-tools.js";
import { RuntimeMailboxAdoptionRefused } from "./runtime-mailbox.js";
import type { LifecycleLog } from "./lifecycle-log.js";
import type { ContinuityState } from "./runtime-controller.js";

/** Discover only a uniquely identified, native-loaded connection owned by this sidecar. */
export const findOwnCodexThread = async (
  rpc: CodexRpcClient, agent: string, cwd: string, signal: AbortSignal,
): Promise<string | undefined> => {
  const loaded = await rpc.request("thread/loaded/list", {}, signal);
  if (!Array.isArray(loaded.data) || loaded.data.length > 128 ||
      loaded.data.some(id => typeof id !== "string")) return undefined;
  const matches: string[] = [];
  for (const id of loaded.data as string[]) {
    signal.throwIfAborted();
    const read = await rpc.request("thread/read", { threadId: id, includeTurns: false }, signal).catch(() => undefined);
    const thread = (read?.thread ?? read) as Record<string, unknown> | undefined;
    if (thread?.id !== id || typeof thread.cwd !== "string" || resolve(thread.cwd) !== resolve(cwd)) continue;
    const response = await rpc.request("mcpServer/tool/call", {
      threadId: id, server: "gptqueue-shared", tool: "get_runtime_status", arguments: {},
    }, signal).catch(() => undefined);
    const status = response?.structuredContent as Record<string, unknown> | undefined;
    if (!response?.isError && status?.status === "ok" && status.agent === agent &&
        (status.runtime === null || (typeof status.runtime === "object" &&
          (status.runtime as Record<string, unknown>).runtime_id === id))) matches.push(id);
    if (matches.length > 1) return undefined;
  }
  return matches.length === 1 ? matches[0] : undefined;
};

/** Truthful binding state while a sidecar is unbound (surfaced by get_runtime_status). */
export type CodexBindingState = Readonly<{
  readiness: "unbound_retrying" | "binding_incomplete" | "bound" | "stopped";
  binding_attempts: number;
  next_attempt_at: string | null;
  /** When the last attempt finished; updated on every attempt, even when nothing is logged. */
  last_attempt_at: string | null;
  /** Why the last attempt did not bind (a stable code, never daemon text). */
  last_binding_outcome: "no_unique_owned_thread" | "daemon_unavailable" | "bind_failed" | "adoption_refused" | "bound" | null;
}>;

export type CodexBootstrapOptions = Readonly<{
  /** Fast attempts before backoff (the original bounded startup budget). */
  fastAttempts?: number;
  fastDelayMs?: number;
  /** Backoff cap once the fast attempts are spent. */
  maxBackoffMs?: number;
  random?: () => number;
  now?: () => number;
  /** Every state change; a caller logs these (never once per attempt). */
  onState?: (state: CodexBindingState) => void;
  /**
   * Every attempt's state, including unchanged retries that onState skips:
   * the externally observable retry status (binding_attempts, next_attempt_at,
   * last_attempt_at) must stay current while duplicate log lines are suppressed.
   */
  onProgress?: (state: CodexBindingState) => void;
}>;

/**
 * WK-6: keep trying to bind for the life of the process. A daemon that is
 * briefly unavailable at startup (reboot, restart) must not leave the
 * sidecar unbound for good. Fast attempts first, then capped exponential
 * backoff with jitter; a daemon notification ends a wait early. Stops once
 * the controller reports a runtime (bound here or by the hook's
 * bind_runtime; whichever is first wins) or on shutdown. Never guesses: it
 * binds only a uniquely owned thread (findOwnCodexThread).
 * `agent` may be a getter: a bind can adopt a different mailbox name, and
 * later attempts must match threads by the name the sidecar now reports.
 * Resolves true when bound, false when stopped by shutdown.
 */
export const bootstrapCodexBinding = async (
  runtime: RuntimeTools, agent: string | (() => string), cwd: string, signal: AbortSignal,
  rpc: CodexRpcClient = new CodexSocketClient(), options: CodexBootstrapOptions = {},
): Promise<boolean> => {
  const currentAgent = typeof agent === "function" ? agent : () => agent;
  const fastAttempts = options.fastAttempts ?? 30;
  const fastDelay = options.fastDelayMs ?? 1_000;
  const maxBackoff = options.maxBackoffMs ?? 30_000;
  const random = options.random ?? Math.random;
  const now = options.now ?? Date.now;
  let state: CodexBindingState = Object.freeze({
    readiness: "unbound_retrying", binding_attempts: 0, next_attempt_at: null, last_attempt_at: null, last_binding_outcome: null });
  // observe: every attempt (status); publish: also a state change (log).
  const observe = (next: CodexBindingState) => { state = next; options.onProgress?.(next); };
  const publish = (next: CodexBindingState) => { observe(next); options.onState?.(next); };
  publish(state);
  // Bound means activation-ready (runtime AND live dispatcher), never merely a
  // runtime recorded by a bind that may still fail (restore/dispatcher start).
  const alreadyBound = () => runtime.status().activation_ready === true;
  const midBind = () => { const r = runtime.status().runtime; return !!r && typeof r === "object"; };
  // The controller's refused adoption (target, reason, first signal): a change is a state change.
  const refusal = () => {
    const c = runtime.status().continuity as Record<string, unknown> | null | undefined;
    return c && typeof c === "object" ? JSON.stringify([c.target, c.reason, c.signal]) : "";
  };
  let lastRefusal = "";
  let wake: AbortController = new AbortController();
  const unsubscribe = rpc.onNotification?.((n) => {
    if (n.method.startsWith("turn/") || n.method.startsWith("thread/")) wake.abort();
  });
  try {
    for (let attempt = 0; !signal.aborted; attempt++) {
      if (alreadyBound()) { publish(Object.freeze({ ...state, readiness: "bound", next_attempt_at: null, last_binding_outcome: "bound" })); return true; }
      let outcome: NonNullable<CodexBindingState["last_binding_outcome"]>;
      try {
        const id = await findOwnCodexThread(rpc, currentAgent(), cwd, signal);
        if (signal.aborted) break;
        if (!id) outcome = "no_unique_owned_thread";
        else {
          const result = await runtime.bind({ client: "codex", runtime_id: id, epoch: id, working_directory: cwd })
            .catch((error: unknown) => error instanceof RuntimeMailboxAdoptionRefused ? "adoption_refused" as const : undefined);
          outcome = result === "adoption_refused" ? result
            : result?.activation_ready === true && (result.runtime as Record<string, unknown> | undefined)?.runtime_id === id
              ? "bound" : "bind_failed";
        }
      } catch {
        if (signal.aborted) break;
        outcome = "daemon_unavailable";
      }
      if (outcome === "bound") {
        publish(Object.freeze({ readiness: "bound", binding_attempts: attempt + 1, next_attempt_at: null,
          last_attempt_at: new Date(now()).toISOString(), last_binding_outcome: "bound" }));
        return true;
      }
      const backoff = attempt + 1 < fastAttempts ? fastDelay
        : Math.min(maxBackoff, fastDelay * 2 ** Math.min(attempt + 1 - fastAttempts + 1, 20));
      const wait = Math.max(1, Math.round(backoff * (0.5 + random() / 2)));
      const at = now();
      const next = Object.freeze({ readiness: midBind() ? "binding_incomplete" as const : "unbound_retrying" as const, binding_attempts: attempt + 1,
        next_attempt_at: new Date(at + wait).toISOString(), last_attempt_at: new Date(at).toISOString(), last_binding_outcome: outcome });
      // State changes only: a new outcome, a new refusal (target, reason or signal), or the switch from fast attempts to backoff.
      const refusedNow = outcome === "adoption_refused" ? refusal() : "";
      const refusalChanged = refusedNow !== lastRefusal;
      lastRefusal = refusedNow;
      if (outcome !== state.last_binding_outcome || refusalChanged || next.readiness !== state.readiness || attempt + 1 === fastAttempts) publish(next); else observe(next);
      wake = new AbortController();
      await delay(wait, undefined, { signal: AbortSignal.any([signal, wake.signal]) }).catch(() => undefined);
    }
    publish(Object.freeze({ ...state, readiness: "stopped", next_attempt_at: null }));
    return false;
  } finally { unsubscribe?.(); await rpc.close(); }
};

/**
 * get_runtime_status while unbound: the controller's status plus the
 * supervisor's truthful retry state. The controller's own fields win: its
 * `continuity` is never overwritten, and its `awaiting_adoption` readiness
 * takes precedence over the supervisor's (R8).
 */
export const withBindingState = (
  status: Record<string, unknown>, state: CodexBindingState | undefined,
): Record<string, unknown> => state && state.readiness !== "stopped" && state.readiness !== "bound" && status.activation_ready !== true
  // A runtime recorded without a live dispatcher is an incomplete bind, not readiness.
  ? { ...status, readiness: status.readiness === "awaiting_adoption" ? "awaiting_adoption" : status.runtime ? "binding_incomplete" : "unbound_retrying",
    binding_attempts: state.binding_attempts, next_attempt_at: state.next_attempt_at, last_attempt_at: state.last_attempt_at,
    last_binding_outcome: state.last_binding_outcome }
  : status;

/**
 * The lifecycle record for a published binding state. A refused adoption
 * carries the controller's classified reason, first blocking signal and
 * target. The bootstrap publishes state changes only, never once per attempt.
 */
export const bindingLifecycleEntry = (state: CodexBindingState, status: Record<string, unknown>): LifecycleLog => {
  const continuity = state.last_binding_outcome === "adoption_refused" ? status.continuity as ContinuityState | null | undefined : undefined;
  return {
    event: state.readiness === "bound" ? "native_binding_ready" : state.readiness === "stopped" ? "native_binding_stopped" : "native_binding_retrying",
    phase: "binding", code: state.last_binding_outcome ?? undefined,
    ...(continuity ? { reason: continuity.reason, signal: continuity.signal ?? undefined, target: continuity.target } : {}),
  };
};
