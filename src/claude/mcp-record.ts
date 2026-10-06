/**
 * Publish this MCP server's held identity for the enclosing Claude session
 * (see session-record.ts for the key). Enabled by GPTQ_CLAUDE_SESSION_RECORD=1,
 * which bin/gptqueue-claude-mcp sets; other runtimes are unaffected.
 */
import { randomBytes } from "node:crypto";
import { SESSION_DEFAULTS } from "../core/keys.js";
import { GptQueueError } from "../mcp-server/errors.js";
import { stableToolError } from "../mcp-server/tool-result.js";
import { processKey, publishSessionRecord, readProcess, type ProcessInfo } from "./session-record.js";

export type IdentitySource = { onIdentityChange(observer: (name: string | null, sessionId: string | null) => void): void; agentName: string | null; sessionId: string | null };

export const claudeProcess = (env: NodeJS.ProcessEnv = process.env): ProcessInfo | null =>
  readProcess(env.GPTQ_CLAUDE_PID ? Number(env.GPTQ_CLAUDE_PID) : process.ppid);

/** Returns a cleanup that removes the record, or null when disabled/unavailable. */
export type ClaudeRecordHandle = Readonly<{ remove: () => void; setOrigin: (origin: NameOrigin) => void }>;

export const startClaudeSessionRecord = (client: IdentitySource, env: NodeJS.ProcessEnv = process.env): ClaudeRecordHandle | null => {
  if (env.GPTQ_CLAUDE_SESSION_RECORD !== "1") return null;
  const claude = claudeProcess(env);
  if (!claude) return null;
  let origin: NameOrigin = "requested";
  const publish = (name: string | null, sessionId: string | null) => {
    try { publishSessionRecord(claude, name, sessionId, env, origin); }
    catch (error) { process.stderr.write(`${JSON.stringify({ event: "claude_session_record_failed", key: processKey(claude), message: (error as Error).message })}\n`); }
  };
  client.onIdentityChange(publish);
  publish(client.agentName, client.sessionId);
  return Object.freeze({ remove: () => publish(null, null), setOrigin: (next: NameOrigin) => { origin = next; } });
};

/** True for the core's typed refusal AGENT_NAME_HELD of a name held by a live session (decision D2; compared case-insensitively). */
export const isNameHeld = (error: unknown): boolean => {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && code.toLowerCase() === "agent_name_held";
};

export type NameOrigin = "requested" | "session_id" | "pid" | "random";

/**
 * Candidate names after the requested one is held, in order: the Claude
 * session-id prefix (when the env carries one), the Claude pid, then random
 * suffixes. A Claude launched from inside another Claude inherits the
 * parent's CLAUDE_* session id, so later candidates must not depend on it.
 */
export const fallbackCandidates = (
  name: string,
  env: NodeJS.ProcessEnv = process.env,
  claudePid: number | null = claudeProcess(env)?.pid ?? null,
  random: () => string = () => randomBytes(4).toString("hex"),
): readonly Readonly<{ name: string; origin: NameOrigin }>[] => {
  const sessionId = (env.CLAUDE_SESSION_ID || env.CLAUDE_CODE_SESSION_ID || "").replace(/[^A-Za-z0-9]/g, "").slice(0, 8);
  const candidates: { name: string; origin: NameOrigin }[] = [];
  if (sessionId) candidates.push({ name: `${name}-${sessionId}`, origin: "session_id" });
  candidates.push({ name: `${name}-${claudePid ?? process.pid}`, origin: "pid" });
  candidates.push({ name: `${name}-${random()}`, origin: "random" }, { name: `${name}-${random()}`, origin: "random" });
  return Object.freeze(candidates.filter((c, i) => candidates.findIndex((o) => o.name === c.name) === i));
};

/** Kept for callers of the first candidate. */
export const fallbackName = (name: string, env: NodeJS.ProcessEnv = process.env, claudePid: number | null = claudeProcess(env)?.pid ?? null): string =>
  fallbackCandidates(name, env, claudePid)[0]!.name;

export type Registrar = { register(role: string, name: string): Promise<{ name: string; session_id: string }> };
export type RegisterDeps = Readonly<{ sleep: (ms: number) => Promise<void>; now: () => number; maxLeaseWaitMs: number; random?: () => string }>;
const DEFAULT_DEPS: RegisterDeps = Object.freeze({
  sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)),
  now: () => Date.now(),
  maxLeaseWaitMs: (SESSION_DEFAULTS.LEASE_TTL_SECONDS + 2) * 1000,
});

type HeldDetails = { holder_liveness_verifiable?: unknown; holder_lease_ttl_ms?: unknown };
const heldDetails = (error: unknown): HeldDetails => ((error as { details?: HeldDetails } | null)?.details ?? {});

export class NameExhaustedError extends Error {
  readonly code = "AGENT_NAME_HELD";
  constructor(readonly tried: readonly string[]) {
    super(`AGENT_NAME_HELD: every candidate name is held by a live session (tried ${tried.join(", ")}); set GPTQ_AGENT_NAME to a free name or GPTQ_AGENT_SHARED=1 to share`);
  }
}

/**
 * Register `name` (Claude only: GPTQ_CLAUDE_SESSION_RECORD=1, and not when
 * GPTQ_AGENT_SHARED=1). On AGENT_NAME_HELD:
 *  1. ID-3: a holder registered by older code has no liveness metadata, so
 *     only its lease can show it is dead. Wait for that lease to lapse and
 *     retry, for at most lease TTL + 2 s in total. A holder with metadata was
 *     already judged live by pid, so there is no wait.
 *  2. Then try fallbackCandidates() in order, a bounded number of times.
 * The result says which name origin was used; onRegistered sees it before the
 * register call so the session record can carry it.
 */
export const registerClaudeAware = async (
  client: Registrar,
  name: string,
  env: NodeJS.ProcessEnv = process.env,
  deps: RegisterDeps = DEFAULT_DEPS,
  onOrigin: (origin: NameOrigin) => void = () => undefined,
): Promise<{ name: string; session_id: string; origin: NameOrigin }> => {
  onOrigin("requested");
  let lastError: unknown;
  try {
    return { ...(await client.register("both", name)), origin: "requested" };
  } catch (error) {
    if (!isNameHeld(error) || env.GPTQ_CLAUDE_SESSION_RECORD !== "1" || env.GPTQ_AGENT_SHARED === "1") throw error;
    lastError = error;
  }
  const deadline = deps.now() + deps.maxLeaseWaitMs;
  while (heldDetails(lastError).holder_liveness_verifiable === false && deps.now() < deadline) {
    const ttl = Number(heldDetails(lastError).holder_lease_ttl_ms);
    const wait = Math.min(Math.max(Number.isFinite(ttl) && ttl > 0 ? ttl + 100 : 250, 50), deadline - deps.now());
    process.stderr.write(`${JSON.stringify({ event: "agent_name_held_awaiting_lease", requested: name, wait_ms: wait })}\n`);
    await deps.sleep(wait);
    try {
      return { ...(await client.register("both", name)), origin: "requested" };
    } catch (error) {
      if (!isNameHeld(error)) throw error;
      lastError = error;
    }
  }
  const tried = [name];
  for (const candidate of fallbackCandidates(name, env, claudeProcess(env)?.pid ?? null, deps.random)) {
    tried.push(candidate.name);
    onOrigin(candidate.origin);
    try {
      const registered = await client.register("both", candidate.name);
      process.stderr.write(`${JSON.stringify({ event: "agent_name_held_fallback", requested: name, registered: candidate.name, origin: candidate.origin })}\n`);
      return { ...registered, origin: candidate.origin };
    } catch (error) {
      if (!isNameHeld(error)) throw error;
    }
  }
  throw new NameExhaustedError(tried);
};

/** Whether a registration failure is transient: the same classification (and retryable flag) a tool reply would carry. */
export const isRetryableRegistrationError = (error: unknown): boolean =>
  (stableToolError(error).structuredContent as { error?: { retryable?: unknown } }).error?.retryable === true;

export type RetryDeps = Readonly<{ sleep: (ms: number) => Promise<void>; baseMs: number; maxMs: number }>;
const DEFAULT_RETRY: RetryDeps = Object.freeze({ sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)), baseMs: 500, maxMs: 30_000 });

/**
 * BC-F1: run `attempt` until it succeeds or fails with a NON-retryable error
 * (e.g. AGENT_NAME_HELD after every fallback). A retryable failure (Redis
 * unavailable at startup) is retried in the background with capped
 * exponential backoff, so it never becomes a permanent refusal; `onRetry`
 * sees each such failure (the hold gate reports it to waiting calls).
 */
export const registerUntilFinal = async <T>(
  attempt: () => Promise<T>,
  onRetry: (error: unknown, delayMs: number) => void = () => undefined,
  deps: RetryDeps = DEFAULT_RETRY,
): Promise<T> => {
  for (let failures = 0; ; failures += 1) {
    try { return await attempt(); } catch (error) {
      if (!isRetryableRegistrationError(error)) throw error;
      const delay = Math.min(deps.maxMs, deps.baseMs * 2 ** Math.min(failures, 20));
      onRetry(error, delay);
      await deps.sleep(delay);
    }
  }
};

type HeldMessage = { readonly method?: unknown; readonly id?: unknown };
type MessageTransport<M> = {
  onmessage?: (message: M, extra?: never) => void;
  onclose?: () => void;
  send?: (message: { jsonrpc: "2.0"; id: string | number; result: Record<string, unknown> }) => Promise<void>;
};

/** Default bound on holding tool calls: covers the 32 s old-holder lease wait plus registration. */
export const HOLD_TOOL_CALLS_MS = 45_000;

/**
 * Delay incoming tools/call requests until `gate` settles, so tools never run
 * against a half-registered identity. Every other message (initialize,
 * tools/list, notifications) passes straight through. Held calls keep their
 * arrival order. If registration succeeds they are delivered; if it fails,
 * or does not settle within `timeoutMs`, each held call is answered with a
 * typed tool error (the registration's own code, or REGISTRATION_PENDING,
 * retryable, while no final outcome exists yet).
 * The gate rejects only on a final (non-retryable) failure; while it keeps
 * retrying, a timed-out call is answered with the latest transient error
 * (`lastError`, e.g. REDIS_UNAVAILABLE, retryable: true) and a later success
 * still turns the gate to ok.
 * If the transport closes first, held calls are dropped, never delivered.
 * Must be called after server.connect() has installed onmessage/onclose.
 */
export const holdToolCallsUntil = <M>(
  transport: MessageTransport<M>, gate: Promise<unknown>, timeoutMs = HOLD_TOOL_CALLS_MS,
  lastError: () => unknown = () => null,
): void => {
  const deliver = transport.onmessage;
  if (!deliver) return;
  // pending: hold; ok: pass through; refused: answer with the typed error.
  // Retries happen behind the gate (registerUntilFinal); a registration that
  // completes after the timeout still turns this to ok.
  let outcome: { kind: "pending" } | { kind: "ok" } | { kind: "refused"; error: unknown; final: boolean } = { kind: "pending" };
  let closed = false;
  const held: Array<{ message: M; extra?: never }> = [];
  const answer = (message: M, error: unknown) => {
    const id = (message as HeldMessage | null)?.id;
    if (closed || (typeof id !== "string" && typeof id !== "number")) return;
    void transport.send?.({ jsonrpc: "2.0", id, result: stableToolError(error) as unknown as Record<string, unknown> })
      .catch(() => undefined);
  };
  const timer = setTimeout(() => {
    if (outcome.kind !== "pending") return;
    const error = lastError() ?? new GptQueueError("REGISTRATION_PENDING",
      `GPTQueue registration is still in progress after ${Math.round(timeoutMs / 1000)} s; retry the tool call shortly.`);
    outcome = { kind: "refused", error, final: false };
    for (const { message } of held.splice(0)) answer(message, error);
  }, timeoutMs);
  timer.unref?.();
  gate.then(
    () => {
      clearTimeout(timer);
      if (outcome.kind === "refused" && outcome.final) return;
      outcome = { kind: "ok" };
      for (const { message, extra } of held.splice(0)) if (!closed) deliver(message, extra);
    },
    (error: unknown) => {
      clearTimeout(timer);
      outcome = { kind: "refused", error, final: true };
      for (const { message } of held.splice(0)) answer(message, error);
    },
  );
  const previousClose = transport.onclose;
  transport.onclose = () => {
    closed = true;
    held.length = 0;
    clearTimeout(timer);
    previousClose?.();
  };
  transport.onmessage = (message: M, extra?: never) => {
    if ((message as HeldMessage | null)?.method !== "tools/call" || outcome.kind === "ok") { deliver(message, extra); return; }
    if (outcome.kind === "refused") { answer(message, outcome.error); return; }
    if (!closed) held.push({ message, extra });
  };
};
