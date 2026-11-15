import { mkdir, open, readdir, stat, unlink, type FileHandle } from "node:fs/promises";
import { constants } from "node:fs";
import { isAbsolute, join } from "node:path";
import { homedir } from "node:os";
import type { AdoptionRefusalReason } from "../core/adoption-refusal.js";

export type LifecycleEvent =
  | "startup_started" | "registration_complete" | "transport_connected"
  | "mcp_initialized" | "startup_failed" | "shutdown_started"
  | "shutdown_complete" | "shutdown_failed" | "native_binding_started"
  | "native_binding_ready" | "native_binding_unavailable" | "native_binding_retrying"
  | "native_binding_stopped";

export interface LifecycleLog {
  readonly event: LifecycleEvent;
  readonly phase?: string;
  readonly duration_ms?: number;
  readonly code?: string;
  /** A shutdown's cause, or (code adoption_refused) the classified refusal reason. */
  readonly reason?: "caller_abort" | "transport_closed" | "explicit_close" | "startup_failed" | "runtime_closed" | AdoptionRefusalReason;
  /** Code adoption_refused only: the first blocking signal, and the mailbox whose adoption was refused. */
  readonly signal?: string;
  readonly target?: string;
}

/** Honor the configured state location and otherwise use the user's state directory. */
export const defaultLogDir = (): string => {
  const configured = process.env.GPTQ_LOG_DIR?.trim();
  if (configured && isAbsolute(configured)) return configured;
  if (isAbsolute(process.env.XDG_STATE_HOME ?? "")) return join(process.env.XDG_STATE_HOME!, "gptqueue", "logs");
  return join(homedir(), ".local", "state", "gptqueue", "logs");
};

export const safeLifecycleCode = (error: unknown): string => {
  if (!(error instanceof Error)) return "unknown_error";
  if (/timed out/iu.test(error.message)) return "timeout";
  if (/abort/iu.test(error.message)) return "aborted";
  return "operation_failed";
};

export const createLifecycleLog = (agentName: string, client: string, start = process.hrtime.bigint()): {
  emit: (entry: LifecycleLog & { session_id?: string }) => void;
  elapsed: () => number;
  close: () => Promise<void>;
} => {
  const started = start;
  const stateRoot = defaultLogDir();
  const path = join(stateRoot, `${agentName}.jsonl`);
  let warning = false;
  let fileReady: Promise<FileHandle> | undefined;
  let writes = Promise.resolve();
  let closing: Promise<void> | undefined;
  const elapsed = () => Number(process.hrtime.bigint() - started) / 1e6;
  const warn = () => {
    if (warning) return;
    warning = true;
    try {
      console.error(JSON.stringify({ schema_version: 1, timestamp: new Date().toISOString(),
        event: "logging_failed", client, pid: process.pid, agent_name: agentName,
        elapsed_ms: Math.round(elapsed() * 100) / 100, code: "log_unavailable" }));
    } catch { /* Logging must never interrupt connection ownership. */ }
  };
  const ensureFile = (): Promise<FileHandle> => fileReady ??= (async () => {
    await mkdir(stateRoot, { recursive: true, mode: 0o700 });
    // A connection owns a fresh file; never reuse a pre-existing target or symlink.
    return open(path, constants.O_APPEND | constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
  })();
  const close = (): Promise<void> => closing ??= (async () => {
    await writes;
    if (!fileReady) return;
    const handle = await fileReady;
    try { await handle.sync(); } finally { await handle.close(); }
  })().catch(warn);
  const emit = (entry: LifecycleLog & { session_id?: string }): void => {
    const record = Object.freeze({ schema_version: 1, timestamp: new Date().toISOString(), event: entry.event,
      client, pid: process.pid, agent_name: agentName, ...(entry.session_id ? { session_id: entry.session_id } : {}),
      elapsed_ms: Math.round(elapsed() * 100) / 100, ...(entry.phase ? { phase: entry.phase } : {}),
      ...(entry.duration_ms === undefined ? {} : { duration_ms: Math.round(entry.duration_ms * 100) / 100 }),
      ...(entry.code ? { code: entry.code } : {}), ...(entry.reason ? { reason: entry.reason } : {}),
      ...(entry.signal ? { signal: entry.signal } : {}), ...(entry.target ? { target: entry.target } : {}) });
    const line = JSON.stringify(record);
    try { console.error(line); } catch { /* stderr is best effort */ }
    if (closing || warning) return;
    writes = writes.then(async () => {
      const handle = await ensureFile();
      await handle.appendFile(`${line}\n`);
    }).catch(warn);
  };
  return Object.freeze({ emit, elapsed, close });
};

export interface HookBindingLog {
  emit: (event: string, fields: Readonly<Record<string, string | number | boolean | undefined>>) => void;
  close: () => Promise<void>;
}

/** Hook binding logs this client owns that fall older than this are deleted when the next one opens. */
export const HOOK_LOG_RETENTION_MS = 14 * 24 * 60 * 60 * 1000;

/** Delete this hook's own log files older than the cutoff; retention is best effort (R3). */
const pruneExpiredHookLogs = async (stateRoot: string, client: string, cutoffMs: number): Promise<void> => {
  const prefix = `hook-${client}-`;
  for (const name of await readdir(stateRoot).catch(() => [] as string[])) {
    if (!name.startsWith(prefix) || !name.endsWith(".jsonl")) continue;
    const path = join(stateRoot, name);
    const info = await stat(path).catch(() => null);
    if (info?.isFile() === true && info.mtimeMs < cutoffMs) await unlink(path).catch(() => undefined);
  }
};

/**
 * Durable hook binding diagnostics: one append-only file per client, session
 * and UTC day — repeated invocations (one per prompt) append to the same day's
 * file instead of minting a new file each time, and this hook's own files
 * older than HOOK_LOG_RETENTION_MS are deleted when the next one opens (R3).
 * Records carry only safe codes and counters — never message payloads,
 * transport errors, or credentials. Failure to log is reported once on stderr
 * and never interrupts binding or hook exit semantics.
 */
export const createHookLog = (
  client: string, sessionId: string, hookEvent: string,
  options: Readonly<{ dir?: string; now?: () => Date }> = {},
): HookBindingLog => {
  const stateRoot = options.dir ?? defaultLogDir();
  const now = options.now ?? (() => new Date());
  const day = now().toISOString().slice(0, 10);
  const safeSession = /^[0-9a-f-]{36}$/iu.test(sessionId) ? sessionId : "unparsed";
  const path = join(stateRoot, `hook-${client}-${safeSession}-${day}.jsonl`);
  let warning = false;
  let fileReady: Promise<FileHandle> | undefined;
  let writes = Promise.resolve();
  let closing: Promise<void> | undefined;
  const warn = () => {
    if (warning) return;
    warning = true;
    try { console.error(JSON.stringify({ schema_version: 1, event: "logging_failed", client, code: "log_unavailable" })); } catch { /* best effort */ }
  };
  const ensureFile = (): Promise<FileHandle> => fileReady ??= (async () => {
    await mkdir(stateRoot, { recursive: true, mode: 0o700 });
    await pruneExpiredHookLogs(stateRoot, client, now().getTime() - HOOK_LOG_RETENTION_MS);
    // One file per client/session/day: same-day invocations append. Never follow a symlink.
    return open(path, constants.O_APPEND | constants.O_CREAT | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
  })();
  const emit = (event: string, fields: Readonly<Record<string, string | number | boolean | undefined>> = {}): void => {
    const record = Object.freeze({ schema_version: 1, timestamp: now().toISOString(),
      event, client, pid: process.pid, session_id: sessionId, hook_event: hookEvent,
      ...Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined)) });
    const line = JSON.stringify(record);
    try { console.error(line); } catch { /* stderr is best effort */ }
    if (closing || warning) return;
    writes = writes.then(async () => {
      const handle = await ensureFile();
      await handle.appendFile(`${line}\n`);
    }).catch(warn);
  };
  const close = (): Promise<void> => closing ??= (async () => {
    await writes;
    if (!fileReady) return;
    const handle = await fileReady;
    try { await handle.sync(); } finally { await handle.close(); }
  })().catch(warn);
  return Object.freeze({ emit, close });
};
