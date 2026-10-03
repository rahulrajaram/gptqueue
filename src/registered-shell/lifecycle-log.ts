import { mkdir, open, type FileHandle } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { constants } from "node:fs";
import { isAbsolute, join } from "node:path";
import { homedir } from "node:os";

export type LifecycleEvent =
  | "startup_started" | "registration_complete" | "transport_connected"
  | "mcp_initialized" | "startup_failed" | "shutdown_started"
  | "shutdown_complete" | "shutdown_failed" | "native_binding_started"
  | "native_binding_ready" | "native_binding_unavailable";

export interface LifecycleLog {
  readonly event: LifecycleEvent;
  readonly phase?: string;
  readonly duration_ms?: number;
  readonly code?: string;
  readonly reason?: "caller_abort" | "transport_closed" | "explicit_close" | "startup_failed";
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
      ...(entry.code ? { code: entry.code } : {}), ...(entry.reason ? { reason: entry.reason } : {}) });
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

/**
 * Durable startup-hook binding diagnostics: one exclusive append-only file per
 * hook invocation, correlated by session and hook event. Records carry only
 * safe codes and counters — never message payloads, transport errors, or
 * credentials. Failure to log is reported once on stderr and never interrupts
 * binding or hook exit semantics.
 */
export const createHookLog = (
  client: string, sessionId: string, hookEvent: string,
  options: Readonly<{ dir?: string; now?: () => Date }> = {},
): HookBindingLog => {
  const stateRoot = options.dir ?? defaultLogDir();
  const stamp = (options.now ?? (() => new Date()))().toISOString().replace(/[:.]/gu, "-");
  const safeSession = /^[0-9a-f-]{36}$/iu.test(sessionId) ? sessionId : "unparsed";
  const path = join(stateRoot, `hook-${client}-${safeSession}-${stamp}-${randomBytes(4).toString("hex")}.jsonl`);
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
    // Fresh file per invocation; never follow a symlink or reuse an existing target.
    return open(path, constants.O_APPEND | constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
  })();
  const emit = (event: string, fields: Readonly<Record<string, string | number | boolean | undefined>> = {}): void => {
    const record = Object.freeze({ schema_version: 1, timestamp: (options.now ?? (() => new Date()))().toISOString(),
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
