import { mkdir, open, type FileHandle } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";

export type LifecycleEvent =
  | "startup_started" | "registration_complete" | "transport_connected"
  | "mcp_initialized" | "startup_failed" | "shutdown_started"
  | "shutdown_complete" | "shutdown_failed";

export interface LifecycleLog {
  readonly event: LifecycleEvent;
  readonly phase?: string;
  readonly duration_ms?: number;
  readonly code?: string;
  readonly reason?: "caller_abort" | "transport_closed" | "explicit_close" | "startup_failed";
}

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
  const stateRoot = process.env.GPTQ_LOG_DIR ?? join(
    isAbsolute(process.env.XDG_STATE_HOME ?? "") ? process.env.XDG_STATE_HOME! : join(homedir(), ".local", "state"),
    "gptqueue", "logs"
  );
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
