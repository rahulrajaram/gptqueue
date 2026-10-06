/**
 * Where a Claude Code session's gptqueue MCP server records the name and
 * gptqueue session it actually holds, so the session's hooks, inbox peek and
 * watcher act on that inbox instead of re-deriving a name from cwd (ID-2).
 *
 * KEY. Claude Code starts the MCP server and runs every hook and Bash-tool
 * command as descendants of the same Claude process. The server runs
 * in-process under bin/gptqueue-claude-mcp, so its parent IS that Claude
 * process. A record is keyed by `<claude pid>-<claude start time>` (the start
 * time is /proc/<pid>/stat field 22, so a recycled pid never matches an old
 * record). A reader walks its own ancestry and takes the record keyed by the
 * nearest ancestor that has one. GPTQ_CLAUDE_PID overrides the server's key
 * when a launcher sits between Claude and the server.
 *
 * DIRECTORY. `$GPTQ_CLAUDE_RUNTIME_DIR`, else `$XDG_RUNTIME_DIR/gptqueue-claude`,
 * else `<tmpdir>/gptqueue-claude-<uid>`. Created 0700; refused when owned by
 * another user or accessible to group/other.
 */
import { chmodSync, existsSync, lstatSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export type ProcessInfo = Readonly<{ pid: number; ppid: number; start: string }>;

/** Parse /proc/<pid>/stat. The comm field may contain spaces and parens. */
export const parseProcStat = (pid: number, text: string): ProcessInfo | null => {
  const close = text.lastIndexOf(")");
  if (close < 0) return null;
  const fields = text.slice(close + 2).trim().split(/\s+/);
  const ppid = Number(fields[1]);
  const start = fields[19];
  return Number.isInteger(ppid) && start ? Object.freeze({ pid, ppid, start }) : null;
};

export const readProcess = (pid: number): ProcessInfo | null => {
  try { return parseProcStat(pid, readFileSync(`/proc/${pid}/stat`, "utf8")); } catch { return null; }
};

/** The process itself first, then each parent up to (excluding) pid 1. */
export const ancestry = (pid: number, read = readProcess): readonly ProcessInfo[] => {
  const chain: ProcessInfo[] = [];
  for (let current = read(pid); current && current.pid > 1 && chain.length < 64; current = read(current.ppid)) {
    chain.push(current);
  }
  return Object.freeze(chain);
};

export const processKey = (info: Pick<ProcessInfo, "pid" | "start">): string => `${info.pid}-${info.start}`;

/** True when `pid` is alive and (if given) still has the recorded start time. */
export const processAlive = (pid: number, start?: string): boolean => {
  const info = readProcess(pid);
  return info !== null && (start === undefined || info.start === start);
};

export class RuntimeDirError extends Error {
  constructor(readonly code: "runtime_dir_unsafe", message: string) { super(message); }
}

export const runtimeDir = (env: NodeJS.ProcessEnv = process.env): string =>
  env.GPTQ_CLAUDE_RUNTIME_DIR
  || (env.XDG_RUNTIME_DIR ? join(env.XDG_RUNTIME_DIR, "gptqueue-claude") : join(tmpdir(), `gptqueue-claude-${process.getuid?.() ?? "user"}`));

/** Create (0700) or validate the per-user runtime dir; throws RuntimeDirError when unsafe. */
export const ensureRuntimeDir = (env: NodeJS.ProcessEnv = process.env): string => {
  const dir = runtimeDir(env);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    chmodSync(dir, 0o700);
  }
  const stat = lstatSync(dir);
  const uid = process.getuid?.();
  if (!stat.isDirectory() || (uid !== undefined && stat.uid !== uid) || (stat.mode & 0o077) !== 0) {
    throw new RuntimeDirError("runtime_dir_unsafe", `${dir} must be a directory owned by uid ${uid} with mode 0700`);
  }
  return dir;
};

export type ClaudeSessionRecord = Readonly<{
  version: 1;
  agent: string;
  gptqueue_session_id: string;
  claude_pid: number;
  claude_start: string;
  server_pid: number;
  /** /proc start time of server_pid, so a recycled pid never validates a stale record. */
  server_start: string;
  written_at: string;
  /** Which candidate the name is: the requested one, or an AGENT_NAME_HELD fallback. */
  name_origin?: "requested" | "session_id" | "pid" | "random";
}>;

const recordPath = (dir: string, key: string) => join(dir, `session-${key}.json`);

const writeAtomic = (path: string, body: string): void => {
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, body, { mode: 0o600 });
  renameSync(tmp, path);
};

/**
 * Record (or, with name === null, remove) the identity the server holds for
 * the Claude process `claude`. Returns the record path.
 */
export const publishSessionRecord = (
  claude: Pick<ProcessInfo, "pid" | "start">,
  name: string | null,
  sessionId: string | null,
  env: NodeJS.ProcessEnv = process.env,
  nameOrigin: NonNullable<ClaudeSessionRecord["name_origin"]> = "requested",
): string => {
  const path = recordPath(ensureRuntimeDir(env), processKey(claude));
  if (name === null || sessionId === null) {
    rmSync(path, { force: true });
    return path;
  }
  const record: ClaudeSessionRecord = {
    version: 1,
    agent: name,
    gptqueue_session_id: sessionId,
    claude_pid: claude.pid,
    claude_start: claude.start,
    server_pid: process.pid,
    server_start: readProcess(process.pid)?.start ?? "",
    written_at: new Date().toISOString(),
    name_origin: nameOrigin,
  };
  writeAtomic(path, JSON.stringify(record));
  return path;
};

const isRecord = (value: unknown): value is ClaudeSessionRecord => {
  const r = value as Partial<ClaudeSessionRecord> | null;
  return !!r && r.version === 1 && typeof r.agent === "string" && r.agent.length > 0
    && typeof r.gptqueue_session_id === "string" && typeof r.claude_pid === "number"
    && typeof r.claude_start === "string" && typeof r.server_pid === "number"
    && typeof r.server_start === "string" && r.server_start.length > 0;
};

export type RecordLookup =
  | Readonly<{ kind: "found"; record: ClaudeSessionRecord; path: string }>
  | Readonly<{ kind: "stale"; path: string; reason: string }>
  | Readonly<{ kind: "none" }>;

/**
 * Find the record for the Claude process that is the nearest ancestor of
 * `chain[0]` with a record. A record whose server process is gone is stale:
 * the name may since have been released, so callers must not trust it.
 */
export const findSessionRecord = (
  chain: readonly ProcessInfo[],
  env: NodeJS.ProcessEnv = process.env,
  alive: (pid: number, start: string) => boolean = (pid, start) => processAlive(pid, start),
): RecordLookup => {
  const dir = runtimeDir(env);
  for (const info of chain) {
    const path = recordPath(dir, processKey(info));
    let raw: string;
    try { raw = readFileSync(path, "utf8"); } catch { continue; }
    let parsed: unknown;
    try { parsed = JSON.parse(raw); } catch { return { kind: "stale", path, reason: "unparseable record" }; }
    if (!isRecord(parsed)) return { kind: "stale", path, reason: "invalid record" };
    if (!alive(parsed.server_pid, parsed.server_start)) return { kind: "stale", path, reason: `server pid ${parsed.server_pid} (start ${parsed.server_start}) is not running` };
    return { kind: "found", record: parsed, path };
  }
  return { kind: "none" };
};

/** Watcher pidfile, keyed by the Claude session_id from hook input. */
export const watcherPidPath = (dir: string, claudeSessionId: string): string =>
  join(dir, `watch-${claudeSessionId.replace(/[^A-Za-z0-9._-]/g, "_")}.pid`);

export type WatcherPid = Readonly<{ pid: number; start: string; agent: string }>;

export const readWatcherPid = (path: string): WatcherPid | null => {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as Partial<WatcherPid>;
    return typeof parsed.pid === "number" && typeof parsed.start === "string" && typeof parsed.agent === "string"
      ? Object.freeze({ pid: parsed.pid, start: parsed.start, agent: parsed.agent }) : null;
  } catch { return null; }
};

export const writeWatcherPid = (path: string, value: WatcherPid): void => writeAtomic(path, JSON.stringify(value));

/** A watcher counts as running only for the same agent and a live, same-start pid. */
export const watcherRunning = (path: string, agent: string): boolean => {
  const pid = readWatcherPid(path);
  return pid !== null && pid.agent === agent && processAlive(pid.pid, pid.start);
};
