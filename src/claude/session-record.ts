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
import { chmodSync, closeSync, constants, existsSync, fstatSync, lstatSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
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

/**
 * Why `dir` is not a safe runtime dir for this uid (lstat, never creates it):
 * not a real directory, a symlink, another user's, or open to group/other.
 * Null when safe. Readers must not trust records from an unsafe dir: another
 * user could have pre-created a predictable <tmpdir> path (SEC-F2).
 */
export const unsafeRuntimeDir = (dir: string, uid: number | undefined = process.getuid?.()): string | null => {
  let stat;
  try { stat = lstatSync(dir); } catch { return "missing"; }
  if (stat.isSymbolicLink()) return "is a symlink";
  if (!stat.isDirectory()) return "not a directory";
  if (uid !== undefined && stat.uid !== uid) return `owned by uid ${stat.uid}, not ${uid}`;
  if ((stat.mode & 0o077) !== 0) return `mode ${(stat.mode & 0o777).toString(8)} is open to group/other`;
  return null;
};

/** Create (0700) or validate the per-user runtime dir; throws RuntimeDirError when unsafe. */
export const ensureRuntimeDir = (env: NodeJS.ProcessEnv = process.env): string => {
  const dir = runtimeDir(env);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    chmodSync(dir, 0o700);
  }
  const unsafe = unsafeRuntimeDir(dir);
  if (unsafe !== null) {
    throw new RuntimeDirError("runtime_dir_unsafe", `${dir} must be a directory owned by uid ${process.getuid?.()} with mode 0700 (${unsafe})`);
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
 * Read a regular file owned by this uid without following a symlink at the
 * final component. Null when absent, a symlink, another user's or unreadable.
 */
export const readOwnFile = (path: string, uid: number | undefined = process.getuid?.()): string | null => {
  let fd: number;
  try { fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW); } catch { return null; }
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || (uid !== undefined && stat.uid !== uid)) return null;
    return readFileSync(fd, "utf8");
  } catch { return null; } finally { closeSync(fd); }
};

/**
 * Agent names a record may carry: any name register_agent accepts (spaces,
 * '/', non-ASCII), bounded, with no NUL or control characters. Readers and
 * the publisher agree, so a renamed session never turns its record invalid;
 * hook text JSON-quotes the name (quoteAgent) and commands shell-quote it.
 */
// eslint-disable-next-line no-control-regex
export const RECORD_AGENT = /^[^\u0000-\u001f\u007f-\u009f\u2028\u2029]{1,512}$/u;

/** A name as it appears in hook text: JSON-quoted, so it cannot restructure the surrounding sentence. */
export const quoteAgent = (agent: string): string => JSON.stringify(agent);

/**
 * Record (or, with name === null, remove) the identity the server holds for
 * the Claude process `claude`. Returns the record path. Removal is
 * compare-and-remove (BC-F4): only a record this very server process wrote
 * (same server_pid and server_start) is deleted, so an old server's late
 * exit never removes a newer server's record for the same Claude.
 */
export const publishSessionRecord = (
  claude: Pick<ProcessInfo, "pid" | "start">,
  name: string | null,
  sessionId: string | null,
  env: NodeJS.ProcessEnv = process.env,
  nameOrigin: NonNullable<ClaudeSessionRecord["name_origin"]> = "requested",
): string => {
  const path = recordPath(ensureRuntimeDir(env), processKey(claude));
  const serverStart = readProcess(process.pid)?.start ?? "";
  if (name === null || sessionId === null) {
    let stored: unknown = null;
    try { stored = JSON.parse(readOwnFile(path) ?? "null"); } catch { /* unparseable: not ours to judge */ }
    const own = stored as Partial<ClaudeSessionRecord> | null;
    if (own && own.server_pid === process.pid && own.server_start === serverStart) rmSync(path, { force: true });
    return path;
  }
  const record: ClaudeSessionRecord = {
    version: 1,
    agent: name,
    gptqueue_session_id: sessionId,
    claude_pid: claude.pid,
    claude_start: claude.start,
    server_pid: process.pid,
    server_start: serverStart,
    written_at: new Date().toISOString(),
    name_origin: nameOrigin,
  };
  writeAtomic(path, JSON.stringify(record));
  return path;
};

const isRecord = (value: unknown): value is ClaudeSessionRecord => {
  const r = value as Partial<ClaudeSessionRecord> | null;
  return !!r && r.version === 1 && typeof r.agent === "string" && r.agent.length <= 512 && RECORD_AGENT.test(r.agent)
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
  const unsafe = unsafeRuntimeDir(dir);
  if (unsafe === "missing") return { kind: "none" };
  if (unsafe !== null) return { kind: "stale", path: dir, reason: `runtime dir ${dir} is unsafe (${unsafe})` };
  for (const info of chain) {
    const path = recordPath(dir, processKey(info));
    const raw = readOwnFile(path);
    if (raw === null) continue;
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

/**
 * A watcher's pidfile. claude_pid/claude_start name the Claude process the
 * watcher serves (LC-F2): a watcher that outlived its Claude (SIGKILL, OOM)
 * must not satisfy a resumed session's Stop gate.
 */
export type WatcherPid = Readonly<{ pid: number; start: string; agent: string; claude_pid: number | null; claude_start: string | null }>;

export const readWatcherPid = (path: string): WatcherPid | null => {
  try {
    const parsed = JSON.parse(readOwnFile(path) ?? "null") as Partial<WatcherPid> | null;
    if (!parsed || typeof parsed.pid !== "number" || typeof parsed.start !== "string" || typeof parsed.agent !== "string") return null;
    return Object.freeze({
      pid: parsed.pid, start: parsed.start, agent: parsed.agent,
      claude_pid: typeof parsed.claude_pid === "number" ? parsed.claude_pid : null,
      claude_start: typeof parsed.claude_start === "string" ? parsed.claude_start : null,
    });
  } catch { return null; }
};

export const writeWatcherPid = (path: string, value: WatcherPid): void => writeAtomic(path, JSON.stringify(value));

export type ClaudeProcess = Readonly<{ pid: number; start: string }>;

/**
 * A watcher counts as running only for the same agent, a live same-start
 * pid, and the caller's own Claude process (same pid and start time). A
 * caller that cannot name its Claude process never counts one as running.
 */
export const watcherRunning = (path: string, agent: string, claude: ClaudeProcess | null): boolean => {
  const pid = readWatcherPid(path);
  return pid !== null && claude !== null && pid.agent === agent
    && pid.claude_pid === claude.pid && pid.claude_start === claude.start
    && processAlive(pid.pid, pid.start);
};
