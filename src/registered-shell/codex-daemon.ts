/**
 * Read-only discovery of the ONE Codex app-server daemon that owns a selected
 * control socket, plus that daemon's children and its gptqueue session
 * sidecars.
 *
 * Every piece of evidence comes from /proc (root injectable for tests; no
 * public CLI switch): /proc/net/unix maps the selected socket path to its
 * inode, the fd table of exactly one process must hold socket:[<inode>], and
 * that process's argv must token-wise confirm `codex ... app-server ...
 * --managed-daemon` with a `unix://` --listen endpoint. Token-wise means each
 * expected string is one NUL-separated argv token: an extra flag anywhere
 * (field daemons carry --analytics-default-enabled) cannot break the match,
 * and a decoy process carrying the whole pattern inside one argv token (for
 * example `sh -c 'codex app-server ...'`) cannot match. No pgrep, no
 * substring over a joined command line, no head -1.
 *
 * The --listen value is NOT required to spell out the selected socket path:
 * the field daemon may listen on a bare `unix://` (its CODEX_HOME default).
 * When the socket owner is proven by fd ownership, that ownership alone
 * selects the daemon. Missing or unreadable ownership refuses; argv alone
 * cannot prove which live socket a process owns. Multiple matching inodes or
 * owners refuse even when aliases refer to the same filesystem path.
 *
 * Ownership discovery supports same-UID daemons only. Every visible process
 * must have readable UID metadata. Within that UID, complete readable argv
 * admits only eligible Codex app-server candidates to descriptor inspection.
 * Known noncandidates and fully foreign-UID processes are outside the fd
 * ownership domain. Unknown UID/argv or eligible-candidate fd evidence refuses.
 * This is a cooperative operational boundary, not a hostile same-UID/root proof.
 * Child snapshots still classify every visible process by parent, regardless
 * of UID, and refuse incomplete evidence rather than inventing zero counts.
 */
import { readdir, readFile, readlink, realpath } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { parseRegisteredShellOptions } from "./args.js";

export type DiscoveryMode =
  | "proc_unreadable"
  | "socket_not_listed"
  | "ambiguous_socket"
  | "no_owner"
  | "ambiguous_owner"
  | "owner_argv_unreadable"
  | "owner_not_codex_app_server"
  | "daemon_not_found"
  | "ambiguous_daemon"
  | "daemon_gone"
  | "daemon_replaced";

export class DiscoveryError extends Error {
  constructor(
    readonly mode: DiscoveryMode,
    message: string,
    readonly detail: Readonly<Record<string, unknown>> = {},
  ) {
    super(message);
    this.name = "DiscoveryError";
  }
}

/** The pinned identity of the selected daemon. */
export type DaemonIdentity = Readonly<{
  pid: number;
  /** /proc/<pid>/stat field 22 (start time in clock ticks): pins the pid against reuse. */
  startTicks: string;
  /** /proc/<pid>/ns/pid link target (e.g. "pid:[4026531836]") or null when unreadable. */
  nsPid: string | null;
  argv: readonly string[];
  /** Selection is proven by the bound socket's fd owner. */
  basis: "socket-owner";
  socketPath: string;
  socketInode: string;
}>;

/** A pinned process: pid + start time (+ argv/ppid when readable). */
export type ProcPin = Readonly<{
  pid: number;
  startTicks: string;
  ppid: number | null;
  argv: readonly string[] | null;
  nsPid: string | null;
}>;

export type DaemonSnapshot = Readonly<{
  identity: DaemonIdentity;
  /** Every live child of the daemon (ppid == daemon pid), pinned by pid + start ticks. */
  children: readonly ProcPin[];
  /** Children in a native gptqueue-session launch position with valid Codex options. */
  sidecars: readonly ProcPin[];
}>;

const isMissing = (error: unknown): boolean => (error as NodeJS.ErrnoException).code === "ENOENT";
const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error));

export const basename = (path: string): string => {
  const parts = path.split("/");
  return parts[parts.length - 1] ?? path;
};

/** argv tokens of a process, or null when the process is gone. Throws when present but unreadable. */
export const readProcessArgv = async (procRoot: string, pid: number, signal?: AbortSignal): Promise<string[] | null> => {
  signal?.throwIfAborted();
  let raw: Buffer;
  try {
    raw = await readFile(join(procRoot, String(pid), "cmdline"), { signal });
  } catch (error) {
    if (isMissing(error)) return null;
    throw error;
  }
  if (raw.length > 0 && raw[raw.length - 1] !== 0) throw new Error(`incomplete process ${pid} argv`);
  const tokens = raw.toString("utf8").split("\0");
  while (tokens.length > 0 && tokens[tokens.length - 1] === "") tokens.pop();
  return tokens;
};

/** ppid and start ticks from /proc/<pid>/stat, or null when the process is gone. */
export const readProcessStat = async (
  procRoot: string,
  pid: number,
  signal?: AbortSignal,
): Promise<Readonly<{ ppid: number; startTicks: string }> | null> => {
  signal?.throwIfAborted();
  let raw: string;
  try {
    raw = (await readFile(join(procRoot, String(pid), "stat"), { signal })).toString("utf8");
  } catch (error) {
    if (isMissing(error)) return null;
    throw error;
  }
  // comm may contain spaces and parens; fields restart after the last ')'.
  const close = raw.lastIndexOf(")");
  const fields = close < 0 ? [] : raw.slice(close + 2).trim().split(/\s+/);
  const ppid = Number(fields[1]);
  const startTicks = fields[19];
  if (close < 0 || !Number.isInteger(ppid) || ppid < 0 || startTicks === undefined || !/^\d+$/.test(startTicks)) {
    throw new Error(`unparsable ${procRoot}/${pid}/stat`);
  }
  return { ppid, startTicks };
};

export const readNsPid = async (procRoot: string, pid: number, signal?: AbortSignal): Promise<string | null> => {
  signal?.throwIfAborted();
  try {
    const ns = await readlink(join(procRoot, String(pid), "ns", "pid"));
    signal?.throwIfAborted();
    return ns;
  } catch {
    signal?.throwIfAborted();
    return null;
  }
};

/**
 * Pin a process (pid + start ticks + argv + ppid), or null when no process
 * with that pid exists. A present-but-unreadable process throws: "identity
 * unknown" must never be mistaken for "process gone".
 */
export const readProcessPin = async (procRoot: string, pid: number, signal?: AbortSignal): Promise<ProcPin | null> => {
  const stat = await readProcessStat(procRoot, pid, signal);
  if (stat === null) return null;
  const argv = await readProcessArgv(procRoot, pid, signal);
  if (argv === null) {
    if (await readProcessStat(procRoot, pid, signal) === null) return null;
    throw new Error(`process ${pid} has no readable cmdline`);
  }
  const after = await readProcessStat(procRoot, pid, signal);
  if (after === null) return null;
  if (after.startTicks !== stat.startTicks || after.ppid !== stat.ppid) throw new Error(`process ${pid} changed during pinning`);
  return { pid, startTicks: stat.startTicks, ppid: stat.ppid, argv, nsPid: await readNsPid(procRoot, pid, signal) };
};

const listPids = async (procRoot: string, signal?: AbortSignal): Promise<number[]> => {
  signal?.throwIfAborted();
  let names: string[];
  try {
    names = await readdir(procRoot);
    signal?.throwIfAborted();
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot enumerate ${procRoot}: ${messageOf(error)}`);
  }
  return names.filter((name) => /^\d+$/.test(name)).map(Number);
};

/** Resolve aliases, including a symlinked parent of a disappeared socket. */
const canonicalSocketPath = async (path: string, signal?: AbortSignal): Promise<string> => {
  signal?.throwIfAborted();
  try { const canonical = await realpath(path); signal?.throwIfAborted(); return canonical; }
  catch (error) {
    if (!isMissing(error)) throw error;
    const parent = dirname(path);
    if (parent === path) return resolve(path);
    return join(await canonicalSocketPath(parent, signal), basename(path));
  }
};

/**
 * Listening AF_UNIX stream inodes for `socketPath` in /proc/net/unix.
 * Accepted connections may carry the very same pathname; only SOCK_STREAM
 * with Linux __SO_ACCEPTCON (1 << 16, performed a listen) selects a listener.
 */
export const unixSocketInodes = async (procRoot: string, socketPath: string, signal?: AbortSignal): Promise<readonly string[]> => {
  let text: string;
  try {
    text = (await readFile(join(procRoot, "net", "unix"), { signal })).toString("utf8");
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot read ${procRoot}/net/unix: ${messageOf(error)}`);
  }
  const selected = await canonicalSocketPath(socketPath, signal);
  const inodes = new Set<string>();
  for (const line of text.split("\n").slice(1)) {
    // Num RefCount Protocol Flags Type St Inode Path. Preserve Path whitespace.
    const match = /^(?:\S+\s+){3}(\S+)\s+(\S+)\s+(\S+)\s+(\S+) (.*)$/.exec(line.trimStart());
    if (!match) {
      if ([socketPath, selected].some(path => line.endsWith(` ${path}`))) {
        throw new DiscoveryError("proc_unreadable", "malformed selected Unix socket row");
      }
      continue;
    }
    const boundPath = match[5]!;
    // The selected spelling was validated above. Other rows are only aliases
    // when successfully resolved; unrelated protected/stale paths are not
    // prerequisites for observing this socket.
    if (boundPath !== socketPath && boundPath !== selected) {
      try {
        if (await realpath(boundPath) !== selected) continue;
      } catch (error) {
        signal?.throwIfAborted();
        if (["ENOENT", "EACCES", "EPERM"].includes((error as NodeJS.ErrnoException).code ?? "")) continue;
        throw error;
      }
    }
    const [, flagsRaw, typeRaw, stateRaw, inode] = match;
    if (!/^[0-9a-fA-F]{1,8}$/.test(flagsRaw!) || !/^[0-9a-fA-F]{1,4}$/.test(typeRaw!) || !/^[0-9a-fA-F]{1,2}$/.test(stateRaw!)) {
      throw new DiscoveryError("proc_unreadable", "malformed selected Unix socket metadata");
    }
    const listening = (Number.parseInt(flagsRaw!, 16) & 0x10000) !== 0;
    if (Number.parseInt(typeRaw!, 16) !== 1 || !listening) continue;
    if (Number.parseInt(stateRaw!, 16) !== 1 || !/^[1-9]\d*$/.test(inode!)) {
      throw new DiscoveryError("proc_unreadable", "invalid selected Unix stream listener state or inode");
    }
    inodes.add(inode!);
  }
  return [...inodes];
};

/**
 * Admit only processes whose real/effective/saved/fs UIDs all match the
 * caller. Unknown credentials cannot be silently classified as foreign.
 */
const sameUidProcess = async (procRoot: string, pid: number, signal?: AbortSignal): Promise<boolean | null> => {
  let status: string;
  try { status = await readFile(join(procRoot, String(pid), "status"), { encoding: "utf8", signal }); }
  catch (error) {
    if (isMissing(error) && await readProcessStat(procRoot, pid, signal) === null) return null;
    throw error;
  }
  const fields = /^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$/m.exec(status)?.slice(1).map(Number);
  if (!fields || fields.some(uid => !Number.isSafeInteger(uid) || uid < 0)) throw new Error(`unreadable UID domain for process ${pid}`);
  const callerUid = process.getuid?.();
  if (callerUid === undefined) throw new Error("same-UID daemon discovery requires Linux UID evidence");
  if (fields.every(uid => uid === callerUid)) return true;
  if (fields.every(uid => uid !== callerUid)) return false;
  throw new Error(`ambiguous mixed UID domain for process ${pid}`);
};

/** Eligible same-UID Codex candidates positively holding the listening inode. */
const socketOwnerPids = async (procRoot: string, inode: string, signal?: AbortSignal): Promise<number[]> => {
  const wanted = `socket:[${inode}]`;
  const owners: number[] = [];
  for (const pid of await listPids(procRoot, signal)) {
    signal?.throwIfAborted();
    if (await sameUidProcess(procRoot, pid, signal) !== true) continue;
    // Classify the complete argv before touching descriptor tables of unrelated
    // same-user services. Unknown evidence cannot prove a noncandidate.
    const argv = await readProcessArgv(procRoot, pid, signal);
    if (argv === null) {
      if (await readProcessStat(procRoot, pid, signal) === null) continue;
      throw new Error(`cannot classify live process ${pid}: missing argv`);
    }
    if (argv.length === 0 || !argv[0]) throw new Error(`cannot classify process ${pid}: empty argv`);
    if (!isCodexAppServerArgv(argv)) continue;
    const fdDir = join(procRoot, String(pid), "fd");
    let names: string[];
    try {
      names = await readdir(fdDir);
    } catch (error) {
      if (isMissing(error) && await readProcessStat(procRoot, pid, signal) === null) continue;
      throw error;
    }
    for (const name of names) {
      signal?.throwIfAborted();
      try {
        if ((await readlink(join(fdDir, name))) === wanted) {
          owners.push(pid);
          break;
        }
      } catch (error) {
        if (!isMissing(error)) throw error;
      }
    }
  }
  return owners;
};

/** The endpoint named by --listen (the following argv token), or null. */
export const listenEndpoint = (argv: readonly string[]): string | null => {
  const index = argv.indexOf("--listen");
  if (index < 0 || index + 1 >= argv.length) return null;
  return argv[index + 1]!;
};

/**
 * Token-wise confirmation of a Codex app-server managed daemon: argv[0]'s
 * basename is `codex`, and `app-server`, `--managed-daemon` and a `unix://`
 * --listen endpoint each appear as their own token. Extra flags anywhere are
 * fine; substrings inside other tokens never match.
 */
export const isCodexAppServerArgv = (argv: readonly string[]): boolean => {
  if (argv.length === 0 || basename(argv[0]!) !== "codex") return false;
  if (!argv.includes("app-server") || !argv.includes("--managed-daemon")) return false;
  return listenEndpoint(argv)?.startsWith("unix://") === true;
};

/** argv names the exact selected endpoint: --listen unix://<socketPath> as two tokens. */
export const listensOnSocket = (argv: readonly string[], socketPath: string): boolean =>
  listenEndpoint(argv) === `unix://${socketPath}`;

/**
 * Token-wise confirmation of a gptqueue Codex sidecar. The
 * `gptqueue-session` script must sit in the executable position: argv[0]
 * itself (direct execution), or the script argument of the project's native
 * Node launch (`node <sidecar> --client codex ...` — the form bin/gptqueue-session
 * is spawned in by pi-extension.ts, the Codex/MCP registration and worker
 * mode). The selected command's full arguments must pass the same pure
 * option parser as native startup and select Codex. Tokens in an unrelated executable's
 * or script's arguments (python3 /tmp/other.py /opt/gptqueue/bin/gptqueue-session
 * --client codex, node /tmp/other.js ...) never qualify.
 */
export const isGptqueueSessionSidecarArgv = (argv: readonly string[]): boolean => {
  if (argv.length === 0) return false;
  const args = basename(argv[0]!) === "gptqueue-session" ? argv.slice(1)
    : basename(argv[0]!) === "node" && argv.length > 1 && basename(argv[1]!) === "gptqueue-session" ? argv.slice(2)
    : null;
  if (args === null) return false;
  try { return parseRegisteredShellOptions(args).client === "codex"; }
  catch { return false; }
};

/**
 * Resolve the daemon behind the selected control socket.
 *
 * Order of evidence: exact socket path in /proc/net/unix -> inode -> the one
 * process whose fd table holds it -> token-wise argv confirmation (the --listen
 * value may be a bare `unix://`; fd ownership already proves the endpoint).
 * Missing, ambiguous or unreadable ownership refuses; argv alone cannot
 * establish ownership of the selected bound endpoint.
 */
export const discoverCodexDaemon = async (
  options: Readonly<{ socketPath: string; procRoot?: string; signal?: AbortSignal }>,
): Promise<DaemonIdentity> => {
  const procRoot = options.procRoot ?? "/proc";
  const signal = options.signal;
  signal?.throwIfAborted();
  const socketPath = options.socketPath;
  const inodes = await unixSocketInodes(procRoot, socketPath, signal);
  if (inodes.length === 0) {
    throw new DiscoveryError(
      "socket_not_listed",
      `${socketPath} is not a bound unix socket in ${procRoot}/net/unix (no daemon is listening on it)`,
      { socketPath },
    );
  }
  if (inodes.length > 1) {
    throw new DiscoveryError("ambiguous_socket", `${socketPath} is bound by ${inodes.length} sockets (inodes ${inodes.join(", ")})`, { socketPath, inodes });
  }
  const inode = inodes[0]!;
  let owners: number[];
  try {
    owners = await socketOwnerPids(procRoot, inode, signal);
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot scan ${procRoot} fd tables: ${messageOf(error)}`);
  }
  if (owners.length > 1) {
    throw new DiscoveryError("ambiguous_owner", `socket ${socketPath} (inode ${inode}) is held by ${owners.length} processes: ${owners.join(", ")}`, { socketPath, inode, pids: owners });
  }
  let identity: DaemonIdentity | null = null;
  if (owners.length === 1) {
    const pid = owners[0]!;
    let argv: string[] | null;
    try {
      argv = await readProcessArgv(procRoot, pid, signal);
    } catch (error) {
      throw new DiscoveryError("owner_argv_unreadable", `cannot read ${procRoot}/${pid}/cmdline: ${messageOf(error)}`, { pid });
    }
    let stat: Readonly<{ ppid: number; startTicks: string }> | null = null;
    if (argv !== null) {
      try {
        stat = await readProcessStat(procRoot, pid, signal);
      } catch (error) {
        throw new DiscoveryError("proc_unreadable", `cannot read ${procRoot}/${pid}/stat: ${messageOf(error)}`, { pid });
      }
    }
    if (argv !== null && stat !== null) {
      if (!isCodexAppServerArgv(argv)) {
        throw new DiscoveryError(
          "owner_not_codex_app_server",
          `socket ${socketPath} is owned by pid ${pid} (${argv[0] ?? "?"}), not a codex app-server --managed-daemon`,
          { pid, argv0: argv[0] ?? null },
        );
      }
      identity = { pid, startTicks: stat.startTicks, nsPid: await readNsPid(procRoot, pid, signal), argv, basis: "socket-owner", socketPath, socketInode: inode };
    }
    // A vanished owner is incomplete evidence and refuses below.
  }
  if (identity) return identity;
  throw new DiscoveryError("no_owner", `no readable owner proves the selected socket ${socketPath}`, { socketPath });
};

/**
 * Fail loudly when the pinned daemon pid no longer names the same process
 * (gone, or start ticks changed = pid reuse). Every verification step calls
 * this so observations made about "the daemon" stay about one process.
 */
export const assertDaemonIdentity = async (
  identity: DaemonIdentity,
  options: Readonly<{ procRoot?: string; signal?: AbortSignal }> = {},
): Promise<void> => {
  const procRoot = options.procRoot ?? "/proc";
  const signal = options.signal;
  signal?.throwIfAborted();
  let stat: Readonly<{ ppid: number; startTicks: string }> | null;
  try {
    stat = await readProcessStat(procRoot, identity.pid, signal);
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot re-read ${procRoot}/${identity.pid}/stat: ${messageOf(error)}`, { pid: identity.pid });
  }
  if (stat === null) {
    throw new DiscoveryError("daemon_gone", `daemon pid ${identity.pid} no longer exists`, { pid: identity.pid });
  }
  if (stat.startTicks !== identity.startTicks) {
    throw new DiscoveryError("daemon_replaced", `daemon pid ${identity.pid} now names a different process (start ticks ${identity.startTicks} -> ${stat.startTicks})`, { pid: identity.pid, before: identity.startTicks, after: stat.startTicks });
  }
  const current = await discoverCodexDaemon({ socketPath: identity.socketPath, procRoot, signal });
  if (current.pid !== identity.pid || current.startTicks !== identity.startTicks ||
      current.socketInode !== identity.socketInode ||
      (identity.nsPid !== null && current.nsPid !== identity.nsPid)) {
    throw new DiscoveryError("daemon_replaced", "selected endpoint owner, inode or namespace changed", { pid: identity.pid });
  }
};

/**
 * Snapshot the pinned daemon's children and gptqueue Codex sidecars
 * (token-wise argv classification). ENOENT races may skip a vanished pid;
 * inaccessible/malformed stat or unreadable known-child argv refuses.
 */
export const snapshotDaemon = async (
  identity: DaemonIdentity,
  options: Readonly<{ procRoot?: string; signal?: AbortSignal }> = {},
): Promise<DaemonSnapshot> => {
  const procRoot = options.procRoot ?? "/proc";
  const signal = options.signal;
  signal?.throwIfAborted();
  await assertDaemonIdentity(identity, { procRoot, signal });
  const children: ProcPin[] = [];
  const sidecars: ProcPin[] = [];
  for (const pid of await listPids(procRoot, signal)) {
    signal?.throwIfAborted();
    if (pid === identity.pid) continue;
    let stat: Readonly<{ ppid: number; startTicks: string }> | null;
    try {
      stat = await readProcessStat(procRoot, pid, signal);
    } catch (error) {
      throw new DiscoveryError("proc_unreadable", `cannot classify process ${pid}: ${messageOf(error)}`);
    }
    if (stat === null || stat.ppid !== identity.pid) continue;
    let argv: string[] | null;
    try {
      argv = await readProcessArgv(procRoot, pid, signal);
    } catch (error) {
      throw new DiscoveryError("proc_unreadable", `cannot classify daemon child ${pid}: ${messageOf(error)}`);
    }
    if (argv === null || argv.length === 0) {
      if (await readProcessStat(procRoot, pid, signal) === null) continue;
      throw new DiscoveryError("proc_unreadable", `daemon child ${pid} has no readable cmdline`);
    }
    const child: ProcPin = { pid, startTicks: stat.startTicks, ppid: stat.ppid, argv, nsPid: null };
    children.push(child);
    if (argv !== null && isGptqueueSessionSidecarArgv(argv)) sidecars.push(child);
  }
  await assertDaemonIdentity(identity, { procRoot, signal });
  return { identity, children, sidecars };
};
