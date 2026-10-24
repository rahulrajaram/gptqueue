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
 * selects the daemon. The argv-scan fallback is used only when owner
 * resolution finds no owner at all, and it must prove the exact endpoint
 * correlation itself (--listen unix://<selected path>); ambiguity or absence
 * is a loud refusal, never an arbitrary pick. There is deliberately no
 * fallback from ambiguity: a socket held by two pids or an owner that is not
 * a Codex app-server is global evidence of a broken environment, not a hint
 * to guess.
 */
import { readdir, readFile, readlink } from "node:fs/promises";
import { join } from "node:path";

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
  /** How selection was proven: the socket's fd owner, or an argv naming the exact endpoint. */
  basis: "socket-owner" | "argv-scan";
  socketPath: string;
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
  /** Children whose argv token-wise names gptqueue-session --client codex. */
  sidecars: readonly ProcPin[];
}>;

const isMissing = (error: unknown): boolean => (error as NodeJS.ErrnoException).code === "ENOENT";
const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error));

export const basename = (path: string): string => {
  const parts = path.split("/");
  return parts[parts.length - 1] ?? path;
};

/** argv tokens of a process, or null when the process is gone. Throws when present but unreadable. */
export const readProcessArgv = async (procRoot: string, pid: number): Promise<string[] | null> => {
  let raw: Buffer;
  try {
    raw = await readFile(join(procRoot, String(pid), "cmdline"));
  } catch (error) {
    if (isMissing(error)) return null;
    throw error;
  }
  const tokens = raw.toString("utf8").split("\0");
  while (tokens.length > 0 && tokens[tokens.length - 1] === "") tokens.pop();
  return tokens;
};

/** ppid and start ticks from /proc/<pid>/stat, or null when the process is gone. */
export const readProcessStat = async (
  procRoot: string,
  pid: number,
): Promise<Readonly<{ ppid: number; startTicks: string }> | null> => {
  let raw: string;
  try {
    raw = (await readFile(join(procRoot, String(pid), "stat"))).toString("utf8");
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

export const readNsPid = async (procRoot: string, pid: number): Promise<string | null> => {
  try {
    return await readlink(join(procRoot, String(pid), "ns", "pid"));
  } catch {
    return null;
  }
};

/**
 * Pin a process (pid + start ticks + argv + ppid), or null when no process
 * with that pid exists. A present-but-unreadable process throws: "identity
 * unknown" must never be mistaken for "process gone".
 */
export const readProcessPin = async (procRoot: string, pid: number): Promise<ProcPin | null> => {
  const stat = await readProcessStat(procRoot, pid);
  const argv = await readProcessArgv(procRoot, pid);
  if (stat === null || argv === null) return null;
  return { pid, startTicks: stat.startTicks, ppid: stat.ppid, argv, nsPid: await readNsPid(procRoot, pid) };
};

const listPids = async (procRoot: string): Promise<number[]> => {
  let names: string[];
  try {
    names = await readdir(procRoot);
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot enumerate ${procRoot}: ${messageOf(error)}`);
  }
  return names.filter((name) => /^\d+$/.test(name)).map(Number);
};

/**
 * The socketfs inodes bound to exactly `socketPath` in /proc/net/unix
 * (bound/listening sockets carry their path; connected client sockets do not).
 */
export const unixSocketInodes = async (procRoot: string, socketPath: string): Promise<readonly string[]> => {
  let text: string;
  try {
    text = (await readFile(join(procRoot, "net", "unix"))).toString("utf8");
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot read ${procRoot}/net/unix: ${messageOf(error)}`);
  }
  const inodes = new Set<string>();
  for (const line of text.split("\n").slice(1)) {
    const fields = line.trim().split(/\s+/);
    if (fields.length < 8) continue;
    if (fields.slice(7).join(" ") !== socketPath) continue;
    const inode = fields[6]!;
    if (/^\d+$/.test(inode)) inodes.add(inode);
  }
  return [...inodes];
};

/** Pids holding an fd to socket:[<inode>]. Unreadable fd tables are skipped per pid, never fatal here. */
const socketOwnerPids = async (procRoot: string, inode: string): Promise<number[]> => {
  const wanted = `socket:[${inode}]`;
  const owners: number[] = [];
  for (const pid of await listPids(procRoot)) {
    const fdDir = join(procRoot, String(pid), "fd");
    let names: string[];
    try {
      names = await readdir(fdDir);
    } catch {
      continue; // vanished, or not ours to read: absence of evidence, not proof
    }
    for (const name of names) {
      try {
        if ((await readlink(join(fdDir, name))) === wanted) {
          owners.push(pid);
          break;
        }
      } catch {
        /* fd replaced mid-scan */
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
 * Token-wise confirmation of a gptqueue Codex sidecar: some token's basename
 * is `gptqueue-session` (argv[0] itself, or the script after the interpreter)
 * and `--client codex` appears as two adjacent tokens.
 */
export const isGptqueueSessionSidecarArgv = (argv: readonly string[]): boolean => {
  if (!argv.some((token) => basename(token) === "gptqueue-session")) return false;
  const index = argv.indexOf("--client");
  return index >= 0 && argv[index + 1] === "codex";
};

/**
 * Resolve the daemon behind the selected control socket.
 *
 * Order of evidence: exact socket path in /proc/net/unix -> inode -> the one
 * process whose fd table holds it -> token-wise argv confirmation (the --listen
 * value may be a bare `unix://`; fd ownership already proves the endpoint).
 * Only when no owner can be found at all does an argv scan run, and it must
 * name the exact `unix://<socketPath>` endpoint; zero or several matches are
 * loud refusals. Ambiguous or non-Codex owners are refused outright.
 */
export const discoverCodexDaemon = async (
  options: Readonly<{ socketPath: string; procRoot?: string }>,
): Promise<DaemonIdentity> => {
  const procRoot = options.procRoot ?? "/proc";
  const socketPath = options.socketPath;
  const inodes = await unixSocketInodes(procRoot, socketPath);
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
    owners = await socketOwnerPids(procRoot, inode);
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
      argv = await readProcessArgv(procRoot, pid);
    } catch (error) {
      throw new DiscoveryError("owner_argv_unreadable", `cannot read ${procRoot}/${pid}/cmdline: ${messageOf(error)}`, { pid });
    }
    let stat: Readonly<{ ppid: number; startTicks: string }> | null = null;
    if (argv !== null) {
      try {
        stat = await readProcessStat(procRoot, pid);
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
      identity = { pid, startTicks: stat.startTicks, nsPid: await readNsPid(procRoot, pid), argv, basis: "socket-owner", socketPath };
    }
    // argv or stat vanished between the fd scan and the read: fall through to
    // the argv scan below rather than reporting a dead owner as selection.
  }
  if (identity) return identity;
  // Fallback (owner resolution found nothing): an argv scan may select the
  // daemon ONLY by naming this exact endpoint. A bare `unix://` listen value
  // cannot be correlated with the selected path, so it is not a candidate.
  const candidates: number[] = [];
  for (const pid of await listPids(procRoot)) {
    let argv: string[] | null;
    try {
      argv = await readProcessArgv(procRoot, pid);
    } catch {
      continue; // unreadable argv cannot prove anything about this pid
    }
    if (argv === null) continue;
    if (!isCodexAppServerArgv(argv) || !listensOnSocket(argv, socketPath)) continue;
    try {
      if ((await readProcessStat(procRoot, pid)) === null) continue;
    } catch {
      continue;
    }
    candidates.push(pid);
  }
  if (candidates.length === 0) {
    throw new DiscoveryError(
      "daemon_not_found",
      `no codex app-server --managed-daemon is provably listening on ${socketPath}: the socket inode has no owner and no argv names this exact endpoint`,
      { socketPath },
    );
  }
  if (candidates.length > 1) {
    throw new DiscoveryError("ambiguous_daemon", `${candidates.length} codex app-server daemons name ${socketPath}: pids ${candidates.join(", ")}`, { socketPath, pids: candidates });
  }
  const pid = candidates[0]!;
  let stat: Readonly<{ ppid: number; startTicks: string }>;
  let argv: string[];
  try {
    argv = (await readProcessArgv(procRoot, pid))!;
    stat = (await readProcessStat(procRoot, pid))!;
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot pin daemon pid ${pid}: ${messageOf(error)}`, { pid });
  }
  return { pid, startTicks: stat.startTicks, nsPid: await readNsPid(procRoot, pid), argv, basis: "argv-scan", socketPath };
};

/**
 * Fail loudly when the pinned daemon pid no longer names the same process
 * (gone, or start ticks changed = pid reuse). Every verification step calls
 * this so observations made about "the daemon" stay about one process.
 */
export const assertDaemonIdentity = async (
  identity: DaemonIdentity,
  options: Readonly<{ procRoot?: string }> = {},
): Promise<void> => {
  const procRoot = options.procRoot ?? "/proc";
  let stat: Readonly<{ ppid: number; startTicks: string }> | null;
  try {
    stat = await readProcessStat(procRoot, identity.pid);
  } catch (error) {
    throw new DiscoveryError("proc_unreadable", `cannot re-read ${procRoot}/${identity.pid}/stat: ${messageOf(error)}`, { pid: identity.pid });
  }
  if (stat === null) {
    throw new DiscoveryError("daemon_gone", `daemon pid ${identity.pid} no longer exists`, { pid: identity.pid });
  }
  if (stat.startTicks !== identity.startTicks) {
    throw new DiscoveryError("daemon_replaced", `daemon pid ${identity.pid} now names a different process (start ticks ${identity.startTicks} -> ${stat.startTicks})`, { pid: identity.pid, before: identity.startTicks, after: stat.startTicks });
  }
};

/**
 * Snapshot the pinned daemon's children and gptqueue Codex sidecars
 * (token-wise argv classification). Per-pid read failures of a vanished or
 * racy child skip that pid; identity is re-verified first.
 */
export const snapshotDaemon = async (
  identity: DaemonIdentity,
  options: Readonly<{ procRoot?: string }> = {},
): Promise<DaemonSnapshot> => {
  const procRoot = options.procRoot ?? "/proc";
  await assertDaemonIdentity(identity, { procRoot });
  const children: ProcPin[] = [];
  const sidecars: ProcPin[] = [];
  for (const pid of await listPids(procRoot)) {
    if (pid === identity.pid) continue;
    let stat: Readonly<{ ppid: number; startTicks: string }> | null;
    try {
      stat = await readProcessStat(procRoot, pid);
    } catch {
      continue; // unreadable stat for one pid cannot prove a parent relation
    }
    if (stat === null || stat.ppid !== identity.pid) continue;
    let argv: string[] | null;
    try {
      argv = await readProcessArgv(procRoot, pid);
    } catch {
      argv = null; // still a child; classified as a sidecar only when argv proves it
    }
    const child: ProcPin = { pid, startTicks: stat.startTicks, ppid: stat.ppid, argv, nsPid: null };
    children.push(child);
    if (argv !== null && isGptqueueSessionSidecarArgv(argv)) sidecars.push(child);
  }
  return { identity, children, sidecars };
};
