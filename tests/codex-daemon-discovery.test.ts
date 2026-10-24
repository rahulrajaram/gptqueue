/**
 * Discovery tests for registered-shell/codex-daemon.ts over synthetic /proc
 * fixture trees (net/unix + per-pid cmdline/stat/fd), plus one real-process
 * integration case. Covers the f2269 field shape (extra --analytics-default-enabled
 * anywhere, bare `unix://` listen value), substring decoys, ambiguity and
 * read-denial refusals, refusal of argv-only ownership, and PID-reuse
 * pinning. No global machine-state assertions: every case is scoped to the
 * fixture tree (or the spawned fixture process).
 */
import { describe, expect, it, afterAll, afterEach, vi } from "vitest";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { createConnection, type Socket } from "node:net";
import {
  DiscoveryError,
  assertDaemonIdentity,
  discoverCodexDaemon,
  isCodexAppServerArgv,
  isGptqueueSessionSidecarArgv,
  listensOnSocket,
  readProcessPin,
  snapshotDaemon,
} from "../src/registered-shell/codex-daemon.js";
import { defaultCodexSocketPath } from "../src/registered-shell/codex-socket.js";

// Deterministic access-denial fixtures; no host UID, Docker, or chmod assumptions.
const deniedPaths = vi.hoisted(() => new Map<string, string>());
const deniedLinks = vi.hoisted(() => new Set<string>());
vi.mock("node:fs/promises", async importOriginal => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return { ...actual, realpath: async (path: string, ...args: unknown[]) => {
    const code = deniedPaths.get(String(path));
    if (code) throw Object.assign(new Error("fixture path access denied"), { code });
    return actual.realpath(path, ...args as []);
  }, readlink: async (path: string, ...args: unknown[]) => {
    if (deniedLinks.has(String(path))) throw Object.assign(new Error("fixture fd access denied"), { code: "EACCES" });
    return actual.readlink(path, ...args as []);
  } };
});
afterEach(() => { deniedPaths.clear(); deniedLinks.clear(); });

type ProcSpec = Readonly<{
  argv?: readonly string[];
  /** Synthetic credentials; all four fields match the caller unless overridden. */
  uids?: readonly [number, number, number, number];
  statusUnreadable?: boolean;
  /** Make cmdline a directory: present but unreadable (readFile fails with EISDIR). */
  cmdlineUnreadable?: boolean;
  /** Make fd a regular file: the fd table cannot be enumerated. */
  fdUnreadable?: boolean;
  ppid?: number;
  startTicks?: string;
  /** fd symlink targets (e.g. ["socket:[42]"]). */
  fds?: readonly string[];
}>;

type TreeSpec = Readonly<{
  sockets: ReadonlyArray<Readonly<{ path: string; inode: number; flags?: string; type?: string; state?: string }>>;
  procs: Readonly<Record<number, ProcSpec>>;
}>;

const statLine = (pid: number, ppid: number, startTicks: string): string =>
  // Fields after "(comm)": [0]=state [1]=ppid ... [19]=starttime.
  `${pid} (proc) S ${ppid} ${"0 ".repeat(17)}${startTicks} 0 0 0`;

const tempRoots: string[] = [];

const makeProcTree = async (spec: TreeSpec): Promise<{ root: string; socketPath: () => string }> => {
  const root = await mkdtemp(join(tmpdir(), "gptq-proc-"));
  tempRoots.push(root);
  const socketPath = spec.sockets[0]?.path ?? "/unused.sock";
  await mkdir(join(root, "net"), { recursive: true });
  const lines = ["Num       RefCount Protocol Flags    Type St Inode Path"];
  for (const socket of spec.sockets) {
    lines.push(`0000000000000000: 00000002 00000000 ${socket.flags ?? "00010000"} ${socket.type ?? "0001"} ${socket.state ?? "01"} ${socket.inode} ${socket.path}`);
  }
  // Noise entries: connected client sockets (no path) and unrelated paths.
  lines.push("0000000000000000: 00000002 00000000 00000000 0001 03 999999");
  lines.push(`0000000000000000: 00000002 00000000 00010000 0001 01 999998 /unrelated/other.sock`);
  await writeFile(join(root, "net", "unix"), `${lines.join("\n")}\n`);
  for (const [pid, proc] of Object.entries(spec.procs)) {
    const dir = join(root, pid);
    await mkdir(dir, { recursive: true });
    const uid = process.getuid!();
    if (proc.statusUnreadable) await mkdir(join(dir, "status"));
    else await writeFile(join(dir, "status"), `Name:\tfixture\nUid:\t${(proc.uids ?? [uid, uid, uid, uid]).join("\t")}\n`);
    if (proc.cmdlineUnreadable) await mkdir(join(dir, "cmdline"));
    else if (proc.argv) await writeFile(join(dir, "cmdline"), `${proc.argv.join("\0")}\0`);
    await writeFile(join(dir, "stat"), statLine(Number(pid), proc.ppid ?? 0, proc.startTicks ?? "100"));
    if (proc.fdUnreadable) await writeFile(join(dir, "fd"), "not a directory");
    else {
      await mkdir(join(dir, "fd"), { recursive: true });
      let fd = 3;
      for (const target of proc.fds ?? []) await symlink(target, join(dir, "fd", String(fd++)));
    }
  }
  return { root, socketPath: () => socketPath };
};

const daemonArgv = (socketPath: string, extra: readonly string[] = []): string[] => [
  "/usr/local/bin/codex", "app-server", ...extra, "--listen", `unix://${socketPath}`, "--managed-daemon",
];

const discoveryError = async (promise: Promise<unknown>): Promise<DiscoveryError> => {
  try {
    await promise;
  } catch (error) {
    if (error instanceof DiscoveryError) return error;
    throw error;
  }
  throw new Error("expected a DiscoveryError");
};

describe("codex daemon discovery (synthetic /proc)", () => {
  it("resolves the daemon by socket ownership and token-wise argv", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 4242 }],
      procs: {
        4242: { argv: daemonArgv("/run/codex.sock"), ppid: 1, startTicks: "777", fds: ["socket:[4242]", "/dev/null"] },
        5: { argv: ["systemd"], ppid: 0, fds: [] },
      },
    });
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    expect(identity).toMatchObject({ pid: 4242, basis: "socket-owner", startTicks: "777", socketPath: "/run/codex.sock" });
  });

  it("f2269 regression: an extra --analytics-default-enabled flag anywhere still matches", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock", ["--analytics-default-enabled"]), fds: ["socket:[10]"] },
      },
    });
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    expect(identity).toMatchObject({ pid: 11, basis: "socket-owner" });
  });

  it("f2269 regression: a bare unix:// listen value still matches when fd ownership proves the socket", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/srv/codex/home/app-server-control/app-server-control.sock", inode: 10 }],
      procs: {
        11: { argv: ["/usr/local/bin/codex", "app-server", "--listen", "unix://", "--managed-daemon", "--analytics-default-enabled"], fds: ["socket:[10]"] },
      },
    });
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    expect(identity).toMatchObject({ pid: 11, basis: "socket-owner" });
  });

  it("refuses a substring decoy: the pattern inside one argv token is not a token match", async () => {
    const decoy = ["sh", "-c", "codex app-server --listen unix:///run/codex.sock --managed-daemon"];
    expect(isCodexAppServerArgv(decoy)).toBe(false);
    // A noncandidate cannot supply eligible daemon ownership...
    const owned = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: decoy, fds: ["socket:[10]"] } },
    });
    const error = await discoveryError(discoverCodexDaemon({ socketPath: owned.socketPath(), procRoot: owned.root }));
    expect(error.mode).toBe("no_owner");
    // ...and in an argv scan it never matches, so the daemon stays not-found.
    const unowned = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: decoy } },
    });
    const notFound = await discoveryError(discoverCodexDaemon({ socketPath: unowned.socketPath(), procRoot: unowned.root }));
    expect(notFound.mode).toBe("no_owner");
  });

  it("token predicates: extra flags are fine, wrong tokens are not", () => {
    expect(isCodexAppServerArgv(daemonArgv("/x.sock", ["--flag", "--another", "value"]))).toBe(true);
    expect(isCodexAppServerArgv(["codex", "app-server", "--listen", "tcp://127.0.0.1:1", "--managed-daemon"])).toBe(false);
    expect(isCodexAppServerArgv(["codex", "exec", "--listen", "unix:///x", "--managed-daemon"])).toBe(false);
    expect(isCodexAppServerArgv(["node", "/codex", "app-server", "--listen", "unix:///x", "--managed-daemon"])).toBe(false);
    expect(listensOnSocket(daemonArgv("/x.sock"), "/x.sock")).toBe(true);
    expect(listensOnSocket(daemonArgv("/x.sock"), "/other.sock")).toBe(false);
    expect(listensOnSocket(["codex", "app-server", "--listen", "unix://", "--managed-daemon"], "/x.sock")).toBe(false);
  });

  it("sidecar classification is positional: executable or native node script position only", () => {
    // Positive real forms: direct executable and the project's native Node launch.
    const sidecar = ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis:///x.sock"];
    expect(isGptqueueSessionSidecarArgv(sidecar)).toBe(true);
    expect(isGptqueueSessionSidecarArgv(["/opt/gptqueue/bin/gptqueue-session", "--client", "codex"])).toBe(true);
    expect(isGptqueueSessionSidecarArgv(["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "pi"])).toBe(false);
    expect(isGptqueueSessionSidecarArgv(["sh", "-c", "gptqueue-session --client codex"])).toBe(false);
    // P004-SOL-FINAL-1: an unrelated executable or script carrying the sidecar
    // filename and --client codex as separate argument tokens never qualifies.
    expect(isGptqueueSessionSidecarArgv(["python3", "/tmp/other.py", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex"])).toBe(false);
    expect(isGptqueueSessionSidecarArgv(["node", "/tmp/other.js", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex"])).toBe(false);
    // --client must come from the selected command's own arguments, not another
    // command's tokens (the interpreter's own flags, or a script's arguments).
    expect(isGptqueueSessionSidecarArgv(["node", "/opt/gptqueue/bin/gptqueue-session", "--client"])).toBe(false);
  });

  it("refuses ambiguity loudly: two owners, two same-path inodes, unproven argv candidates", async () => {
    const twoOwners = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock"), fds: ["socket:[10]"] },
        12: { argv: daemonArgv("/run/codex.sock"), fds: ["socket:[10]"] },
      },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: twoOwners.socketPath(), procRoot: twoOwners.root }))).mode).toBe("ambiguous_owner");

    const twoInodes = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }, { path: "/run/codex.sock", inode: 20 }],
      procs: {},
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: twoInodes.socketPath(), procRoot: twoInodes.root }))).mode).toBe("ambiguous_socket");

    const twoScans = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock") },
        12: { argv: daemonArgv("/run/codex.sock") },
      },
    });
    const ambiguous = await discoveryError(discoverCodexDaemon({ socketPath: twoScans.socketPath(), procRoot: twoScans.root }));
    expect(ambiguous.mode).toBe("no_owner");
  });

  it("refuses without positive eligible-candidate fd ownership, even with matching argv elsewhere", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: ["python3", "-m", "http.server"], fds: ["socket:[10]"] },
        12: { argv: daemonArgv("/run/codex.sock") },
      },
    });
    const error = await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }));
    expect(error.mode).toBe("no_owner");
    expect(error.message).toContain("no readable owner");
  });

  it("refuses an unreadable owner argv (read denial), never reports zero daemons", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { cmdlineUnreadable: true, fds: ["socket:[10]"] } },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }))).mode).toBe("proc_unreadable");
  });

  it("missing socket and unreadable /proc/net/unix are loud environment refusals", async () => {
    const { root } = await makeProcTree({
      sockets: [{ path: "/run/other.sock", inode: 10 }],
      procs: { 11: { argv: daemonArgv("/run/other.sock"), fds: ["socket:[10]"] } },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/run/codex.sock", procRoot: root }))).mode).toBe("socket_not_listed");

    const rootless = await mkdtemp(join(tmpdir(), "gptq-proc-empty-"));
    tempRoots.push(rootless);
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/run/codex.sock", procRoot: rootless }))).mode).toBe("proc_unreadable");
  });

  it("argv alone never establishes selected-socket ownership, including exact endpoint argv", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock"), fds: ["socket:[999997]"] }, // owns some other socket
      },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }))).mode).toBe("no_owner");

    const bare = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: ["codex", "app-server", "--listen", "unix://", "--managed-daemon"] } },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: bare.socketPath(), procRoot: bare.root }))).mode).toBe("no_owner");
  });

  it("an eligible Codex candidate's unreadable fd table refuses incomplete candidate evidence", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock"), fdUnreadable: true }, // the true owner, unreadable fds
        12: { argv: ["bash"], fds: [] },
      },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }))).mode).toBe("proc_unreadable");
  });

  it("snapshots children and sidecars scoped to the daemon, and refuses a replaced daemon pid", async () => {
    const spec: TreeSpec = {
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        4242: { argv: daemonArgv("/run/codex.sock"), ppid: 1, startTicks: "777", fds: ["socket:[10]"] },
        100: { argv: ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis://x"], ppid: 4242, startTicks: "800" },
        101: { argv: ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "pi"], ppid: 4242, startTicks: "801" },
        102: { argv: ["codex", "exec", "--sandbox"], ppid: 4242, startTicks: "802" },
        103: { argv: ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex"], ppid: 999, startTicks: "803" },
        104: { argv: ["python3", "/tmp/other.py", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex"], ppid: 4242, startTicks: "804" },
        105: { argv: ["node", "/tmp/other.js", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex"], ppid: 4242, startTicks: "805" },
      },
    };
    const { root, socketPath } = await makeProcTree(spec);
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    const snapshot = await snapshotDaemon(identity, { procRoot: root });
    expect(snapshot.children.map((child) => child.pid).sort()).toEqual([100, 101, 102, 104, 105]);
    expect(snapshot.sidecars.map((sidecar) => sidecar.pid)).toEqual([100]);

    // PID reuse: the daemon pid now names a different process (start ticks moved).
    await writeFile(join(root, "4242", "stat"), statLine(4242, 1, "999"));
    const replaced = await discoveryError(snapshotDaemon(identity, { procRoot: root }));
    expect(replaced.mode).toBe("daemon_replaced");

    // Daemon gone entirely.
    await rm(join(root, "4242"), { recursive: true, force: true });
    const gone = await discoveryError(assertDaemonIdentity(identity, { procRoot: root }));
    expect(gone.mode).toBe("daemon_gone");
  });

  it("readProcessPin: gone is null; identity carries start ticks and ppid (PID-reuse pinning)", async () => {
    const { root } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: ["sleep", "1000"], ppid: 4242, startTicks: "555" } },
    });
    expect(await readProcessPin(root, 11)).toMatchObject({ pid: 11, startTicks: "555", ppid: 4242, argv: ["sleep", "1000"] });
    expect(await readProcessPin(root, 99999)).toBeNull();
  });
});

describe("P004 daemon safety regressions", () => {
  it.each(["default", "explicit"])("R3 canonicalizes %s CODEX_HOME/socket aliases", async mode => {
    const home = await mkdtemp(join(tmpdir(), "gptq-home-")); tempRoots.push(home);
    const real = join(home, "real  home"); const alias = join(home, "alias");
    await mkdir(join(real, "app-server-control"), { recursive: true }); await symlink(real, alias);
    const socket = join(real, "app-server-control", "app-server-control.sock"); await writeFile(socket, "fixture");
    const { root } = await makeProcTree({ sockets: [{ path: socket, inode: 10 }], procs: { 11: { argv: daemonArgv(socket), fds: ["socket:[10]"] } } });
    vi.stubEnv("CODEX_HOME", alias); vi.stubEnv("GPTQUEUE_CODEX_APP_SERVER_SOCKET", "");
    try {
      const selected = mode === "default" ? defaultCodexSocketPath() : join(alias, "app-server-control", "app-server-control.sock");
      expect((await discoverCodexDaemon({ socketPath: selected, procRoot: root })).pid).toBe(11);
      await writeFile(join(root, "net", "unix"), `header\n0: 2 0 10000 1 01 10 ${socket}\n0: 2 0 10000 1 01 20 ${selected}\n`);
      expect((await discoveryError(discoverCodexDaemon({ socketPath: selected, procRoot: root }))).mode).toBe("ambiguous_socket");
    } finally { vi.unstubAllEnvs(); }
  });

  it("one stream listener remains unique beside accepted connections sharing its path and alias", async () => {
    const dir = await mkdtemp(join(tmpdir(), "gptq-listener-")); tempRoots.push(dir);
    const actual = join(dir, "actual.sock"), alias = join(dir, "alias.sock");
    await writeFile(actual, "fixture"); await symlink(actual, alias);
    const { root } = await makeProcTree({ sockets: [
      { path: actual, inode: 10 },
      { path: actual, inode: 20, flags: "00000000", state: "03" },
      { path: alias, inode: 21, flags: "00000000", state: "03" },
      { path: actual, inode: 22, flags: "00000000", state: "03" },
      { path: alias, inode: 30, flags: "00000000", type: "0002" },
      { path: actual, inode: 31, type: "0005" },
    ], procs: { 11: { argv: daemonArgv(actual), fds: ["socket:[10]", "socket:[20]", "socket:[21]"] } } });
    const pin = await discoverCodexDaemon({ socketPath: alias, procRoot: root });
    expect(pin).toMatchObject({ pid: 11, socketInode: "10" });
    await expect(assertDaemonIdentity(pin, { procRoot: root })).resolves.toBeUndefined();
    // A second genuine listener through the alias remains ambiguous.
    await writeFile(join(root, "net", "unix"), `header\n0: 2 0 10000 1 01 10 ${actual}\n0: 2 0 10000 1 01 40 ${alias}\n0: 2 0 0 1 03 20 ${actual}\n`);
    expect((await discoveryError(discoverCodexDaemon({ socketPath: alias, procRoot: root }))).mode).toBe("ambiguous_socket");
  });
  it.each([
    { flags: "not-hex" }, { type: "invalid" }, { state: "??" }, { flags: "10000", state: "03" },
  ])("malformed selected listener evidence refuses: %j", async metadata => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10, ...metadata }],
      procs: { 11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] } } });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root }))).mode).toBe("proc_unreadable");
  });
  it("unrelated protected/nonexistent bound paths do not block selected discovery", async () => {
    const protectedPath = "/fixture-protected/unrelated.sock";
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }, { path: protectedPath, inode: 20 }, { path: "/fixture-missing/unrelated.sock", inode: 30 }],
      procs: { 11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] } } });
    deniedPaths.set(protectedPath, "EACCES");
    expect((await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root })).pid).toBe(11);
  });
  it("selected-path permission failure remains critical", async () => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: { 11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] } } });
    deniedPaths.set("/s.sock", "EACCES");
    await expect(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root })).rejects.toMatchObject({ code: "EACCES" });
  });
  it.each(["cmdline", "stat"])("foreign-UID unreadable fd tables are outside the owner domain; child %s classification stays strict", async critical => {
    const foreign = process.getuid!() + 1;
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] },
      12: { argv: ["foreign"], uids: [foreign, foreign, foreign, foreign], fdUnreadable: true, ppid: 11 },
    } });
    const pin = await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root });
    expect((await snapshotDaemon(pin, { procRoot: root })).children.map(child => child.pid)).toEqual([12]);
    await rm(join(root, "12", critical)); await mkdir(join(root, "12", critical));
    expect((await discoveryError(snapshotDaemon(pin, { procRoot: root }))).mode).toBe("proc_unreadable");
  });
  it.each(["same-uid-fd", "unknown-uid", "missing-uid", "missing-fd", "mixed-uid", "owner-fd"])("unreadable/ambiguous admitted owner evidence refuses: %s", async kind => {
    const uid = process.getuid!(), foreign = uid + 1;
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"], fdUnreadable: kind === "owner-fd" },
      12: { argv: daemonArgv("/s.sock"), fdUnreadable: kind === "same-uid-fd", statusUnreadable: kind === "unknown-uid", uids: kind === "mixed-uid" ? [uid, foreign, uid, uid] : undefined },
    } });
    if (kind === "missing-uid") await rm(join(root, "12", "status"));
    if (kind === "missing-fd") await rm(join(root, "12", "fd"), { recursive: true });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root }))).mode).toBe("proc_unreadable");
  });
  it.each(["table", "descriptor"])("known same-UID noncandidate with unreadable %s does not block selected discovery", async kind => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] },
      12: { argv: ["/usr/lib/systemd/systemd", "--user"], fdUnreadable: kind === "table", fds: ["/dev/null"] },
    } });
    if (kind === "descriptor") deniedLinks.add(join(root, "12", "fd", "3"));
    expect((await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root })).pid).toBe(11);
  });
  it("eligible candidate descriptor read denial still refuses despite another proven owner", async () => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] },
      12: { argv: daemonArgv("/other.sock"), fds: ["/dev/null"] },
    } });
    deniedLinks.add(join(root, "12", "fd", "3"));
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root }))).mode).toBe("proc_unreadable");
  });
  it.each(["unreadable", "missing", "empty", "truncated"])("unknown same-UID candidate-domain argv refuses: %s", async kind => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] },
      12: { argv: ["unrelated"], cmdlineUnreadable: kind === "unreadable" },
    } });
    if (kind === "missing") await rm(join(root, "12", "cmdline"));
    if (kind === "empty") await writeFile(join(root, "12", "cmdline"), "");
    if (kind === "truncated") await writeFile(join(root, "12", "cmdline"), "codex\0app-server");
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root }))).mode).toBe("proc_unreadable");
  });
  it("foreign-UID daemons are explicitly unsupported, never selected from argv", async () => {
    const foreign = process.getuid!() + 1;
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), uids: [foreign, foreign, foreign, foreign], fds: ["socket:[10]"] },
    } });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root }))).mode).toBe("no_owner");
  });
  it("R7 cancelled proc observations stop instead of continuing traversal", async () => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: { 11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] } } });
    const pin = await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root });
    const signal = AbortSignal.abort();
    await expect(discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root, signal })).rejects.toThrow();
    await expect(snapshotDaemon(pin, { procRoot: root, signal })).rejects.toThrow();
    await expect(readProcessPin(root, 11, signal)).rejects.toThrow();
  });
  it("R5 refuses endpoint rebinding while the old daemon PID remains alive", async () => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] }, 12: { argv: daemonArgv("/s.sock"), fds: ["socket:[20]"] },
    } });
    const pin = await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root });
    await writeFile(join(root, "net", "unix"), "header\n0: 2 0 10000 1 01 20 /s.sock\n");
    expect((await discoveryError(assertDaemonIdentity(pin, { procRoot: root }))).mode).toBe("daemon_replaced");
  });
  it("R5 refuses namespace identity change or loss", async () => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: { 11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] } } });
    await mkdir(join(root, "11", "ns")); await symlink("pid:[1]", join(root, "11", "ns", "pid"));
    const pin = await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root });
    await rm(join(root, "11", "ns", "pid")); await symlink("pid:[2]", join(root, "11", "ns", "pid"));
    expect((await discoveryError(assertDaemonIdentity(pin, { procRoot: root }))).mode).toBe("daemon_replaced");
    await rm(join(root, "11", "ns", "pid"));
    expect((await discoveryError(assertDaemonIdentity(pin, { procRoot: root }))).mode).toBe("daemon_replaced");
  });
  it.each(["stat", "cmdline", "emptyCmdline", "root"])("R11 never counts unreadable %s evidence as zero children", async target => {
    const { root } = await makeProcTree({ sockets: [{ path: "/s.sock", inode: 10 }], procs: {
      11: { argv: daemonArgv("/s.sock"), fds: ["socket:[10]"] }, 12: { argv: ["child"], ppid: 11 },
    } });
    const pin = await discoverCodexDaemon({ socketPath: "/s.sock", procRoot: root });
    if (target === "root") {
      await expect(snapshotDaemon(pin, { procRoot: join(root, "net", "unix") })).rejects.toThrow();
    } else if (target === "emptyCmdline") {
      await writeFile(join(root, "12", "cmdline"), "");
      expect((await discoveryError(snapshotDaemon(pin, { procRoot: root }))).mode).toBe("proc_unreadable");
    } else {
      await rm(join(root, "12", target)); await mkdir(join(root, "12", target));
      expect((await discoveryError(snapshotDaemon(pin, { procRoot: root }))).mode).toBe("proc_unreadable");
    }
  });
});

describe("codex daemon discovery (real process)", () => {
  it("resolves a real spawned daemon that owns a real unix socket (argv0 = codex)", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptq-real-daemon-"));
    tempRoots.push(root);
    const socketPath = join(root, "app-server-control.sock");
    const script = join(root, "daemon-fixture.mjs");
    await writeFile(script, [
      "import { createServer } from 'node:net';",
      `const server = createServer(() => undefined);`,
      `server.listen(${JSON.stringify(socketPath)}, () => { process.stdout.write('ready\\n'); });`,
      "setInterval(() => undefined, 1000);",
    ].join("\n"));
    const child = spawn(process.execPath, [script, "app-server", "--listen", `unix://${socketPath}`, "--managed-daemon", "--analytics-default-enabled"], {
      argv0: "codex",
      stdio: ["ignore", "pipe", "pipe"],
    });
    await new Promise<void>((resolve, reject) => {
      child.stdout.once("data", () => resolve());
      child.once("error", reject);
      setTimeout(() => reject(new Error("daemon fixture did not start")), 5_000);
    });
    let held: Socket | undefined;
    try {
      const identity = await discoverCodexDaemon({ socketPath });
      expect(identity).toMatchObject({ pid: child.pid, basis: "socket-owner" });
      expect(identity.argv[0]).toBe("codex");
      held = createConnection(socketPath);
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("owned connection timeout")), 2_000);
        held!.once("connect", () => { clearTimeout(timer); resolve(); });
        held!.once("error", error => { clearTimeout(timer); reject(error); });
      });
      // A held accepted AF_UNIX connection must not invalidate the listener pin.
      await assertDaemonIdentity(identity);
      const snapshot = await snapshotDaemon(identity);
      expect(snapshot.children).toEqual([]);
      expect(snapshot.sidecars).toEqual([]);
    } finally {
      held?.destroy();
      child.kill("SIGKILL");
      await new Promise<void>((resolve) => child.once("exit", () => resolve()));
    }
  });
});

afterAll(async () => {
  await Promise.all(tempRoots.map((root) => rm(root, { recursive: true, force: true })));
});
