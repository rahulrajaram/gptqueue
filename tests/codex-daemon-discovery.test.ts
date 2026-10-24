/**
 * Discovery tests for registered-shell/codex-daemon.ts over synthetic /proc
 * fixture trees (net/unix + per-pid cmdline/stat/fd), plus one real-process
 * integration case. Covers the f2269 field shape (extra --analytics-default-enabled
 * anywhere, bare `unix://` listen value), substring decoys, ambiguity and
 * read-denial refusals, the exact-endpoint-only fallback, and PID-reuse
 * pinning. No global machine-state assertions: every case is scoped to the
 * fixture tree (or the spawned fixture process).
 */
import { describe, expect, it, afterAll } from "vitest";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
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

type ProcSpec = Readonly<{
  argv?: readonly string[];
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
  sockets: ReadonlyArray<Readonly<{ path: string; inode: number }>>;
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
    lines.push(`0000000000000000: 00000002 00000000 00010000 0001 01 ${socket.inode} ${socket.path}`);
  }
  // Noise entries: connected client sockets (no path) and unrelated paths.
  lines.push("0000000000000000: 00000002 00000000 00010000 0001 03 999999");
  lines.push(`0000000000000000: 00000002 00000000 00010000 0001 01 999998 /unrelated/other.sock`);
  await writeFile(join(root, "net", "unix"), `${lines.join("\n")}\n`);
  for (const [pid, proc] of Object.entries(spec.procs)) {
    const dir = join(root, pid);
    await mkdir(dir, { recursive: true });
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
    // As the socket owner it is refused as a non-Codex owner...
    const owned = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: decoy, fds: ["socket:[10]"] } },
    });
    const error = await discoveryError(discoverCodexDaemon({ socketPath: owned.socketPath(), procRoot: owned.root }));
    expect(error.mode).toBe("owner_not_codex_app_server");
    // ...and in an argv scan it never matches, so the daemon stays not-found.
    const unowned = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: decoy } },
    });
    const notFound = await discoveryError(discoverCodexDaemon({ socketPath: unowned.socketPath(), procRoot: unowned.root }));
    expect(notFound.mode).toBe("daemon_not_found");
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

  it("sidecar classification is token-wise over the whole argv", () => {
    const sidecar = ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis:///x.sock"];
    expect(isGptqueueSessionSidecarArgv(sidecar)).toBe(true);
    expect(isGptqueueSessionSidecarArgv(["/opt/gptqueue/bin/gptqueue-session", "--client", "codex"])).toBe(true);
    expect(isGptqueueSessionSidecarArgv(["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "pi"])).toBe(false);
    expect(isGptqueueSessionSidecarArgv(["sh", "-c", "gptqueue-session --client codex"])).toBe(false);
  });

  it("refuses ambiguity loudly: two owners, two same-path inodes, two fallback matches", async () => {
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
    expect(ambiguous.mode).toBe("ambiguous_daemon");
    expect(ambiguous.detail.pids).toEqual([11, 12]);
  });

  it("refuses a non-Codex socket owner instead of falling back to a scan", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: ["python3", "-m", "http.server"], fds: ["socket:[10]"] },
        12: { argv: daemonArgv("/run/codex.sock") },
      },
    });
    const error = await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }));
    expect(error.mode).toBe("owner_not_codex_app_server");
    expect(error.message).toContain("pid 11");
  });

  it("refuses an unreadable owner argv (read denial), never reports zero daemons", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { cmdlineUnreadable: true, fds: ["socket:[10]"] } },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: socketPath(), procRoot: root }))).mode).toBe("owner_argv_unreadable");
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

  it("fallback argv scan selects only with the exact endpoint; bare unix:// cannot be correlated", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock"), fds: ["socket:[999997]"] }, // owns some other socket
      },
    });
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    expect(identity).toMatchObject({ pid: 11, basis: "argv-scan" });

    const bare = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: { 11: { argv: ["codex", "app-server", "--listen", "unix://", "--managed-daemon"] } },
    });
    expect((await discoveryError(discoverCodexDaemon({ socketPath: bare.socketPath(), procRoot: bare.root }))).mode).toBe("daemon_not_found");
  });

  it("an unreadable fd table of one pid is skipped, not fatal", async () => {
    const { root, socketPath } = await makeProcTree({
      sockets: [{ path: "/run/codex.sock", inode: 10 }],
      procs: {
        11: { argv: daemonArgv("/run/codex.sock"), fdUnreadable: true }, // the true owner, unreadable fds
        12: { argv: ["bash"], fds: [] },
      },
    });
    // pid 11's fd table cannot be read; it is still found by the argv scan's exact endpoint.
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    expect(identity).toMatchObject({ pid: 11, basis: "argv-scan" });
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
      },
    };
    const { root, socketPath } = await makeProcTree(spec);
    const identity = await discoverCodexDaemon({ socketPath: socketPath(), procRoot: root });
    const snapshot = await snapshotDaemon(identity, { procRoot: root });
    expect(snapshot.children.map((child) => child.pid).sort()).toEqual([100, 101, 102]);
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
    try {
      const identity = await discoverCodexDaemon({ socketPath });
      expect(identity).toMatchObject({ pid: child.pid, basis: "socket-owner" });
      expect(identity.argv[0]).toBe("codex");
      const snapshot = await snapshotDaemon(identity);
      expect(snapshot.children).toEqual([]);
      expect(snapshot.sidecars).toEqual([]);
    } finally {
      child.kill("SIGKILL");
      await new Promise<void>((resolve) => child.once("exit", () => resolve()));
    }
  });
});

afterAll(async () => {
  await Promise.all(tempRoots.map((root) => rm(root, { recursive: true, force: true })));
});
