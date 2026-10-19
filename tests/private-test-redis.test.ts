/**
 * The run's private test Redis (scripts/lib/owned-redis.mjs,
 * tests/helpers/global-setup.ts, tests/helpers/test-redis.ts): privacy checks,
 * fail-closed startup, teardown, stale-instance reaping, propagation to
 * workers and children, and refusal of every TCP URL.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, readlinkSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import {
  RUN_DIR_PREFIX, parsePrivateRedisUrl, privateRedisUrl, probeServer, reapStaleOwnedRedis, startOwnedRedis,
  type OwnedRedis, type ServerProbe,
} from "../scripts/lib/owned-redis.mjs";
import { exportPrivateRedis } from "./helpers/global-setup.js";
import { testRedisTarget, testRedisUrl } from "./helpers/test-redis.js";

const REPO = process.cwd();
const HELPER = join(REPO, "scripts/lib/owned-redis.mjs");
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
const waitFor = async (check: () => boolean, ms = 5_000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (check()) return true; await new Promise((r) => setTimeout(r, 25)); }
  return check();
};
/** Inodes of LISTEN sockets in /proc/net/tcp{,6} (state 0A). */
const tcpListenInodes = (): Set<string> => new Set(["/proc/net/tcp", "/proc/net/tcp6"].flatMap((file) => {
  try {
    return readFileSync(file, "utf8").split("\n").slice(1).map((line) => line.trim().split(/\s+/u))
      .filter((fields) => fields[3] === "0A").map((fields) => fields[9] ?? "");
  } catch { return []; }
}));
const socketInodes = (pid: number): string[] => readdirSync(`/proc/${pid}/fd`).flatMap((fd) => {
  try { return [/^socket:\[(\d+)\]$/u.exec(readlinkSync(`/proc/${pid}/fd/${fd}`))?.[1] ?? ""].filter(Boolean); } catch { return []; }
});

// Every instance this file starts lives under its own root, so leftovers are observable.
let root: string;
const started: OwnedRedis[] = [];
const extraPids: number[] = [];
// A short root: the socket path must fit in sun_path (108 bytes).
beforeAll(() => { root = mkdtempSync(join(tmpdir(), "gq-")); });
afterAll(async () => {
  for (const redis of started) await redis.close();
  for (const pid of extraPids) { try { process.kill(pid, "SIGKILL"); } catch { /* gone */ } }
  rmSync(root, { recursive: true, force: true });
});
afterEach(() => { vi.unstubAllEnvs(); });

const start = async (options: Parameters<typeof startOwnedRedis>[0] = {}) => {
  const redis = await startOwnedRedis({ root, ...options });
  started.push(redis);
  return redis;
};
const runDirs = () => readdirSync(root).filter((name) => name.startsWith(RUN_DIR_PREFIX));

describe("the run's private instance (globalSetup)", () => {
  it("is exported to this worker as a validated socket URL with db 15", () => {
    const url = testRedisUrl();
    expect(process.env.REDIS_URL).toBe(url);
    expect(testRedisTarget()).toMatchObject({ db: 15 });
    expect(url).toMatch(/^\/[A-Za-z0-9._/-]+\/gptq-redis-[A-Za-z0-9]{6}\/redis\.sock\?db=15$/u);
  });

  it("passes the privacy check: port 0, this socket, the recorded server pid, and no TCP listener", async () => {
    const target = testRedisTarget();
    const owner = JSON.parse(readFileSync(join(target.dir, "owner.json"), "utf8")) as { redis_pid: number; socket: string };
    const facts = await probeServer({ socket: target.path, timeoutMs: 1_000 });
    expect(facts).toEqual({ port: "0", unixsocket: target.path, processId: owner.redis_pid });
    expect(owner.socket).toBe(target.path);
    const listening = tcpListenInodes();
    expect(socketInodes(owner.redis_pid).filter((inode) => listening.has(inode))).toEqual([]);
  });

  it("is what spawned children inherit, and they reach the same instance", async () => {
    const key = `gptq:test:child-visibility:${randomUUID()}`;
    const script = [
      `import { Redis } from "ioredis";`,
      `const redis = new Redis(process.env.REDIS_URL);`,
      `await redis.set(${JSON.stringify(key)}, "from-child");`,
      `console.log(JSON.stringify({ url: process.env.REDIS_URL, path: redis.options.path, db: redis.options.db }));`,
      `await redis.quit();`,
    ].join("\n");
    const child = spawnSync(process.execPath, ["--input-type=module", "-e", script], { cwd: REPO, encoding: "utf8", timeout: 15_000 });
    expect(child.status, child.stderr).toBe(0);
    const seen = JSON.parse(child.stdout.trim()) as { url: string; path: string; db: number };
    expect(seen).toEqual({ url: testRedisUrl(), path: testRedisTarget().path, db: 15 });
    const redis = new Redis(testRedisUrl());
    try {
      expect(await redis.get(key)).toBe("from-child");
      await redis.del(key);
    } finally { await redis.quit(); }
  });
});

describe("startup fails closed and exports nothing", () => {
  const lie = (patch: (facts: Awaited<ReturnType<ServerProbe>>) => Partial<Awaited<ReturnType<ServerProbe>>>, seen: number[]): ServerProbe =>
    async (target) => {
      const facts = await probeServer(target);
      seen.push(facts.processId);
      return { ...facts, ...patch(facts) };
    };

  it.each([
    ["a TCP port", (): Partial<Awaited<ReturnType<ServerProbe>>> => ({ port: "6379" }), /CONFIG GET port is "6379"/u],
    ["another process", (f: Awaited<ReturnType<ServerProbe>>) => ({ processId: f.processId + 100_000 }), /process_id is \d+, not the spawned pid/u],
    ["another socket", (): Partial<Awaited<ReturnType<ServerProbe>>> => ({ unixsocket: "/var/run/redis/redis-server.sock" }), /CONFIG GET unixsocket is/u],
  ] as const)("when the server reports %s", async (_label, patch, message) => {
    const env: NodeJS.ProcessEnv = { REDIS_URL: "redis://127.0.0.1:6379/15" };
    const seen: number[] = [];
    await expect(exportPrivateRedis({ root, probe: lie(patch, seen) }, env)).rejects.toThrow(message);
    expect(env.REDIS_URL).toBe("redis://127.0.0.1:6379/15"); // untouched: nothing exported
    expect(seen).toHaveLength(1);
    expect(await waitFor(() => !alive(seen[0]!))).toBe(true); // the server was stopped
    expect(runDirs()).toEqual([]); // and its run dir removed
  });

  it("when redis-server cannot be spawned", async () => {
    const env: NodeJS.ProcessEnv = {};
    await expect(exportPrivateRedis({ root, serverBin: join(root, "no-such-redis-server") }, env)).rejects.toThrow(/spawn failed/u);
    expect(env.REDIS_URL).toBeUndefined();
    expect(runDirs()).toEqual([]);
  });

  it("when the socket never becomes ready within the bound", async () => {
    const env: NodeJS.ProcessEnv = {};
    const never: ServerProbe = async () => { throw new Error("not ready"); };
    await expect(exportPrivateRedis({ root, readyTimeoutMs: 300, probe: never }, env)).rejects.toThrow(/not ready within 300 ms/u);
    expect(env.REDIS_URL).toBeUndefined();
    expect(runDirs()).toEqual([]);
  });
});

describe("teardown", () => {
  it("close() stops the server and removes the run dir; it is idempotent", async () => {
    const redis = await startOwnedRedis({ root });
    expect(alive(redis.pid)).toBe(true);
    expect(existsSync(redis.dir)).toBe(true);
    await redis.close();
    await redis.close();
    expect(await waitFor(() => !alive(redis.pid))).toBe(true);
    expect(existsSync(redis.dir)).toBe(false);
    expect(runDirs()).toEqual([]);
  });
});

describe("stale-instance reaping", () => {
  const namespace = readlinkSync("/proc/self/ns/pid");
  const deadPid = (): number => {
    const child = spawnSync(process.execPath, ["-e", "console.log(process.pid)"], { encoding: "utf8" });
    return Number(child.stdout.trim());
  };
  const fakeRunDir = (owner: Record<string, unknown>) => {
    const dir = mkdtempSync(join(root, RUN_DIR_PREFIX));
    chmodSync(dir, 0o700);
    writeFileSync(join(dir, "owner.json"), JSON.stringify({ socket: join(dir, "redis.sock"), created_at: new Date().toISOString(), ...owner }));
    return dir;
  };

  it("kills only a redis-server serving the dead owner's socket and never a live owner's instance", async () => {
    // 1. A real orphan: its owner is SIGKILLed after startup, so no cleanup ran.
    const ownerProc: ChildProcess = spawn(process.execPath, ["--input-type=module", "-e",
      `const m = await import(${JSON.stringify(HELPER)}); const r = await m.startOwnedRedis({ root: ${JSON.stringify(root)}, reap: false });` +
      ` console.log(JSON.stringify({ pid: r.pid, dir: r.dir })); setInterval(() => {}, 60_000);`], { stdio: ["ignore", "pipe", "inherit"] });
    const orphan = await new Promise<{ pid: number; dir: string }>((resolve, reject) => {
      let out = "";
      ownerProc.stdout!.on("data", (chunk) => { out += chunk; if (out.includes("\n")) resolve(JSON.parse(out)); });
      ownerProc.once("exit", (code) => reject(new Error(`owner exited ${code}`)));
    });
    extraPids.push(orphan.pid);
    ownerProc.kill("SIGKILL");
    await new Promise((resolve) => ownerProc.once("exit", resolve));
    expect(alive(orphan.pid)).toBe(true);

    // 2. A live owner (this worker) with its own server: must be left alone.
    const live = await start({ reap: false });
    // 3. Dead owner whose recorded pid is a live NON-redis process: never killed.
    const sleeper = spawn("sleep", ["30"], { stdio: "ignore" });
    extraPids.push(sleeper.pid!);
    const decoyNotRedis = fakeRunDir({ owner_pid: deadPid(), owner_start: "0", pid_ns: namespace, redis_pid: sleeper.pid });
    // 4. Dead owner whose recorded pid is a redis-server serving ANOTHER socket: never killed.
    const decoyOtherSocket = fakeRunDir({ owner_pid: deadPid(), owner_start: "0", pid_ns: namespace, redis_pid: live.pid });

    const reaped = await reapStaleOwnedRedis({ root });

    expect(reaped).toEqual(expect.arrayContaining([
      { dir: orphan.dir, killed: orphan.pid },
      { dir: decoyNotRedis, killed: null },
      { dir: decoyOtherSocket, killed: null },
    ]));
    expect(reaped).toHaveLength(3);
    expect(await waitFor(() => !alive(orphan.pid))).toBe(true);
    expect(existsSync(orphan.dir)).toBe(false);
    expect(alive(sleeper.pid!)).toBe(true);
    expect(alive(live.pid)).toBe(true);
    expect(existsSync(live.dir)).toBe(true);
    expect(privateRedisUrl(live.url)).toBe(live.url);
    sleeper.kill("SIGKILL");
  }, 30_000);

  it("leaves a directory from another pid namespace until it has been silent for the grace period", async () => {
    const foreign = fakeRunDir({ owner_pid: 1, owner_start: "1", pid_ns: "pid:[1]", redis_pid: 2 });
    expect(await reapStaleOwnedRedis({ root })).toEqual([]);
    expect(existsSync(foreign)).toBe(true);
    expect(await reapStaleOwnedRedis({ root, now: Date.now() + 61_000 })).toEqual([{ dir: foreign, killed: null }]);
    expect(existsSync(foreign)).toBe(false);
  });

  it("ignores directories that are not 0700 run dirs owned by this user", async () => {
    const loose = mkdtempSync(join(root, RUN_DIR_PREFIX));
    chmodSync(loose, 0o755);
    const other = join(root, "not-a-run-dir");
    mkdirSync(other, { mode: 0o700 });
    expect(await reapStaleOwnedRedis({ root, now: Date.now() + 3_600_000 })).toEqual([]);
    expect(existsSync(loose) && existsSync(other)).toBe(true);
    rmSync(loose, { recursive: true }); rmSync(other, { recursive: true });
  });
});

describe("no path to a TCP Redis", () => {
  it.each([
    "redis://127.0.0.1:6379/15",
    "redis://127.0.0.1:6379/0",
    "redis://localhost/15",
    "rediss://127.0.0.1:6379/15",
    "127.0.0.1:6379",
  ])("testRedisUrl() refuses REDIS_URL=%s", (url) => {
    vi.stubEnv("REDIS_URL", url);
    expect(() => testRedisUrl()).toThrow(/private unix-socket instance/u);
  });

  it("refuses an unset REDIS_URL instead of falling back", () => {
    vi.stubEnv("REDIS_URL", undefined);
    expect(() => testRedisUrl()).toThrow(/it is unset/u);
  });

  it("refuses socket forms outside a private run dir, with another db, or not normalized", () => {
    const { dir } = testRedisTarget();
    for (const value of [
      "/var/run/redis/redis-server.sock?db=15",
      `${dir}/redis.sock`,
      `${dir}/redis.sock?db=0`,
      `${dir}/redis.sock?db=16`,
      `${dir}/redis.sock?db=15&host=127.0.0.1`,
      `${dir}/redis.sock?db=15#x`,
      `${dir}/../${dir.split("/").at(-1)}/redis.sock?db=15`,
      `${dir}//redis.sock?db=15`,
      `${dir}/redis%2esock?db=15`,
      `//${dir}/redis.sock?db=15`,
    ]) expect(() => parsePrivateRedisUrl(value), value).toThrow(/private unix-socket instance/u);
  });

  it("refuses a well-formed socket URL whose run dir is not a 0700 dir owned by this user", () => {
    const loose = mkdtempSync(join(root, RUN_DIR_PREFIX));
    chmodSync(loose, 0o755);
    try {
      expect(() => privateRedisUrl(join(loose, "redis.sock") + "?db=15")).toThrow(/0700 directory owned by this user/u);
    } finally { rmSync(loose, { recursive: true }); }
  });

  it("parses exactly as ioredis does (no parser differential)", () => {
    const target = testRedisTarget();
    const redis = new Redis(testRedisUrl(), { lazyConnect: true });
    try {
      expect(redis.options.path).toBe(target.path);
      expect(redis.options.db).toBe(target.db);
    } finally { redis.disconnect(); }
  });
});
