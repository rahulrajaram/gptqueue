// The ONE private test Redis implementation. Every vitest run (globalSetup),
// verify script, release installer and acceptance fixture starts its own
// redis-server through startOwnedRedis():
//
//   - reachable ONLY over a unix socket in a fresh 0700 run directory under
//     $TMPDIR (`--port 0`: no TCP listener at all);
//   - verified private before anything is exported: over the socket,
//     `CONFIG GET port` must be "0", `CONFIG GET unixsocket` must be this run's
//     socket and `INFO server` process_id must be the spawned child's pid;
//   - addressed by one connection string, `<runDir>/redis.sock?db=N`, which
//     ioredis parses as { path, db } (see parsePrivateRedisUrl);
//   - torn down by close() (SIGTERM, then SIGKILL after a bound) or, on the
//     synchronous exit path, by stop() (SIGKILL); the run directory is removed;
//   - crash safe: owner.json records the owner and the server pid, and the next
//     start reaps instances whose owner died (only after checking that the pid
//     is a redis-server serving exactly that socket).
//
// The test helpers accept no other Redis target, so the production server
// (TCP 127.0.0.1:6379) is unreachable through them.
import { spawn } from "node:child_process";
import { chmodSync, lstatSync, mkdtempSync, readFileSync, readdirSync, readlinkSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { Redis } from "ioredis";

export const RUN_DIR_PREFIX = "gptq-redis-";
const RUN_DIR = /^gptq-redis-[A-Za-z0-9]{6}$/u;
const SOCKET_NAME = "redis.sock";
const OWNER_FILE = "owner.json";
const DEFAULT_SERVER = "/usr/bin/redis-server";
/** sun_path holds 108 bytes including the terminating NUL. */
const MAX_SOCKET_PATH_BYTES = 107;
/** A run dir in another (invisible) pid namespace is reaped only when nothing serves it for this long. */
const FOREIGN_STALE_MS = 60_000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const uid = () => process.getuid?.() ?? -1;

// ---- Connection string --------------------------------------------------

const refuse = (reason) => new Error(
  `Test Redis must be this run's private unix-socket instance (<run dir>/redis.sock?db=N, ` +
  `started by vitest globalSetup or scripts/lib/owned-redis.mjs); refusing REDIS_URL: ${reason}. ` +
  `TCP Redis URLs (including the production 127.0.0.1:6379) are never accepted by the test helpers.`
);

const describeRefused = (value) => {
  const scheme = /^([A-Za-z][A-Za-z0-9+.-]*):/u.exec(value)?.[1];
  return scheme ? `a ${scheme}: URL` : "not an absolute socket path";
};

/**
 * Shape check (no filesystem access). Accepts exactly
 * `/<seg>/.../gptq-redis-XXXXXX/redis.sock?db=<1-15>` where every segment is
 * `[A-Za-z0-9._-]+` and none is `.` or `..`. That character set has no meaning
 * to any URL parser, so ioredis's `url.parse` yields exactly
 * { path: <everything before ?>, db } — there is no parser differential (F1).
 * @param {string | undefined} value
 */
export function parsePrivateRedisUrl(value) {
  if (typeof value !== "string" || value === "") throw refuse("it is unset");
  if (!value.startsWith("/")) throw refuse(describeRefused(value));
  const mark = value.indexOf("?");
  if (mark === -1) throw refuse("no ?db=N");
  const path = value.slice(0, mark);
  const query = value.slice(mark + 1);
  const db = /^db=([1-9]|1[0-5])$/u.exec(query)?.[1];
  if (db === undefined) throw refuse("the query must be exactly db=1..15");
  const segments = path.split("/").slice(1);
  if (segments.some((s) => s === "." || s === ".." || !/^[A-Za-z0-9._-]+$/u.test(s))) {
    throw refuse("the path must be absolute and normalized, with [A-Za-z0-9._-] segments");
  }
  if (segments.at(-1) !== SOCKET_NAME || !RUN_DIR.test(segments.at(-2) ?? "")) {
    throw refuse(`the path must be <dir>/${RUN_DIR_PREFIX}XXXXXX/${SOCKET_NAME}`);
  }
  return Object.freeze({ path, dir: dirname(path), db: Number(db) });
}

/**
 * The shape check plus live ownership: the run dir is a real 0700 directory
 * owned by this user and the socket exists and is ours. Returns the value.
 * @param {string | undefined} value
 */
export function privateRedisUrl(value) {
  const target = parsePrivateRedisUrl(value);
  const dir = lstatSync(target.dir, { throwIfNoEntry: false });
  if (!dir?.isDirectory() || dir.uid !== uid() || (dir.mode & 0o777) !== 0o700) {
    throw refuse("its run directory is not a 0700 directory owned by this user");
  }
  const socket = lstatSync(target.path, { throwIfNoEntry: false });
  if (!socket?.isSocket() || socket.uid !== uid()) throw refuse("its socket does not exist or is not owned by this user");
  return /** @type {string} */ (value);
}

// ---- Process identity ---------------------------------------------------

const pidAlive = (pid) => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (error) { return error?.code === "EPERM"; }
};
/** Field 22 of /proc/<pid>/stat (start time): pid reuse changes it. */
const procStart = (pid) => {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] ?? null;
  } catch { return null; }
};
const pidNamespace = () => { try { return readlinkSync("/proc/self/ns/pid"); } catch { return "unknown"; } };
const argvOf = (pid) => {
  try {
    const argv = readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0");
    if (argv.at(-1) === "") argv.pop();
    return argv;
  } catch { return null; }
};
/** True only for a redis-server whose argv serves exactly `socket` (we pass --set-proc-title no). */
export const isRedisServingSocket = (pid, socket) => {
  const argv = argvOf(pid);
  if (!argv || !basename(argv[0] ?? "").startsWith("redis-server")) return false;
  const at = argv.indexOf("--unixsocket");
  return at !== -1 && argv[at + 1] === socket;
};

const writeOwner = (dir, owner) => {
  const tmp = join(dir, `${OWNER_FILE}.tmp`);
  writeFileSync(tmp, JSON.stringify(owner), { mode: 0o600 });
  renameSync(tmp, join(dir, OWNER_FILE));
};
const readOwner = (dir) => {
  try {
    const owner = JSON.parse(readFileSync(join(dir, OWNER_FILE), "utf8"));
    return owner && typeof owner === "object" ? owner : null;
  } catch { return null; }
};

const socketAnswers = (path, timeoutMs = 300) => new Promise((resolve) => {
  const socket = createConnection({ path });
  const done = (answer) => { clearTimeout(timer); socket.destroy(); resolve(answer); };
  // A hung listener counts as live: never delete a directory something may still serve.
  const timer = setTimeout(() => done(true), timeoutMs);
  socket.once("connect", () => done(true));
  socket.once("error", () => done(false));
});

const waitGone = async (pid, ms) => {
  const end = Date.now() + ms;
  while (pidAlive(pid) && Date.now() < end) await sleep(25);
  return !pidAlive(pid);
};

const signalPid = (pid, signal) => { try { process.kill(pid, signal); } catch { /* already gone */ } };

/**
 * Reap private instances left behind by owners that died without close()
 * (e.g. a SIGKILLed vitest). Only directories named gptq-redis-XXXXXX, owned by
 * this user with mode 0700, are considered. A live owner's directory is never
 * touched. A process is killed only when its argv proves it is a redis-server
 * serving exactly that directory's socket.
 * @returns {Promise<Array<{ dir: string, killed: number | null }>>}
 */
export async function reapStaleOwnedRedis({ root = tmpdir(), now = Date.now(), foreignStaleMs = FOREIGN_STALE_MS, stopTimeoutMs = 3_000 } = {}) {
  const reaped = [];
  let entries;
  try { entries = readdirSync(root, { withFileTypes: true }); } catch { return reaped; }
  const namespace = pidNamespace();
  for (const entry of entries) {
    if (!entry.isDirectory() || !RUN_DIR.test(entry.name)) continue;
    const dir = join(root, entry.name);
    const stat = lstatSync(dir, { throwIfNoEntry: false });
    if (!stat?.isDirectory() || stat.uid !== uid() || (stat.mode & 0o777) !== 0o700) continue;
    const socket = join(dir, SOCKET_NAME);
    const owner = readOwner(dir);
    const sameNamespace = owner !== null && owner.pid_ns === namespace;
    if (sameNamespace && pidAlive(owner.owner_pid) && (owner.owner_start === null || procStart(owner.owner_pid) === owner.owner_start)) continue;
    const serving = await socketAnswers(socket);
    let killed = null;
    if (sameNamespace) {
      // The owner is provably gone. Stop its server only if it is verifiably ours.
      if (pidAlive(owner.redis_pid) && isRedisServingSocket(owner.redis_pid, socket)) {
        signalPid(owner.redis_pid, "SIGTERM");
        if (!(await waitGone(owner.redis_pid, stopTimeoutMs))) {
          signalPid(owner.redis_pid, "SIGKILL");
          await waitGone(owner.redis_pid, stopTimeoutMs);
        }
        killed = owner.redis_pid;
      } else if (serving) continue; // something unverifiable answers there: leave it alone
    } else if (serving || now - stat.mtimeMs < foreignStaleMs) {
      // Another pid namespace (or no owner record yet): its processes are invisible
      // here, so only a long-silent directory is safe to remove.
      continue;
    }
    rmSync(dir, { recursive: true, force: true });
    reaped.push({ dir, killed });
  }
  return reaped;
}

// ---- Server -------------------------------------------------------------

/**
 * Default privacy probe: connect over the socket only (an options object with
 * `path`, never a URL string) and read the facts the caller verifies.
 * @param {{ socket: string, timeoutMs: number }} target
 */
export async function probeServer({ socket, timeoutMs }) {
  const client = new Redis({
    path: socket, db: 0, lazyConnect: true, connectTimeout: timeoutMs, commandTimeout: timeoutMs,
    maxRetriesPerRequest: 0, retryStrategy: () => null, enableOfflineQueue: false,
  });
  client.on("error", () => { /* surfaced through the awaited calls */ });
  try {
    await client.connect();
    const [, port] = /** @type {string[]} */ (await client.config("GET", "port"));
    const [, unixsocket] = /** @type {string[]} */ (await client.config("GET", "unixsocket"));
    const info = await client.info("server");
    const processId = Number(/^process_id:(\d+)\r?$/mu.exec(info)?.[1]);
    return { port: String(port), unixsocket: String(unixsocket), processId };
  } finally { client.disconnect(); }
}

/** SIGTERM, then SIGKILL after `boundMs`; `exited` was subscribed at spawn time. */
const stopChild = async (child, exited, boundMs) => {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const within = (ms) => Promise.race([exited.then(() => true), sleep(ms).then(() => false)]);
  child.kill("SIGTERM");
  if (await within(boundMs)) return;
  child.kill("SIGKILL");
  await within(boundMs);
};

/**
 * Start, verify and return a private socket-only redis-server.
 * @param {import("./owned-redis.mjs").StartOwnedRedisOptions} [options]
 */
export async function startOwnedRedis({
  db = 15,
  root = tmpdir(),
  serverBin = process.env.GPTQ_TEST_REDIS_SERVER || DEFAULT_SERVER,
  readyTimeoutMs = 5_000,
  stopTimeoutMs = 3_000,
  probe = probeServer,
  reap = true,
} = {}) {
  if (!Number.isInteger(db) || db < 1 || db > 15) throw new Error(`startOwnedRedis: db must be 1..15, got ${db}`);
  if (reap) await reapStaleOwnedRedis({ root, stopTimeoutMs });
  const dir = mkdtempSync(join(root, RUN_DIR_PREFIX));
  chmodSync(dir, 0o700);
  const socket = join(dir, SOCKET_NAME);
  const logfile = join(dir, "redis.log");
  if (Buffer.byteLength(socket) > MAX_SOCKET_PATH_BYTES) {
    rmSync(dir, { recursive: true, force: true });
    throw new Error(`startOwnedRedis: socket path ${socket} exceeds ${MAX_SOCKET_PATH_BYTES} bytes; point TMPDIR at a shorter directory`);
  }
  const owner = { owner_pid: process.pid, owner_start: procStart(process.pid), pid_ns: pidNamespace(), socket, redis_pid: null, created_at: new Date().toISOString() };
  // Recorded before the spawn, so a directory with a live server always names its owner.
  writeOwner(dir, owner);

  const child = spawn(serverBin, [
    "--port", "0", "--unixsocket", socket, "--unixsocketperm", "700",
    "--save", "", "--appendonly", "no", "--dir", dir, "--databases", "16",
    "--daemonize", "no", "--logfile", logfile, "--set-proc-title", "no",
  ], { stdio: ["ignore", "ignore", "pipe"] });
  const exited = new Promise((resolve) => child.once("exit", resolve));
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr = (stderr + chunk).slice(-2_000); });
  let spawnError;
  child.once("error", (error) => { spawnError = error; });
  const diagnostics = () => {
    let log = "";
    try { log = readFileSync(logfile, "utf8").slice(-2_000); } catch { /* no log yet */ }
    return `${stderr}${log}`.trim();
  };

  // No redis-server outlives its owner: process.exit() or a natural exit with
  // close() never called still kills it. Default-action signals skip "exit";
  // the next start reaps those (a PID-namespace sandbox also reaps its own).
  let closed = false;
  const stopSync = () => {
    if (closed) return;
    closed = true;
    process.off("exit", stopSync);
    if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
      try { child.kill("SIGKILL"); } catch { /* gone */ }
    }
    rmSync(dir, { recursive: true, force: true });
  };
  process.once("exit", stopSync);
  const close = async () => {
    if (closed) return;
    closed = true;
    process.off("exit", stopSync);
    await stopChild(child, exited, stopTimeoutMs);
    rmSync(dir, { recursive: true, force: true });
  };

  try {
    if (child.pid !== undefined) writeOwner(dir, { ...owner, redis_pid: child.pid });
    const deadline = Date.now() + readyTimeoutMs;
    let facts;
    for (;;) {
      if (spawnError) throw new Error(`redis-server spawn failed (${serverBin}): ${spawnError.message}`);
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(`redis-server exited before readiness (${child.exitCode ?? child.signalCode}): ${diagnostics()}`);
      }
      if (Date.now() > deadline) throw new Error(`redis-server socket not ready within ${readyTimeoutMs} ms: ${diagnostics()}`);
      if (lstatSync(socket, { throwIfNoEntry: false })?.isSocket()) {
        try { facts = await probe({ socket, timeoutMs: 500 }); break; } catch { /* not accepting yet */ }
      }
      await sleep(25);
    }
    const problems = [
      facts.port !== "0" && `CONFIG GET port is ${JSON.stringify(facts.port)}, not "0"`,
      facts.unixsocket !== socket && `CONFIG GET unixsocket is ${JSON.stringify(facts.unixsocket)}, not ${socket}`,
      facts.processId !== child.pid && `INFO server process_id is ${facts.processId}, not the spawned pid ${child.pid}`,
    ].filter(Boolean);
    if (problems.length > 0) throw new Error(`private Redis verification failed; nothing exported: ${problems.join("; ")}`);
  } catch (error) {
    await close();
    throw error;
  }
  return Object.freeze({ url: `${socket}?db=${db}`, socket, dir, db, pid: /** @type {number} */ (child.pid), close, stop: stopSync });
}
