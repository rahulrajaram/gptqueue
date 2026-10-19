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

// ---- Child environments -------------------------------------------------

/**
 * Every environment variable through which gptqueue code, its scripts or its
 * tests name a Redis endpoint. URL aliases: REDIS_URL (all entry points),
 * GPTQUEUE_REDIS_URL (the OpenCode plugin prefers it over REDIS_URL),
 * GPTQ_VERIFY_REDIS_URL (verify-wrapper-child). Components: REDIS_HOST,
 * REDIS_PORT and GPTQ_REDIS_DB (scripts/check-queue.sh, consulted only when
 * REDIS_URL is empty). An inherited value of ANY of them could point a child
 * at another server, so the private URL replaces every one.
 */
export const REDIS_ENDPOINT_ENV = Object.freeze([
  "REDIS_URL", "GPTQUEUE_REDIS_URL", "GPTQ_VERIFY_REDIS_URL", "REDIS_HOST", "REDIS_PORT", "GPTQ_REDIS_DB",
]);

/**
 * The ONE builder for a child process environment: a copy of `base` with every
 * Redis endpoint variable set to the private URL `url` (default: base.REDIS_URL).
 * The URL must be a live private socket instance (privateRedisUrl), so this
 * throws instead of propagating a TCP or foreign endpoint. Applied last, so no
 * earlier key or spread in `base` can override an alias. For the components a
 * socket path is not a host, port or number, so check-queue.sh's TCP fallback
 * fails closed (and REDIS_URL, which it prefers, is the socket).
 * @param {Record<string, string | undefined>} base
 * @param {string} [url]
 */
export function privateRedisEnv(base, url = base.REDIS_URL) {
  const target = privateRedisUrl(url);
  const env = { ...base };
  for (const name of REDIS_ENDPOINT_ENV) env[name] = target;
  return env;
}

// ---- Process identity ---------------------------------------------------

/** Field 22 of /proc/<pid>/stat (start time in clock ticks): pid reuse changes it. */
const procStart = (pid) => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return null;
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19] ?? null;
  } catch { return null; }
};
const procArgv = (pid) => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return null;
  try {
    const argv = readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0");
    if (argv.at(-1) === "") argv.pop();
    return argv;
  } catch { return null; }
};
/** The real /proc reader; tests inject a fake with the same shape. */
export const linuxProc = Object.freeze({ start: procStart, argv: procArgv });
const pidNamespace = () => { try { return readlinkSync("/proc/self/ns/pid"); } catch { return "unknown"; } };

/** argv of a redis-server serving exactly `socket` (we pass --set-proc-title no, so argv is kept). */
const servesSocket = (argv, socket) => {
  if (!argv || !basename(argv[0] ?? "").startsWith("redis-server")) return false;
  const at = argv.indexOf("--unixsocket");
  return at !== -1 && argv[at + 1] === socket;
};
export const isRedisServingSocket = (pid, socket, proc = linuxProc) => servesSocket(proc.argv(pid), socket);

/**
 * True only while `server` ({ pid, start, socket, argv? }, recorded in
 * owner.json) is still the same process: same start time, still a
 * redis-server serving that socket, and (when recorded) the same argv. A
 * reused pid, an exited process or a zombie (empty cmdline) all fail. Every
 * signal and every liveness poll re-checks this, so a pid that was recycled
 * between polls is never signalled.
 */
export const sameServer = (server, proc = linuxProc) => {
  if (!server || typeof server.start !== "string" || typeof server.socket !== "string") return false;
  if (proc.start(server.pid) !== server.start) return false;
  const argv = proc.argv(server.pid);
  if (!servesSocket(argv, server.socket)) return false;
  return !Array.isArray(server.argv) || (argv.length === server.argv.length && argv.every((arg, i) => arg === server.argv[i]));
};

const POLL_MS = 25;
const defaultKill = (pid, signal) => { try { process.kill(pid, signal); } catch { /* already gone */ } };

/**
 * SIGTERM, then SIGKILL after `boundMs`, each sent only after `server` is
 * re-verified; polls re-verify too. Returns the signals actually sent.
 */
export async function stopVerifiedServer(server, { proc = linuxProc, kill = defaultKill, sleep: wait = sleep, boundMs = 3_000 } = {}) {
  const sent = [];
  const polls = Math.max(1, Math.ceil(boundMs / POLL_MS));
  for (const signal of ["SIGTERM", "SIGKILL"]) {
    if (!sameServer(server, proc)) return sent; // gone or replaced: never signal it
    kill(server.pid, signal);
    sent.push(signal);
    for (let i = 0; i < polls; i += 1) {
      if (!sameServer(server, proc)) return sent;
      await wait(POLL_MS);
    }
  }
  return sent;
}

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

/**
 * Reap private instances left behind by owners that died without close()
 * (e.g. a SIGKILLed vitest). Only directories named gptq-redis-XXXXXX, owned by
 * this user with mode 0700, are considered. A live owner's directory is never
 * touched. A server is signalled only while its recorded identity (pid, start
 * time, argv, socket) still matches, re-checked before every signal and poll.
 * `proc`, `kill` and `sleep` are injectable for deterministic tests.
 * @returns {Promise<Array<{ dir: string, signals: string[] }>>}
 */
export async function reapStaleOwnedRedis({
  root = tmpdir(), now = Date.now(), foreignStaleMs = FOREIGN_STALE_MS, stopTimeoutMs = 3_000,
  proc = linuxProc, kill = defaultKill, sleep: wait = sleep,
} = {}) {
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
    const ownerStart = sameNamespace ? proc.start(owner.owner_pid) : null;
    if (sameNamespace && ownerStart !== null && (owner.owner_start === null || ownerStart === owner.owner_start)) continue;
    const serving = await socketAnswers(socket);
    let signals = [];
    if (sameNamespace) {
      // The owner is provably gone. Stop its server only while it is verifiably that server.
      const server = { pid: owner.redis_pid, start: owner.redis_start, argv: owner.redis_argv, socket };
      if (sameServer(server, proc)) signals = await stopVerifiedServer(server, { proc, kill, sleep: wait, boundMs: stopTimeoutMs });
      else if (serving) continue; // something unverifiable answers there: leave it alone
    } else if (serving || now - stat.mtimeMs < foreignStaleMs) {
      // Another pid namespace (or no owner record yet): its processes are invisible
      // here, so only a directory at least foreignStaleMs old whose socket does
      // not answer now is removed.
      continue;
    }
    rmSync(dir, { recursive: true, force: true });
    reaped.push({ dir, signals });
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
    // The start time is fixed at fork, so it can be recorded before exec completes.
    if (child.pid !== undefined) writeOwner(dir, { ...owner, redis_pid: child.pid, redis_start: procStart(child.pid) });
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
    // Full identity for the reaper: pid, start time, the exec'd argv and the socket.
    writeOwner(dir, { ...owner, redis_pid: child.pid, redis_start: procStart(child.pid), redis_argv: procArgv(child.pid) });
  } catch (error) {
    await close();
    throw error;
  }
  return Object.freeze({ url: `${socket}?db=${db}`, socket, dir, db, pid: /** @type {number} */ (child.pid), close, stop: stopSync });
}
