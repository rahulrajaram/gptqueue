/**
 * F1: an inherited Redis endpoint alias (GPTQUEUE_REDIS_URL, ...) must never
 * redirect a test child. A SECOND private instance stands in for "another
 * server": every alias in the parent env points at it. A child runs the real
 * OpenCode plugin entry point (which prefers GPTQUEUE_REDIS_URL over
 * REDIS_URL) and registers a session. The intended private instance must
 * receive that registration and the sentinel must receive ZERO connections and
 * ZERO commands (INFO stats / INFO keyspace on the sentinel).
 */
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import * as ownedRedis from "../scripts/lib/owned-redis.mjs";
import { exportPrivateRedis } from "./helpers/global-setup.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const ROOT = process.cwd();
const DIST = join(ROOT, "dist");
/** Spelled out (not imported) so this file also exercises older helpers. */
const ALIASES = ["REDIS_URL", "GPTQUEUE_REDIS_URL", "GPTQ_VERIFY_REDIS_URL", "REDIS_HOST", "REDIS_PORT", "GPTQ_REDIS_DB"] as const;

const CHILD = `
const [dist, sessionID] = process.argv.slice(2);
const { default: plugin } = await import(dist + "/registered-shell/opencode-plugin.js");
const { opencodeAgentName } = await import(dist + "/registered-shell/opencode-backend.js");
const directory = process.cwd();
const client = { session: {
  get: async () => ({ id: sessionID, directory }),
  status: async () => ({}),
  messages: async () => [],
  promptAsync: async () => undefined,
} };
const hooks = await plugin({ client, directory }, { intervalMs: 60000 });
await hooks.event({ event: { type: "session.created", properties: { info: { id: sessionID, directory } } } });
process.stdout.write(JSON.stringify({ agent: opencodeAgentName(sessionID) }) + "\\n");
await new Promise((resolve) => process.stdin.once("data", resolve));
await hooks.dispose();
process.exit(0);
`;

let root: string;
let script: string;
let sentinel: ownedRedis.OwnedRedis;
const started: ownedRedis.OwnedRedis[] = [];

beforeAll(async () => {
  if (!existsSync(join(DIST, "registered-shell/opencode-plugin.js"))) throw new Error("dist is missing: run npm run build");
  root = mkdtempSync(join(tmpdir(), "gq-env-")); // short: sun_path is 108 bytes
  script = join(root, "plugin-child.mjs");
  writeFileSync(script, CHILD);
  sentinel = await ownedRedis.startOwnedRedis({ root });
  started.push(sentinel);
});
afterAll(async () => {
  for (const redis of started) await redis.close();
  rmSync(root, { recursive: true, force: true });
});
afterEach(() => { vi.unstubAllEnvs(); });

/** Connections, commands and keys seen by an instance, read over one long-lived observer connection. */
const stats = async (observer: Redis) => {
  const info = String(await observer.call("INFO", "stats", "keyspace"));
  const field = (name: string) => Number(new RegExp(`^${name}:(\\d+)`, "mu").exec(info)?.[1]);
  return { connections: field("total_connections_received"), commands: field("total_commands_processed"), keyspace: /^db\d+:/mu.test(info) };
};

/** Run the real plugin entry point in a child with `env`; resolve once it has registered. */
const runPluginChild = async (env: NodeJS.ProcessEnv) => {
  const sessionID = `ses-hostile-${randomUUID()}`;
  const child = spawn(process.execPath, [script, DIST, sessionID], { cwd: root, env, stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  child.stderr!.on("data", (chunk) => { stderr += chunk; });
  const exited = new Promise<number | null>((resolve) => child.once("exit", resolve));
  const agent = await new Promise<string>((resolve, reject) => {
    let out = "";
    child.stdout!.on("data", (chunk) => { out += chunk; if (out.includes("\n")) resolve((JSON.parse(out) as { agent: string }).agent); });
    child.once("exit", (code) => reject(new Error(`plugin child exited ${code} before registering: ${stderr}`)));
  });
  return { agent, finish: async () => { child.stdin!.end("done\n"); expect(await exited).toBe(0); } };
};

/** Run the child, then check where its writes landed. */
const assertOnlyReaches = async (env: NodeJS.ProcessEnv, intended: ownedRedis.OwnedRedis) => {
  const watch = new Redis(sentinel.url);
  const target = new Redis(intended.url);
  try {
    const before = await stats(watch);
    const run = await runPluginChild(env);
    try {
      await expect.poll(() => target.hexists(SESSION_KEYS.registry, run.agent), { timeout: 5_000 }).toBe(1);
    } finally { await run.finish(); }
    const after = await stats(watch);
    // The sentinel saw only this observer's own INFO: no new connection, no other command, no key.
    expect({ connections: after.connections - before.connections, commands: after.commands - before.commands, keyspace: after.keyspace })
      .toEqual({ connections: 0, commands: 1, keyspace: false });
  } finally {
    watch.disconnect(); target.disconnect();
  }
};

describe("hostile inherited Redis aliases (F1)", () => {
  it("globalSetup replaces EVERY alias, so a child spreading the exported env reaches only the private instance", async () => {
    const env: NodeJS.ProcessEnv = { PATH: process.env.PATH };
    for (const name of ALIASES) env[name] = sentinel.url; // a hostile inherited environment
    const intended = await exportPrivateRedis({ root }, env);
    started.push(intended);
    await assertOnlyReaches(env, intended);
    for (const name of ALIASES) expect(env[name], name).toBe(intended.url);
  }, 30_000);

  it("a fixture with its own instance builds its child env with privateRedisEnv, overriding every inherited alias", async () => {
    for (const name of ALIASES) vi.stubEnv(name, sentinel.url); // the parent env is hostile
    const fixture = await ownedRedis.startOwnedRedis({ root });
    started.push(fixture);
    const env = ownedRedis.privateRedisEnv(process.env, fixture.url);
    await assertOnlyReaches(env, fixture);
  }, 30_000);
});

describe("privateRedisEnv", () => {
  it("covers every Redis endpoint variable the code reads", () => {
    expect([...ownedRedis.REDIS_ENDPOINT_ENV].sort()).toEqual([...ALIASES].sort());
  });

  it("sets every alias to the private URL, and refuses a TCP or foreign URL", () => {
    const url = sentinel.url;
    const env = ownedRedis.privateRedisEnv({ PATH: "/bin", GPTQUEUE_REDIS_URL: "redis://127.0.0.1:6379/0", REDIS_HOST: "127.0.0.1" }, url);
    for (const name of ALIASES) expect(env[name]).toBe(url);
    expect(env.PATH).toBe("/bin");
    expect(() => ownedRedis.privateRedisEnv({}, "redis://127.0.0.1:6379/15")).toThrow(/private unix-socket instance/u);
    expect(() => ownedRedis.privateRedisEnv({ REDIS_URL: "redis://127.0.0.1:6379/15" })).toThrow(/private unix-socket instance/u);
    expect(() => ownedRedis.privateRedisEnv({})).toThrow(/it is unset/u);
  });

  it("globalSetup left every alias in this worker pointing at the run's instance", () => {
    for (const name of ALIASES) expect(process.env[name], name).toBe(process.env.REDIS_URL);
    expect(ownedRedis.privateRedisUrl(process.env.REDIS_URL)).toBe(process.env.REDIS_URL);
  });
});
