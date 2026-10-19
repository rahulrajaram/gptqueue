/**
 * Loop adapter S1: the Claude hook records turn state as a side effect.
 * Spawns the real built gptqueue-claude-hook; this test process plays the
 * Claude process whose session record names the agent.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { publishSessionRecord, readProcess } from "../src/claude/session-record.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { TurnStateStore } from "../src/core/turn-state.js";
import { assertPrivateTestRedis } from "./helpers/redis-test-utils.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "claude-turn-state");
const BIN = resolve(__dirname, "..", "bin");

let home = "";
let env: NodeJS.ProcessEnv = {};
let redis: Redis;
const clients: RedisClient[] = [];

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-claude-turn-"));
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off", GPTQ_CLAUDE_WATCH: "off" };
  redis = new Redis(REDIS_URL);
});
afterAll(async () => {
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  publishSessionRecord(readProcess(process.pid)!, null, null, env);
  redis.disconnect();
  await rm(home, { recursive: true, force: true });
});

const hook = (event: string, json: Record<string, unknown>) =>
  new Promise<{ stdout: string; code: number | null }>((done) => {
    const child = spawn(join(BIN, "gptqueue-claude-hook"), [event], { env, stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    child.stdout.on("data", (d) => { stdout += d; });
    child.on("close", (code) => done({ stdout: stdout.trim(), code }));
    child.stdin.end(JSON.stringify({ cwd: home, ...json }));
  });

describe("Claude hook turn state", { timeout: 120_000 }, () => {
  it("records running on a prompt and idle on an allowed stop, printing nothing for the prompt", async () => {
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const { name, session_id } = await client.register("both", `claude-turn-${randomUUID().slice(0, 8)}`);
    publishSessionRecord(readProcess(process.pid)!, name, session_id, env);
    const claudeSession = randomUUID();
    try {
      const prompt = await hook("user-prompt-submit", { session_id: claudeSession, prompt: "hello" });
      expect(prompt).toEqual({ stdout: "", code: 0 });
      expect(await new TurnStateStore(redis).read(name)).toMatchObject({ phase: "running", runtime: "claude", session_id: claudeSession });

      const stop = await hook("stop", { session_id: claudeSession });
      expect(stop.code).toBe(0);
      expect(await new TurnStateStore(redis).read(name)).toMatchObject({ phase: "idle", session_id: claudeSession });
    } finally {
      await redis.del(SESSION_KEYS.turnState(name));
    }
  });

  it("records nothing for an unconfirmed identity", async () => {
    publishSessionRecord(readProcess(process.pid)!, null, null, env);
    const prompt = await hook("user-prompt-submit", { session_id: randomUUID() });
    expect(prompt).toEqual({ stdout: "", code: 0 });
    // The derived fallback name is claude-<basename of cwd>; it must not gain a turn state.
    const derived = `claude-${home.split("/").at(-1)}`;
    expect(await redis.exists(SESSION_KEYS.turnState(derived))).toBe(0);
  });
});
