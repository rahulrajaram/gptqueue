import { privateRedisEnv, REDIS_ENDPOINT_ENV } from "./helpers/test-redis.js";
import { testRedisUrl } from "./helpers/test-redis.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { execSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = testRedisUrl();
const ROOT = resolve(import.meta.dirname, "..");

describe("stdio server lifecycle", () => {
  let redis: Redis;
  beforeAll(() => {
    execSync("npm run build", { cwd: ROOT, stdio: "pipe" });
    redis = new Redis(TEST_REDIS_URL);
  }, 120_000);
  afterAll(async () => {
    await redis.quit();
  });

  it("exits when its client closes stdin instead of staying leased forever", async () => {
    const agent = `stdio-orphan-${randomUUID()}`;
    const child = spawn(process.execPath, ["dist/mcp-server/index.js", agent], {
      cwd: ROOT, env: privateRedisEnv(process.env, TEST_REDIS_URL), stdio: ["pipe", "ignore", "pipe"],
    });
    const exited = new Promise<number | null>((done) => child.on("exit", (code) => done(code)));
    try {
      for (let i = 0; i < 2400 && !(await redis.hexists(SESSION_KEYS.registry, agent)); i += 1) { // up to 120 s: module loads from a contended mount can be slow
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(await redis.hexists(SESSION_KEYS.registry, agent)).toBe(1);
      child.stdin!.end();
      const code = await Promise.race([exited, new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 8_000))]);
      expect(code).toBe(0);
      // A clean exit closes the session instead of leaving it to the lease TTL,
      // so a restarted client can take the name back at once.
      expect(await redis.scard(SESSION_KEYS.agentSessions(agent))).toBe(0);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await redis.hdel(SESSION_KEYS.registry, agent);
      await redis.del(SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent), SESSION_KEYS.agentSessions(agent));
    }
  }, 180_000);

  it("GQ-001: a final startup registration failure logs startup_registration_failed and exits non-zero, not an unhandled crash", async () => {
    const agent = `held-startup-${randomUUID()}`;
    const holder = new RedisClient(null, TEST_REDIS_URL);
    try {
      await holder.register("both", agent, "holder of the name");
      const child = spawn(process.execPath, ["dist/mcp-server/index.js", agent], {
        cwd: ROOT, env: privateRedisEnv(process.env, TEST_REDIS_URL), stdio: ["ignore", "ignore", "pipe"],
      });
      let stderr = "";
      child.stderr!.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
      try {
        const exited = new Promise<number | null>((done) => child.on("exit", (code) => done(code)));
        const code = await Promise.race([exited, new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 90_000))]);
        // Fail fast, but cleanly: a logged, non-zero exit.
        expect(code).toBe(1);
        // The typed startup failure event must be logged. Before GQ-001 the
        // registration was awaited at top level before the .catch could
        // attach, so module evaluation crashed and this line never ran.
        expect(stderr).toContain("startup_registration_failed");
        expect(stderr).toContain("AGENT_NAME_HELD");
        // A clean exit prints the event, not an unhandled crash stack.
        expect(stderr).not.toMatch(/\n\s+at \S+\s+\(/);
      } finally {
        if (child.exitCode === null) child.kill("SIGKILL");
      }
    } finally {
      await holder.closeCurrentSession().catch(() => undefined);
      await holder.shutdown();
      await redis.hdel(SESSION_KEYS.registry, agent);
      await redis.del(SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent), SESSION_KEYS.agentSessions(agent));
    }
  }, 120_000);

  it("GQ-001: a transient registration failure (Redis unavailable) retries in the background instead of crashing", async () => {
    // Point the child's Redis endpoint vars at a dead TCP port: ECONNREFUSED
    // maps to REDIS_UNAVAILABLE (retryable), the BC-F1 transient case.
    const deadRedis = "redis://127.0.0.1:9/0";
    const env = privateRedisEnv(process.env);
    for (const key of REDIS_ENDPOINT_ENV) if (typeof env[key] === "string") env[key] = deadRedis;
    const child = spawn(process.execPath, ["dist/mcp-server/index.js", "transient-startup-agent"], {
      cwd: ROOT, env, stdio: ["ignore", "ignore", "pipe"],
    });
    let stderr = "";
    child.stderr!.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    try {
      const deadline = Date.now() + 60_000;
      while (!stderr.includes("startup_registration_retry") && child.exitCode === null && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
      }
      // The retry loop keeps the process alive; before GQ-001 the awaited
      // rejection crashed module evaluation and the process died instead.
      expect(child.exitCode).toBeNull();
      expect(stderr).toContain("startup_registration_retry");
    } finally {
      if (child.exitCode === null) {
        const reaped = new Promise<void>((resolve) => child.once("exit", () => resolve()));
        child.kill("SIGKILL");
        await reaped;
      }
    }
  }, 120_000);

  it("RedisClient.shutdown returns at its deadline and force-disconnects when QUIT hangs", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    const internals = client as unknown as { redis: { quit: () => Promise<unknown> }; connectionsForcedClosed: boolean };
    internals.redis.quit = () => new Promise(() => undefined);
    const started = Date.now();
    await client.shutdown(100);
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(internals.connectionsForcedClosed).toBe(true);
  });
});
