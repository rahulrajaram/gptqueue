import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { execSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
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
      cwd: ROOT, env: { ...process.env, REDIS_URL: TEST_REDIS_URL }, stdio: ["pipe", "ignore", "pipe"],
    });
    const exited = new Promise<number | null>((done) => child.on("exit", (code) => done(code)));
    try {
      for (let i = 0; i < 100 && !(await redis.hexists(SESSION_KEYS.registry, agent)); i += 1) {
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(await redis.hexists(SESSION_KEYS.registry, agent)).toBe(1);
      child.stdin!.end();
      const code = await Promise.race([exited, new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 8_000))]);
      expect(code).toBe(0);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await redis.hdel(SESSION_KEYS.registry, agent);
      await redis.del(SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent));
    }
  }, 20_000);

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
