import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const SCRIPT = resolve(import.meta.dirname, "../scripts/check-queue.sh");
const hasRedisCli = spawnSync("sh", ["-c", "command -v redis-cli"]).status === 0;

describe.skipIf(!hasRedisCli)("check-queue.sh stop gate", () => {
  let redis: Redis;
  const agent = `stopgate-${randomUUID()}`;
  beforeAll(() => { redis = new Redis(TEST_REDIS_URL); });
  afterAll(async () => { await redis.del(SESSION_KEYS.queue(agent)); await redis.quit(); });

  const run = (env: Record<string, string>, arg = "--stop") =>
    spawnSync("bash", [SCRIPT, arg], { env: { PATH: process.env.PATH ?? "", GPTQ_AGENT_NAME: agent, ...env }, encoding: "utf8" });

  it("blocks stop while the inbox in REDIS_URL's database has messages", async () => {
    await redis.rpush(SESSION_KEYS.queue(agent), "m1", "m2");
    const out = run({ REDIS_URL: TEST_REDIS_URL });
    expect(JSON.parse(out.stdout)).toMatchObject({ decision: "block" });
    expect(out.stdout).toContain("2 pending");
  });

  it("allows stop for an empty inbox, and warns instead of blocking when Redis is unreachable", async () => {
    await redis.del(SESSION_KEYS.queue(agent));
    expect(JSON.parse(run({ REDIS_URL: TEST_REDIS_URL }).stdout)).toEqual({});
    const down = run({ REDIS_URL: "redis://127.0.0.1:1/15" });
    expect(JSON.parse(down.stdout)).toEqual({});
    expect(down.stderr).toContain("queue check failed");
  });

  it("rejects unknown arguments", () => {
    expect(run({}, "--bogus").status).toBe(1);
  });
});
