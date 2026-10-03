import { afterEach, beforeAll, afterAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { RedisWatcher } from "../src/pty-wrapper/redis-watcher.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

describe("PTY RedisWatcher", () => {
  let redis: Redis;
  let originalFlags = "";
  const watchers: RedisWatcher[] = [];
  const agents: string[] = [];

  beforeAll(async () => {
    redis = new Redis(TEST_REDIS_URL);
    [, originalFlags = ""] = (await redis.config("GET", "notify-keyspace-events")) as string[];
  });

  afterEach(async () => {
    for (const w of watchers.splice(0)) await w.stop().catch(() => undefined);
    for (const a of agents.splice(0)) await redis.del(SESSION_KEYS.queue(a));
    await redis.config("SET", "notify-keyspace-events", originalFlags);
  });

  afterAll(async () => {
    await redis.quit();
  });

  const watch = async (): Promise<{ agent: string; events: number[] }> => {
    const agent = `pty-watch-${randomUUID()}`;
    agents.push(agent);
    const watcher = new RedisWatcher(agent, TEST_REDIS_URL);
    watchers.push(watcher);
    const events: number[] = [];
    watcher.on("message", (n: number) => events.push(n));
    await watcher.start();
    return { agent, events };
  };

  const until = async (check: () => boolean): Promise<void> => {
    for (let i = 0; i < 100 && !check(); i += 1) await new Promise((r) => setTimeout(r, 20));
    expect(check()).toBe(true);
  };

  it("emits on a push to a queue in the database the URL selects (not db 0)", async () => {
    const { agent, events } = await watch();
    expect(events).toEqual([]);
    await redis.rpush(SESSION_KEYS.queue(agent), JSON.stringify({ id: "m1" }));
    await until(() => events.length > 0);
    expect(events[0]).toBe(1);
  });

  it("adds the list keyspace flags without dropping ones already configured", async () => {
    await redis.config("SET", "notify-keyspace-events", "Ex");
    await watch();
    const [, flags = ""] = (await redis.config("GET", "notify-keyspace-events")) as string[];
    for (const flag of ["E", "x", "K", "l"]) expect(flags).toContain(flag);
  });
});
