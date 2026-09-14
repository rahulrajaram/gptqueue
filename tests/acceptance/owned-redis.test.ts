import { afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";

describe("owned Redis fixture", () => {
  let owned: OwnedRedis | undefined;
  afterEach(async () => { await owned?.close(); owned = undefined; });

  it("starts an isolated loopback DB15 instance and cleans it up", async () => {
    owned = await startOwnedRedis();
    expect(owned.url).toMatch(/^redis:\/\/127\.0\.0\.1:\d+\/15$/);
    const redis = new Redis(owned.url);
    try {
      expect(await redis.set("gptq:owned-fixture", "ok")).toBe("OK");
      expect(await redis.get("gptq:owned-fixture")).toBe("ok");
    } finally { await redis.quit(); }
  });

  it("fails closed when the owned server cannot be spawned", async () => {
    const previous = process.env.REDIS_SERVER_BIN;
    process.env.REDIS_SERVER_BIN = "/definitely/missing/redis-server";
    try { await expect(startOwnedRedis()).rejects.toThrow(/spawn failed|exited before readiness/u); }
    finally {
      if (previous === undefined) delete process.env.REDIS_SERVER_BIN;
      else process.env.REDIS_SERVER_BIN = previous;
    }
  });
});
