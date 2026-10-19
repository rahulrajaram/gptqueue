import { afterEach, describe, expect, it, vi } from "vitest";
import { Redis } from "ioredis";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { privateRedisUrl } from "../../scripts/lib/owned-redis.mjs";

describe("owned Redis fixture", () => {
  let owned: OwnedRedis | undefined;
  afterEach(async () => { await owned?.close(); owned = undefined; vi.unstubAllEnvs(); });

  it("starts a private socket-only DB15 instance and cleans it up", async () => {
    owned = await startOwnedRedis();
    expect(owned.url).toBe(`${owned.socket}?db=15`);
    expect(privateRedisUrl(owned.url)).toBe(owned.url);
    const redis = new Redis(owned.url);
    try {
      expect(await redis.set("gptq:owned-fixture", "ok")).toBe("OK");
      expect(await redis.get("gptq:owned-fixture")).toBe("ok");
    } finally { await redis.quit(); }
  });

  it("fails closed when the owned server cannot be spawned", async () => {
    vi.stubEnv("GPTQ_TEST_REDIS_SERVER", "/definitely/missing/redis-server");
    await expect(startOwnedRedis()).rejects.toThrow(/spawn failed|exited before readiness/u);
  });
});
