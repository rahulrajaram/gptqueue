import { describe, expect, it, vi } from "vitest";
import type { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { testRedisTarget, testRedisUrl } from "./helpers/test-redis.js";

const fakeClient = (options: Record<string, unknown>) => {
  const scan = vi.fn(async () => ["0", []]);
  const del = vi.fn();
  return { redis: { options, scan, del } as unknown as Redis, scan, del };
};

describe("Redis cleanup safety", () => {
  it("refuses every TCP URL, including the production server, before touching the client", async () => {
    for (const url of ["redis://127.0.0.1:6379/15", "redis://127.0.0.1:6379/0", "redis://127.0.0.1:6379", "rediss://127.0.0.1:6379/15"]) {
      const { redis, scan, del } = fakeClient({ path: testRedisTarget().path, db: 15 });
      await expect(flushTestKeys(redis, url)).rejects.toThrow(/private unix-socket instance/u);
      expect(scan).not.toHaveBeenCalled();
      expect(del).not.toHaveBeenCalled();
    }
  });

  it("rejects a client whose actual connection is not the private socket, even when the URL is", async () => {
    for (const options of [{ host: "127.0.0.1", port: 6379, db: 15 }, { db: 15 }, { path: "/tmp/other/redis.sock", db: 15 }]) {
      const { redis, scan } = fakeClient(options);
      await expect(flushTestKeys(redis, testRedisUrl())).rejects.toThrow("not connected to the run's private test Redis");
      expect(scan).not.toHaveBeenCalled();
    }
  });

  it("rejects a client on another database of the private instance (including db0)", async () => {
    for (const db of [0, 14, undefined]) {
      const { redis, scan } = fakeClient({ path: testRedisTarget().path, db });
      await expect(flushTestKeys(redis, testRedisUrl())).rejects.toThrow("not connected to the run's private test Redis");
      expect(scan).not.toHaveBeenCalled();
    }
  });

  it("scans only gptq:* for the private socket client", async () => {
    const { redis, scan } = fakeClient({ path: testRedisTarget().path, db: 15 });
    await flushTestKeys(redis, testRedisUrl());
    expect(scan).toHaveBeenCalledWith("0", "MATCH", "gptq:*", "COUNT", 500);
  });
});
