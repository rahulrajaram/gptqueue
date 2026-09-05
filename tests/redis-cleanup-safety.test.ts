import { describe, expect, it, vi } from "vitest";
import type { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

describe("Redis cleanup safety", () => {
  it("rejects the actual live connection even when a supplied URL says db15", async () => {
    const scan = vi.fn();
    const del = vi.fn();
    const redis = { options: { db: 0 }, scan, del } as unknown as Redis;
    await expect(flushTestKeys(redis, "redis://127.0.0.1:6379/15")).rejects.toThrow("actual Redis connection targets live db0");
    expect(scan).not.toHaveBeenCalled();
    expect(del).not.toHaveBeenCalled();
  });
  it("fails closed when actual connection database is absent", async () => {
    const scan = vi.fn();
    const redis = { options: {}, scan } as unknown as Redis;
    await expect(flushTestKeys(redis, "redis://127.0.0.1:6379/15")).rejects.toThrow("live db0");
    expect(scan).not.toHaveBeenCalled();
  });
});
