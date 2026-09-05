import { defineConfig } from "vitest/config";

// Fail closed before collecting tests, including when a runner strips caller env.
const testRedisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
if (new URL(testRedisUrl).pathname !== "/15") {
  throw new Error("GPTQueue tests require isolated Redis database 15");
}
process.env.REDIS_URL = testRedisUrl;

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 15_000,
    hookTimeout: 10_000,
    fileParallelism: false,
  },
});
