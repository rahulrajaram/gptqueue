import { defineConfig } from "vitest/config";

// Fail closed before collecting tests, including when a runner strips caller env.
const testRedisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
if (new URL(testRedisUrl).pathname !== "/15") {
  throw new Error("GPTQueue tests require isolated Redis database 15");
}
process.env.REDIS_URL = testRedisUrl;

// Hermetic worker mode: GPTQUEUE_WORKER=1 (exported by headless-worker
// wrappers) or CLAUDE_CODE_ENTRYPOINT=sdk-cli (a `claude -p` caller) would put
// every child a test spawns with the inherited env into headless worker mode
// (src/claude/worker-mode.ts), silently skipping registration and hook work.
// Drop both before any worker or child inherits them; the worker-mode tests
// set them explicitly.
delete process.env.GPTQUEUE_WORKER;
delete process.env.CLAUDE_CODE_ENTRYPOINT;

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 15_000,
    hookTimeout: 10_000,
    fileParallelism: false,
  },
});
