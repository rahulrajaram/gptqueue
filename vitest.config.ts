import { defineConfig } from "vitest/config";

// Redis: every run gets its own private, socket-only redis-server from
// tests/helpers/global-setup.ts (scripts/lib/owned-redis.mjs). It replaces any
// inherited REDIS_URL before workers start; tests read it only through
// tests/helpers/test-redis.ts, which refuses every TCP URL.

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
    globalSetup: ["./tests/helpers/global-setup.ts"],
    testTimeout: 15_000,
    hookTimeout: 10_000,
    fileParallelism: false,
  },
});
