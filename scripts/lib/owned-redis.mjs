// Start a private, throwaway Redis: binds 127.0.0.1 on a free port, persists
// nothing, and lives in a temp dir removed on stop(). Shared by
// with-owned-redis.mjs (test runner) and install-runtime.mjs (release verify).
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const freePort = () => new Promise((resolve, reject) => {
  const probe = createServer();
  probe.once("error", reject);
  probe.listen(0, "127.0.0.1", () => {
    const { port } = probe.address();
    probe.close(() => resolve(port));
  });
});

const waitForReady = (server, timeoutMs) => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error(`redis-server not ready within ${timeoutMs} ms`)), timeoutMs);
  let output = "";
  server.stdout.on("data", (chunk) => {
    output += chunk;
    if (output.includes("Ready to accept connections")) { clearTimeout(timer); resolve(); }
  });
  server.once("exit", (code) => { clearTimeout(timer); reject(new Error(`redis-server exited early (${code}): ${output.slice(-500)}`)); });
});

/** @returns {Promise<{ port: number, url: string, stop: () => void }>} url targets database 15. */
export async function startOwnedRedis({ readyTimeoutMs = 10_000, stderr = "inherit" } = {}) {
  const port = await freePort();
  const dir = mkdtempSync(join(tmpdir(), "gptqueue-owned-redis-"));
  const redis = spawn(process.env.REDIS_SERVER_BIN ?? "redis-server", [
    "--port", String(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
    "--dir", dir, "--protected-mode", "yes",
  ], { stdio: ["ignore", "pipe", stderr] });
  const stop = () => {
    if (redis.exitCode === null && redis.signalCode === null) redis.kill("SIGTERM");
    rmSync(dir, { recursive: true, force: true });
  };
  try {
    await waitForReady(redis, readyTimeoutMs);
  } catch (error) {
    stop();
    throw error;
  }
  redis.stdout.resume();
  return Object.freeze({ port, url: `redis://127.0.0.1:${port}/15`, stop });
}
