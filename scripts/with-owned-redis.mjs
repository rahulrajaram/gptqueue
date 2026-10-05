#!/usr/bin/env node
// Run a command against a private, throwaway Redis instead of a shared one.
// The server binds 127.0.0.1 on a free port, persists nothing, and is stopped
// when the command exits. The child gets REDIS_URL=redis://127.0.0.1:<port>/15.
//
//   node scripts/with-owned-redis.mjs -- npx vitest run tests/foo.test.ts
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const separator = process.argv.indexOf("--");
const command = separator === -1 ? [] : process.argv.slice(separator + 1);
if (command.length === 0) {
  console.error("usage: with-owned-redis.mjs -- <command> [args...]");
  process.exit(2);
}

const freePort = () => new Promise((resolve, reject) => {
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

const port = await freePort();
const dir = mkdtempSync(join(tmpdir(), "gptqueue-owned-redis-"));
const redis = spawn(process.env.REDIS_SERVER_BIN ?? "redis-server", [
  "--port", String(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
  "--dir", dir, "--protected-mode", "yes",
], { stdio: ["ignore", "pipe", "inherit"] });

const stopRedis = () => { if (redis.exitCode === null) redis.kill("SIGTERM"); };
const cleanup = () => { stopRedis(); rmSync(dir, { recursive: true, force: true }); };

try {
  await waitForReady(redis, 10_000);
} catch (error) {
  console.error(String(error));
  cleanup();
  process.exit(1);
}

// The command runs in its own process group so a signal reaches every
// descendant (npx -> sh -> vitest -> workers), not just the direct child.
const child = spawn(command[0], command.slice(1), {
  stdio: "inherit",
  detached: true,
  env: { ...process.env, REDIS_URL: `redis://127.0.0.1:${port}/15` },
});
const signalGroup = (signal) => { try { process.kill(-child.pid, signal); } catch { /* group already gone */ } };
const forward = (signal) => () => {
  signalGroup(signal);
  // Escalate if the group ignores the signal, then stop Redis regardless.
  setTimeout(() => { signalGroup("SIGKILL"); cleanup(); process.exit(1); }, 5_000).unref();
};
process.on("SIGINT", forward("SIGINT"));
process.on("SIGTERM", forward("SIGTERM"));
process.on("SIGHUP", forward("SIGHUP"));
child.once("exit", (code, signal) => {
  // Reap any descendants the command left behind before stopping Redis.
  signalGroup("SIGTERM");
  cleanup();
  process.exit(code ?? (signal ? 1 : 0));
});
