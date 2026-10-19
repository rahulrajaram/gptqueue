#!/usr/bin/env node
// Run a command against a private, throwaway Redis instead of a shared one.
// The server listens only on a unix socket in a 0700 run directory (no TCP),
// is verified private before use, persists nothing, and is stopped when the
// command exits. The child gets REDIS_URL=<runDir>/redis.sock?db=15
// (scripts/lib/owned-redis.mjs). A vitest command starts its own instance in
// globalSetup regardless; this wrapper serves non-vitest commands.
//
//   node scripts/with-owned-redis.mjs -- <command> [args...]
import { spawn } from "node:child_process";
import { startOwnedRedis } from "./lib/owned-redis.mjs";

const separator = process.argv.indexOf("--");
const command = separator === -1 ? [] : process.argv.slice(separator + 1);
if (command.length === 0) {
  console.error("usage: with-owned-redis.mjs -- <command> [args...]");
  process.exit(2);
}

let redis;
try {
  redis = await startOwnedRedis();
} catch (error) {
  console.error(String(error));
  process.exit(1);
}
const cleanup = () => redis.stop();

// The command runs in its own process group so a signal reaches every
// descendant (npx -> sh -> vitest -> workers), not just the direct child.
const child = spawn(command[0], command.slice(1), {
  stdio: "inherit",
  detached: true,
  env: { ...process.env, REDIS_URL: redis.url },
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
// A command that cannot be spawned (ENOENT, EACCES) emits only "error".
child.once("error", (error) => {
  console.error(`with-owned-redis: cannot run ${command[0]}: ${error.message}`);
  cleanup();
  process.exit(127);
});
child.once("exit", (code, signal) => {
  // Reap any descendants the command left behind before stopping Redis.
  signalGroup("SIGTERM");
  cleanup();
  process.exit(code ?? (signal ? 1 : 0));
});
