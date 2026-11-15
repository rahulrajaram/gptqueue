import { testRedisUrl } from "./helpers/test-redis.js";
import { spawn } from "node:child_process";
import { createServer, type Socket } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Redis } from "ioredis";
import { describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";

const launch = (url = testRedisUrl()) => {
  const child = spawn(process.execPath, [resolve("bin/gptqueue-session"), "--client", "codex", "--redis-url", url],
    { stdio: "pipe", env: { ...process.env, GPTQUEUE_WORKER: "0" } });
  let stderr = "";
  const done = new Promise<{ code: number | null; signal: string | null }>((resolve) => child.on("close", (code, signal) => resolve({ code, signal })));
  const ready = new Promise<string>((resolve, reject) => {
    child.stderr.on("data", (data) => {
      stderr += data;
      const lines = stderr.split("\n");
      let name: string | undefined;
      for (const line of lines) {
        try {
          const event = JSON.parse(line) as { event?: string; agent_name?: string };
          if (event.event === "transport_connected" && event.agent_name) name = event.agent_name;
        } catch { /* stderr may contain human-readable diagnostics */ }
      }
      if (name) resolve(name);
    });
    child.on("error", reject);
    child.on("close", () => reject(new Error(`Child exited before readiness: ${stderr}`)));
  });
  // Failure tests deliberately exit before registration; observe that promise too.
  void ready.catch(() => undefined);
  return { child, done, ready };
};

// The pass conditions are events (the child's `transport_connected` line, then
// its exit). The per-test timeout is only an upper bound for a hung child: a
// cold `gptqueue-session` start plus retirement takes a few seconds alone but
// can exceed vitest's 15 s default under parallel suite load.
const LIFETIME_UPPER_BOUND_MS = 90_000;

describe("registered stdio process lifetime", () => {
  it.each([{ event: "EOF", code: 0 }, { event: "SIGINT", code: 130 }, { event: "SIGTERM", code: 143 }] as const)(
    "retires its session on $event and exits $code", async ({ event, code }) => {
      const redis = new Redis(testRedisUrl());
      const started = performance.now();
      const job = launch(); let name: string | undefined;
      try {
        name = await job.ready;
        const readyMs = Math.round(performance.now() - started);
        const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
        expect(sessions).toHaveLength(1);
        const stopping = performance.now();
        if (event === "EOF") job.child.stdin.end(); else job.child.kill(event);
        const exit = await job.done;
        // Phase timings make a slow start or a slow retirement visible in the log.
        console.info(JSON.stringify({ test: "registered-shell-lifetime", event, ready_ms: readyMs, retire_ms: Math.round(performance.now() - stopping) }));
        expect(exit).toEqual({ code, signal: null });
        expect(await redis.smembers(SESSION_KEYS.agentSessions(name))).toEqual([]);
        expect(await redis.exists(SESSION_KEYS.lease(sessions[0]!))).toBe(0);
        expect(await redis.exists(SESSION_KEYS.session(sessions[0]!))).toBe(0);
      } finally {
        job.child.kill("SIGKILL");
        if (name) {
          await redis.hdel(SESSION_KEYS.registry, name);
          await redis.del(SESSION_KEYS.agent(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name), SESSION_KEYS.agentSessions(name));
        }
        await redis.quit();
      }
    }, LIFETIME_UPPER_BOUND_MS
  );
  it("honors SIGTERM while Redis startup is blocked", { timeout: LIFETIME_UPPER_BOUND_MS }, async () => {
    // A silent listener on a private unix socket (no TCP): accepts, never answers.
    const dir = mkdtempSync(join(tmpdir(), "gptq-blocked-redis-"));
    const path = join(dir, "blocked.sock");
    const sockets = new Set<Socket>(); let connected!: () => void;
    const accepted = new Promise<void>((resolve) => { connected = resolve; });
    const server = createServer((socket) => { sockets.add(socket); socket.resume(); connected(); });
    await new Promise<void>((resolve) => server.listen(path, resolve));
    const job = launch(`${path}?db=15`);
    try {
      await accepted; job.child.kill("SIGTERM");
      expect(await job.done).toEqual({ code: 143, signal: null });
    } finally {
      job.child.kill("SIGKILL"); for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
