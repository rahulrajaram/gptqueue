import { spawn } from "node:child_process";
import { createServer, type Socket } from "node:net";
import { resolve } from "node:path";
import { Redis } from "ioredis";
import { describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";

const launch = (url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15") => {
  const child = spawn(process.execPath, [resolve("bin/gptqueue-session"), "--client", "codex", "--redis-url", url], { stdio: "pipe" });
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

describe("registered stdio process lifetime", () => {
  it.each([{ event: "EOF", code: 0 }, { event: "SIGINT", code: 130 }, { event: "SIGTERM", code: 143 }] as const)(
    "retires its session on $event and exits $code", async ({ event, code }) => {
      const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15");
      const job = launch(); let name: string | undefined;
      try {
        name = await job.ready;
        const sessions = await redis.smembers(SESSION_KEYS.agentSessions(name));
        expect(sessions).toHaveLength(1);
        if (event === "EOF") job.child.stdin.end(); else job.child.kill(event);
        expect(await job.done).toEqual({ code, signal: null });
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
    }
  );
  it("honors SIGTERM while Redis startup is blocked", async () => {
    const sockets = new Set<Socket>(); let connected!: () => void;
    const accepted = new Promise<void>((resolve) => { connected = resolve; });
    const server = createServer((socket) => { sockets.add(socket); socket.resume(); connected(); });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address(); if (!address || typeof address === "string") throw new Error("No port");
    const job = launch(`redis://127.0.0.1:${address.port}/15`);
    try {
      await accepted; job.child.kill("SIGTERM");
      expect(await job.done).toEqual({ code: 143, signal: null });
    } finally {
      job.child.kill("SIGKILL"); for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
