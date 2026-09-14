import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../src/core/keys.js";
import { startRegisteredShell, type RegisteredShellHandle } from "../src/registered-shell/server.js";

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const dirs: string[] = [];
const removeOwned = async (redis: Redis, agent: string) => {
  await redis.hdel(SESSION_KEYS.registry, agent);
  await redis.del(SESSION_KEYS.queue(agent), SESSION_KEYS.agent(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.heartbeat(agent), SESSION_KEYS.agentSessions(agent));
};
const dispose = async (shell: RegisteredShellHandle, client: Client, redis: Redis) => {
  try { await shell.close(); }
  finally {
    await client.close();
    await removeOwned(redis, shell.agentName);
    await redis.quit();
  }
};

const eventually = async <T>(read: () => Promise<T>, predicate: (value: T) => boolean) => {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      const value = await read();
      if (predicate(value)) return value;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return read();
};
const events = async (dir: string, agent: string, wanted?: string) => {
  const text = await eventually(() => readFile(join(dir, `${agent}.jsonl`), "utf8"), (value) => !wanted || value.includes(`"event":"${wanted}"`));
  return text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>);
};

afterEach(async () => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  await Promise.all(dirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("registered shell lifecycle logging", () => {
  it("records registration before MCP initialization and handshake after connect", async () => {
    const dir = await mkdtemp(join(tmpdir(), "gptqueue-log-")); dirs.push(dir); vi.stubEnv("GPTQ_LOG_DIR", dir);
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    const shell = await startRegisteredShell({ client: "codex", redisUrl: REDIS_URL, cwd: "/tmp/log-test" }, undefined, () => serverTransport);
    const redis = new Redis(REDIS_URL);
    const client = new Client({ name: "logging-test", version: "1" });
    try {
    const before = await events(dir, shell.agentName, "registration_complete");
    expect(before.map((event) => event.event)).toContain("registration_complete");
    expect(before.map((event) => event.event)).not.toContain("mcp_initialized");
    await client.connect(clientTransport);
    const after = await events(dir, shell.agentName, "mcp_initialized");
    expect(after.map((event) => event.event)).toContain("mcp_initialized");
    for (const event of after) {
      expect(event.schema_version).toBe(1);
      expect(typeof event.timestamp).toBe("string");
      expect(typeof event.elapsed_ms).toBe("number");
      expect(event.elapsed_ms).toBeGreaterThanOrEqual(0);
      expect(event).not.toHaveProperty("redis_url");
    }
    await client.close(); await shell.close(); await serverTransport.close();
    await shell.close();
    const ended = await events(dir, shell.agentName, "shutdown_complete");
    expect(ended.filter((event) => event.event === "shutdown_started")).toHaveLength(1);
    expect(ended.filter((event) => event.event === "shutdown_complete")).toHaveLength(1);
    expect(ended.find((event) => event.event === "shutdown_complete")?.session_id).toBe(shell.sessionId);
    expect(Number.isFinite(ended.find((event) => event.event === "shutdown_complete")?.duration_ms)).toBe(true);
    expect(ended.find((event) => event.event === "shutdown_complete")?.reason).toBe(ended.find((event) => event.event === "shutdown_started")?.reason);
    expect(ended.map((event) => event.elapsed_ms)).toEqual(ended.map((event) => event.elapsed_ms).sort((a, b) => Number(a) - Number(b)));
    expect((await stat(join(dir, `${shell.agentName}.jsonl`))).mode & 0o777).toBe(0o600);
    expect(await redis.scard(SESSION_KEYS.agentSessions(shell.agentName))).toBe(0);
    } finally {
      await dispose(shell, client, redis);
    }
  });

  it("logs a safe startup failure and retires a failed transport session", async () => {
    const dir = await mkdtemp(join(tmpdir(), "gptqueue-log-")); dirs.push(dir); vi.stubEnv("GPTQ_LOG_DIR", dir);
    const redactionFixture = "super-secret-redis-token";
    const transport: Transport = { start: async () => { throw new Error(`transport failed: ${redactionFixture}`); }, send: async () => {}, close: async () => {} };
    const redis = new Redis(REDIS_URL);
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
    let agent: string | undefined;
    try {
    await expect(startRegisteredShell({ client: "codex", redisUrl: REDIS_URL, cwd: "/tmp/failure-log" }, undefined, () => transport)).rejects.toThrow(redactionFixture);
    const files = await readdir(dir);
    expect(files).toHaveLength(1);
    const text = await readFile(join(dir, files[0]!), "utf8");
    expect(text).toContain('"event":"startup_failed"');
    expect(text).not.toContain(redactionFixture);
    expect(text).not.toContain(REDIS_URL);
    agent = files[0]!.replace(/\.jsonl$/u, "");
    const records = text.trim().split("\n").map((line) => JSON.parse(line));
    const registered = records.find((event) => event.event === "registration_complete");
    expect(records.find((event) => event.event === "startup_failed")?.phase).toBe("transport");
    expect(await redis.exists(SESSION_KEYS.lease(registered.session_id), SESSION_KEYS.session(registered.session_id))).toBe(0);
    expect(await redis.scard(SESSION_KEYS.agentSessions(agent))).toBe(0);
    expect(JSON.stringify(stderr.mock.calls)).not.toContain(redactionFixture);
    } finally {
      if (agent) await removeOwned(redis, agent);
      await redis.quit();
    }
  });

  it("keeps startup and cleanup usable when the log directory is invalid", async () => {
    const parent = await mkdtemp(join(tmpdir(), "gptqueue-log-invalid-")); dirs.push(parent);
    const file = join(parent, "log-file"); await writeFile(file, "occupied"); vi.stubEnv("GPTQ_LOG_DIR", join(file, "child"));
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
    const shell = await startRegisteredShell({ client: "codex", redisUrl: REDIS_URL, cwd: "/tmp/invalid-log" }, undefined, () => serverTransport);
    const client = new Client({ name: "invalid-log-test", version: "1" });
    const redis = new Redis(REDIS_URL);
    try {
      await client.connect(clientTransport); expect((await client.listTools()).tools.length).toBe(13);
      await shell.close();
      expect(await redis.scard(SESSION_KEYS.agentSessions(shell.agentName))).toBe(0);
      expect(await redis.exists(SESSION_KEYS.lease(shell.sessionId), SESSION_KEYS.session(shell.sessionId))).toBe(0);
    } finally { await dispose(shell, client, redis); }
  });
});
