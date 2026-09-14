import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { startRegisteredShell, shellIdentity } from "../src/registered-shell/server.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";

const URL15 = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const live: Array<{ shell: Awaited<ReturnType<typeof startRegisteredShell>>; client: Client; peer: Transport; redis: Redis }> = [];
const result = (value: unknown) => JSON.parse((value as { content: Array<{ text: string }> }).content[0].text) as any;
let logDir: string;
beforeEach(async () => {
  logDir = await mkdtemp(join(tmpdir(), "gptqueue-discovery-"));
  vi.stubEnv("GPTQ_LOG_DIR", logDir);
});

afterEach(async () => {
  for (const item of live.splice(0)) {
    await item.shell.close().catch(() => undefined);
    await item.client.close().catch(() => undefined);
    await item.peer.close().catch(() => undefined);
    const keys = [SESSION_KEYS.queue(item.shell.agentName), SESSION_KEYS.agent(item.shell.agentName), SESSION_KEYS.mailboxMeta(item.shell.agentName), SESSION_KEYS.heartbeat(item.shell.agentName), SESSION_KEYS.agentSessions(item.shell.agentName)];
    await item.redis.hdel(SESSION_KEYS.registry, item.shell.agentName);
    await item.redis.del(...keys);
    await item.redis.quit();
  }
  vi.unstubAllEnvs();
  await rm(logDir, { recursive: true, force: true });
});

const connected = async (cwd: string) => {
  const [server, peer] = InMemoryTransport.createLinkedPair();
  const shell = await startRegisteredShell({ client: "codex", redisUrl: URL15, cwd }, new AbortController().signal, () => server);
  const client = new Client({ name: "agent-discovery-test", version: "1" });
  const redis = new Redis(URL15);
  live.push({ shell, client, peer, redis });
  await client.connect(peer);
  return { shell, client, redis };
};

describe("registered agent discovery metadata", () => {
  it("returns readable identity fields in bound list_agents text and structured data", async () => {
    const { shell, client } = await connected("/tmp/Readable Workspace Ω");
    const response = await client.callTool({ name: "list_agents", arguments: {} });
    const listed = result(response);
    expect(response.structuredContent).toEqual({ status: "ok", agents: listed });
    const agent = listed.find((entry: any) => entry.name === shell.agentName);
    expect(agent.label).toBe(`Readable Workspace Ω · codex · ${agent.uuid.slice(0, 8)}`);
    expect(agent.uuid).toBe(shell.agentName.slice(-36));
    expect(agent.uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
    expect(agent.working_directory).toBe("/tmp/Readable Workspace Ω");
    expect(agent.client).toBe("codex");
    expect(agent.online).toBe(true);
    expect(agent.pid).toBe(process.pid);
    expect(agent.session_id).toBeUndefined();
    expect(JSON.stringify(listed)).not.toContain("redis://");
  });

  it("keeps same-directory shells distinct", async () => {
    const first = await connected("/tmp/same");
    const second = await connected("/tmp/same");
    expect(first.shell.agentName).not.toBe(second.shell.agentName);
    const listed = result(await first.client.callTool({ name: "list_agents", arguments: {} }));
    const ours = listed.filter((entry: any) => [first.shell.agentName, second.shell.agentName].includes(entry.name));
    expect(new Set(ours.map((entry: any) => entry.uuid)).size).toBe(2);
    expect(new Set(ours.map((entry: any) => entry.label)).size).toBe(2);
  });

  it("lists legacy registrations with explicit null metadata", async () => {
    const redis = new RedisClient(null, URL15);
    const name = `legacy-${randomUUID()}`;
    try {
    await redis.register("both", name, "legacy");
    const listed = await redis.listAgents();
    const legacy = listed.find((entry) => entry.name === name);
    expect(legacy).toMatchObject({ name, label: name, uuid: null, client: null, working_directory: null, registered_at: expect.any(String), pid: process.pid, description: "legacy", role: "both", online: true });
    } finally { await redis.unregister(); await redis.shutdown(); }
  });

  it("projects only valid public metadata and skips corrupt registry values", async () => {
    const { shell, client, redis } = await connected("/tmp/malformed");
    const stored = JSON.parse((await redis.hget(SESSION_KEYS.registry, shell.agentName))!);
    const invalidName = `invalid-${randomUUID()}`;
    try {
      await redis.hset(SESSION_KEYS.registry, shell.agentName, JSON.stringify({ ...stored,
        session_id: "secret-session", redis_url: "redis://secret",
        metadata: { label: { secret: "hidden-value" }, uuid: ["secret-uuid"], client: "invented", working_directory: "relative" },
        registered_at: "invalid", pid: -1,
      }), invalidName, "null");
      const listed = result(await client.callTool({ name: "list_agents", arguments: {} }));
      expect(listed.find((entry: any) => entry.name === shell.agentName)).toMatchObject({ label: shell.agentName, uuid: null, client: null, working_directory: null, registered_at: null, pid: null });
      expect(listed.some((entry: any) => entry.name === invalidName)).toBe(false);
      expect(JSON.stringify(listed)).not.toMatch(/secret|hidden-value/u);
    } finally { await redis.hdel(SESSION_KEYS.registry, invalidName); }
  });

  it("uses root as the readable directory label", () => {
    const identity = shellIdentity("pi", "/");
    expect(identity.label).toMatch(/^\/ · pi · [0-9a-f]{8}$/u);
    expect(identity.working_directory).toBe("/");
  });
});

describe("additive discovery tools", () => {
  it("finds declared purpose while preserving ambiguity and private credentials", async () => {
    const first = await connected("/tmp/discovery-purpose");
    const second = await connected("/tmp/discovery-purpose");
    for (const current of [first, second]) {
      await current.client.callTool({ name: "set_agent_profile", arguments: { label: "Delivery controller", purpose: "coordinate delivery audit", kind: "controller" } });
    }
    const found = result(await first.client.callTool({ name: "find_agents", arguments: { query: "delivery audit", working_directory: "/tmp/discovery-purpose", limit: 1 } }));
    expect(found.resolution).toBe("ambiguous");
    expect(found.total_matches).toBe(2);
    expect(found.truncated).toBe(true);
    expect(found.matches[0].profile.authoritative).toBe(false);
    expect(JSON.stringify(found)).not.toContain(first.shell.sessionId);
    const details = result(await first.client.callTool({ name: "get_agent_details", arguments: {} }));
    expect(details.readiness).toBe("unbound");
    expect(details.capabilities.protocol_version).toBe("2");
  });

  it("inspects a queued message without exposing content or consuming it", async () => {
    const { shell, client, redis } = await connected("/tmp/diagnostic-readonly");
    const id = randomUUID();
    const raw = JSON.stringify({ id, to: shell.agentName, from: "peer", type: "task", payload: { content: "private fixture content" } });
    await redis.rpush(SESSION_KEYS.queue(shell.agentName), raw);
    const inspected = await client.callTool({ name: "get_delivery_status", arguments: { message_id: id } });
    expect(result(inspected).status).toBe("queued");
    expect(JSON.stringify(inspected)).not.toContain("private fixture content");
    expect(await redis.lrange(SESSION_KEYS.queue(shell.agentName), 0, -1)).toEqual([raw]);
  });
});
