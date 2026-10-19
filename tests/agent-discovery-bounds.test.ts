import { testRedisUrl } from "./helpers/test-redis.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { startRegisteredShell } from "../src/registered-shell/server.js";

/** Bounded, filtered discovery: list_agents and find_agents never return the whole registry by default. */
const URL15 = testRedisUrl();
const SEEDED = 620;
const prefix = `bounds-${randomUUID().slice(0, 8)}-`;
const names = Array.from({ length: SEEDED }, (_, i) => `${prefix}${String(i).padStart(4, "0")}`);
const online = names.filter((_, i) => i % 100 === 7);

type Listed = { content: Array<{ text: string }>; structuredContent: Record<string, any>; isError?: boolean };
let shell: Awaited<ReturnType<typeof startRegisteredShell>>;
let client: Client; let peer: Transport; let redis: Redis; let logDir: string;
const call = async (name: string, args: Record<string, unknown>) => await client.callTool({ name, arguments: args }) as unknown as Listed;
const legacy = (result: Listed) => JSON.parse(result.content[0]!.text) as Array<{ name: string }>;

beforeAll(async () => {
  logDir = await mkdtemp(join(tmpdir(), "gptqueue-discovery-bounds-"));
  vi.stubEnv("GPTQ_LOG_DIR", logDir);
  redis = new Redis(URL15);
  const registered_at = new Date().toISOString();
  await redis.hset(SESSION_KEYS.registry, Object.fromEntries(names.map((name) =>
    [name, JSON.stringify({ role: "worker", registered_at, pid: 4242, metadata: { label: name, client: "pi", working_directory: "/tmp/bounds" } })])));
  if (online.length) await redis.mset(Object.fromEntries(online.map((name) => [SESSION_KEYS.heartbeat(name), "1"])));
  const [server, clientSide] = InMemoryTransport.createLinkedPair();
  peer = clientSide;
  shell = await startRegisteredShell({ client: "codex", redisUrl: URL15, cwd: "/tmp/bounds-caller" }, new AbortController().signal, () => server);
  client = new Client({ name: "agent-discovery-bounds-test", version: "1" });
  await client.connect(peer);
}, 30_000);

afterAll(async () => {
  await shell?.close().catch(() => undefined);
  await client?.close().catch(() => undefined);
  await peer?.close().catch(() => undefined);
  await redis.hdel(SESSION_KEYS.registry, ...names, shell.agentName);
  await redis.del(...online.map((name) => SESSION_KEYS.heartbeat(name)));
  await redis.quit();
  vi.unstubAllEnvs();
  await rm(logDir, { recursive: true, force: true });
});

describe("bounded list_agents", () => {
  it("returns at most 50 compact entries by default with 600+ registered, and says more exist", async () => {
    const result = await call("list_agents", {});
    expect(result.isError).toBeFalsy();
    const page = legacy(result);
    expect(Array.isArray(page)).toBe(true);
    expect(page).toHaveLength(50);
    expect(result.structuredContent).toMatchObject({ status: "ok", truncated: true, next_cursor: "50" });
    expect(result.structuredContent.agents).toEqual(page);
    expect(result.structuredContent.total).toBeGreaterThanOrEqual(SEEDED);
    expect(result.content[1]?.text).toMatch(/more agents match.*cursor: "50"/u);
    expect(Buffer.byteLength(result.content[0]!.text)).toBeLessThan(40_000);
  });

  it("filters by name_prefix and pages with cursor until next_cursor is null", async () => {
    const first = await call("list_agents", { name_prefix: prefix, limit: 500 });
    expect(legacy(first)).toHaveLength(500);
    expect(first.structuredContent).toMatchObject({ total: SEEDED, truncated: true, next_cursor: "500" });
    const second = await call("list_agents", { name_prefix: prefix, limit: 500, cursor: first.structuredContent.next_cursor });
    expect(legacy(second)).toHaveLength(SEEDED - 500);
    expect(second.structuredContent).toMatchObject({ total: SEEDED, truncated: false, next_cursor: null });
    expect(second.content).toHaveLength(1);
    expect([...legacy(first), ...legacy(second)].map((agent) => agent.name)).toEqual(names);
  });

  it("accepts offset as the numeric form of cursor", async () => {
    const byOffset = await call("list_agents", { name_prefix: prefix, limit: 3, offset: 10 });
    expect(legacy(byOffset).map((agent) => agent.name)).toEqual(names.slice(10, 13));
    expect(byOffset.structuredContent.next_cursor).toBe("13");
  });

  it("keeps only online agents when online_only is set", async () => {
    const result = await call("list_agents", { name_prefix: prefix, online_only: true });
    expect(legacy(result).map((agent) => agent.name)).toEqual(online);
    expect(legacy(result).every((agent: any) => agent.online === true)).toBe(true);
    expect(result.structuredContent).toMatchObject({ total: online.length, truncated: false, next_cursor: null });
  });

  it("rejects a limit above 500 and a malformed cursor", async () => {
    expect((await call("list_agents", { limit: 501 })).isError).toBe(true);
    expect((await call("list_agents", { cursor: "not-a-number" })).isError).toBe(true);
  });
});

describe("bounded find_agents", () => {
  it("returns at most 50 matches by default with a next_cursor", async () => {
    const result = await call("find_agents", { name_prefix: prefix });
    expect(result.isError).toBeFalsy();
    expect(result.structuredContent.matches).toHaveLength(50);
    expect(result.structuredContent).toMatchObject({ total_matches: SEEDED, truncated: true, next_cursor: "50" });
    expect(result.structuredContent.matches.map((match: any) => match.name)).toEqual(names.slice(0, 50));
  });

  it("pages with cursor and offset, and accepts a limit up to 500", async () => {
    const paged = await call("find_agents", { name_prefix: prefix, limit: 5, cursor: "615" });
    expect(paged.structuredContent.matches.map((match: any) => match.name)).toEqual(names.slice(615));
    expect(paged.structuredContent).toMatchObject({ truncated: false, next_cursor: null });
    const byOffset = await call("find_agents", { name_prefix: prefix, limit: 2, offset: 4 });
    expect(byOffset.structuredContent.matches.map((match: any) => match.name)).toEqual(names.slice(4, 6));
    expect((await call("find_agents", { name_prefix: prefix, limit: 500 })).structuredContent.matches).toHaveLength(500);
    expect((await call("find_agents", { limit: 501 })).isError).toBe(true);
  }, 30_000);

  it("keeps only online agents when online_only is set", async () => {
    const result = await call("find_agents", { name_prefix: prefix, online_only: true });
    expect(result.structuredContent.matches.map((match: any) => match.name)).toEqual(online);
    expect(result.structuredContent).toMatchObject({ total_matches: online.length, truncated: false, next_cursor: null });
  });
});

describe("corrupt registrations are excluded before paging", () => {
  it("a corrupt registration sorting first does not consume the page or the total", async () => {
    const corrupt = `${prefix}!corrupt`;
    expect(corrupt < names[0]!).toBe(true);
    await redis.hset(SESSION_KEYS.registry, corrupt, "{not json");
    try {
      const listed = await call("list_agents", { name_prefix: prefix, limit: 1 });
      expect(legacy(listed).map((agent) => agent.name)).toEqual([names[0]]);
      expect(listed.structuredContent).toMatchObject({ total: SEEDED, truncated: true, next_cursor: "1" });
    } finally { await redis.hdel(SESSION_KEYS.registry, corrupt); }
  });
});
