/**
 * Registration audit stream and the read-only `gptqueue-doctor registrations`
 * query (private test Redis, db 15).
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SessionStore } from "../src/core/session-store.js";
import { runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { AdoptionRefusedError } from "../src/core/adoption-refusal.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { sessionTag } from "../src/core/session-tag.js";
import { auditClaudeSession, auditRuntime, coverageOf, parseRegistrationsArgs, queryRegistrations } from "../src/core/registration-audit.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const ROOT = resolve(__dirname, "..");
const BIN = join(ROOT, "bin");
const AUDIT = SESSION_KEYS.registrationAudit;
let redis: Redis;
let home = "";
const clients: RedisClient[] = [];
const children: ChildProcess[] = [];
const scratchKeys: string[] = [];

beforeAll(async () => {
  redis = new Redis(REDIS_URL);
  home = await mkdtemp(join(tmpdir(), "gptq-reg-audit-"));
});
afterEach(async () => {
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  if (scratchKeys.length) await redis.del(...scratchKeys.splice(0));
});
afterAll(async () => { redis.disconnect(); await rm(home, { recursive: true, force: true }); });

type Fields = Record<string, string>;
const eventsFor = async (name: string): Promise<Array<{ id: string; f: Fields }>> =>
  (await redis.xrange(AUDIT, "-", "+")).map(([id, fields]) => {
    const f: Fields = {};
    for (let i = 0; i < fields.length; i += 2) f[fields[i]!] = fields[i + 1]!;
    return { id, f };
  }).filter((e) => e.f.name === name);
const waitFor = async (check: () => Promise<boolean>, ms = 120_000) => {
  const deadline = Date.now() + ms;
  while (!(await check()) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
};

describe("registration audit: pure parts", () => {
  it("classifies the runtime from values the registrant already has", () => {
    expect(auditRuntime(undefined, {})).toBe("generic");
    expect(auditRuntime(null, { GPTQ_CLAUDE_SESSION_RECORD: "1" })).toBe("claude");
    expect(auditRuntime("codex", { GPTQ_CLAUDE_SESSION_RECORD: "1" })).toBe("codex");
    expect(auditRuntime("pi", {})).toBe("pi");
    expect(auditRuntime("opencode", {})).toBe("unknown");
    const uuid = randomUUID();
    expect(auditClaudeSession("claude", { CLAUDE_SESSION_ID: uuid })).toBe(uuid);
    expect(auditClaudeSession("claude", { CLAUDE_SESSION_ID: "not-a-uuid" })).toBe("");
    expect(auditClaudeSession("generic", { CLAUDE_SESSION_ID: uuid })).toBe("");
  });

  it("coverage: untrimmed from before since is complete; trimmed, deleted or later-started is incomplete; missing or old Redis is unknown", () => {
    const since = 1_000_000;
    const at = (ms: number) => new Date(ms).toISOString();
    expect(coverageOf({ exists: false, length: 0, firstEntryId: null }, since)).toEqual({ coverage: "unknown", complete_from: null });
    expect(coverageOf({ exists: true, length: 3, firstEntryId: "900000-0", entriesAdded: 3 }, since).coverage).toBe("unknown");
    expect(coverageOf({ exists: true, length: 3, firstEntryId: "900000-0", entriesAdded: 3, maxDeletedEntryId: "0-0" }, since))
      .toEqual({ coverage: "complete", complete_from: at(900000) });
    // Started after since: complete only from the first entry (deploy).
    expect(coverageOf({ exists: true, length: 3, firstEntryId: "1000001-0", entriesAdded: 3, maxDeletedEntryId: "0-0" }, since))
      .toEqual({ coverage: "incomplete", complete_from: at(1000001) });
    // Trimmed (entries-added > length; trims leave max-deleted at 0-0).
    expect(coverageOf({ exists: true, length: 2, firstEntryId: "950000-0", entriesAdded: 5, maxDeletedEntryId: "0-0" }, since).coverage).toBe("complete");
    expect(coverageOf({ exists: true, length: 2, firstEntryId: "1000500-0", entriesAdded: 5, maxDeletedEntryId: "0-0" }, since))
      .toEqual({ coverage: "incomplete", complete_from: at(1000501) });
    // XDEL hole after since.
    expect(coverageOf({ exists: true, length: 4, firstEntryId: "950000-0", entriesAdded: 5, maxDeletedEntryId: "1000400-0" }, since))
      .toEqual({ coverage: "incomplete", complete_from: at(1000401) });
    expect(coverageOf({ exists: true, length: 4, firstEntryId: "950000-0", entriesAdded: 5, maxDeletedEntryId: "960000-0" }, since).coverage).toBe("complete");
  });

  it("argument validation", () => {
    expect(parseRegistrationsArgs(["--since", "2026-10-19T00:00:00Z", "--until", "2026-10-19T01:00:00Z"], {})).toMatch(/--json/);
    expect(parseRegistrationsArgs(["--since", "yesterday", "--until", "2026-10-19T01:00:00Z", "--json"], {})).toMatch(/ISO-8601/);
    expect(parseRegistrationsArgs(["--since", "2026-10-19T02:00:00Z", "--until", "2026-10-19T01:00:00Z", "--json"], {})).toMatch(/earlier/);
    expect(parseRegistrationsArgs(["--bogus", "x", "--json"], {})).toMatch(/Unknown option/);
    const ok = parseRegistrationsArgs(["--since", "2026-10-19T00:00:00Z", "--until", "2026-10-19T01:00:00Z", "--name", "n", "--json"], { REDIS_URL: "redis://x/15" });
    expect(ok).toMatchObject({ redisUrl: "redis://x/15", query: { name: "n", sinceMs: Date.parse("2026-10-19T00:00:00Z") } });
  });
});

describe("registration audit against Redis", { timeout: 300_000 }, () => {
  it("the generic register path writes one tagged event atomically with the commit; a refused registration writes none", async () => {
    const name = `audit-generic-${randomUUID().slice(0, 8)}`;
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const reg = await client.register("both", name);
    const events = await eventsFor(name);
    expect(events).toHaveLength(1);
    expect(events[0]!.f).toEqual({ event: "registered", name, session_tag: sessionTag(reg.session_id), runtime: "generic",
      pid: String(process.pid), cwd: process.cwd(), claude_session: "", via: "register" });
    expect(JSON.stringify(events)).not.toContain(reg.session_id);
    const rival = new RedisClient(null, REDIS_URL);
    clients.push(rival);
    await expect(rival.register("both", name)).rejects.toMatchObject({ code: "AGENT_NAME_HELD" });
    expect(await eventsFor(name)).toHaveLength(1);
    // Unregistering removes the registry entry but never the audit event.
    await client.unregister();
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
    expect(await eventsFor(name)).toHaveLength(1);
  });

  const startMcp = async (extra: NodeJS.ProcessEnv, cwd: string) => {
    const child = spawn(join(BIN, "gptqueue-claude-mcp"), [], {
      cwd, env: { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off", ...extra },
      stdio: ["pipe", "pipe", "pipe"],
    });
    children.push(child);
    let stdout = "";
    child.stdout!.on("data", (d) => { stdout += d; });
    child.stdin!.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "t", version: "0" } } })}\n`);
    await waitFor(async () => stdout.includes('"id":1'));
    return child;
  };

  it("the Claude MCP path records runtime claude, its cwd and the Claude session; worker mode records nothing", async () => {
    const dir = join(home, `proj-${randomUUID().slice(0, 8)}`);
    await mkdir(dir, { recursive: true });
    const name = `audit-claude-${randomUUID().slice(0, 8)}`;
    const claudeSession = randomUUID();
    await startMcp({ GPTQ_AGENT_NAME: name, CLAUDE_SESSION_ID: claudeSession, CLAUDE_PROJECT_DIR: dir }, dir);
    await waitFor(async () => (await eventsFor(name)).length > 0);
    const [event] = await eventsFor(name);
    expect(event!.f).toMatchObject({ event: "registered", name, runtime: "claude", cwd: dir, claude_session: claudeSession });
    expect(event!.f.session_tag).toMatch(/^sid-sha256:[0-9a-f]{12}$/);
    expect(Number(event!.f.pid)).toBeGreaterThan(0);

    const workerDir = join(home, `wk-${randomUUID().slice(0, 8)}`);
    await mkdir(workerDir, { recursive: true });
    const since = new Date().toISOString();
    await startMcp({ CLAUDE_CODE_ENTRYPOINT: "sdk-cli", CLAUDE_SESSION_ID: randomUUID(), CLAUDE_PROJECT_DIR: workerDir }, workerDir);
    await new Promise((r) => setTimeout(r, 1_500));
    const report = await queryRegistrations(redis, { sinceMs: Date.parse(since), untilMs: Date.now(), cwd: workerDir });
    expect(report.events).toEqual([]);
    expect(await eventsFor(`claude-${workerDir.split("/").pop()!}`)).toEqual([]);
  });

  it("filters, truncates at 500 and reports trimmed coverage on a scratch stream", async () => {
    const key = `gptq:test:registration-audit:${randomUUID()}`;
    scratchKeys.push(key);
    const session = randomUUID();
    for (let i = 0; i < 520; i += 1) {
      await redis.xadd(key, `${1_000_000 + i}-0`, "event", "registered", "name", i % 2 ? "odd" : "even", "session_tag", sessionTag(`s-${i}`),
        "runtime", "generic", "pid", "7", "cwd", i < 10 ? "/a" : "/b", "claude_session", i === 3 ? session : "");
    }
    const window = { sinceMs: 1_000_000, untilMs: 1_000_519 };
    const all = await queryRegistrations(redis, window, key);
    expect(all).toMatchObject({ schema_version: 1, coverage: "complete", truncated: true });
    expect(all.events).toHaveLength(500);
    expect(all.events[0]).toEqual({ at: new Date(1_000_000).toISOString(), name: "even", session_tag: sessionTag("s-0"), runtime: "generic", pid: 7, cwd: "/a", claude_session: "", via: "register" });
    expect((await queryRegistrations(redis, { ...window, name: "odd" }, key)).events).toHaveLength(260);
    expect((await queryRegistrations(redis, { ...window, cwd: "/a", name: "odd" }, key)).events).toHaveLength(5);
    const bySession = await queryRegistrations(redis, { ...window, claudeSession: session.toUpperCase() }, key);
    expect(bySession.events.map((e) => e.name)).toEqual(["odd"]);
    expect((await queryRegistrations(redis, { sinceMs: 1_000_010, untilMs: 1_000_011 }, key)).events).toHaveLength(2);
    // Trimmed: events before the newest deleted id are gone, so a window reaching back there is incomplete.
    await redis.xtrim(key, "MAXLEN", 100);
    const trimmed = await queryRegistrations(redis, window, key);
    expect(trimmed).toMatchObject({ coverage: "incomplete", complete_from: new Date(1_000_421).toISOString(), truncated: false });
    expect((await queryRegistrations(redis, { sinceMs: 1_000_421, untilMs: 1_000_519 }, key)).coverage).toBe("complete");
    // XDEL of a later entry opens a hole: coverage starts after it.
    await redis.xdel(key, "1000500-0");
    expect((await queryRegistrations(redis, { sinceMs: 1_000_421, untilMs: 1_000_519 }, key)))
      .toMatchObject({ coverage: "incomplete", complete_from: new Date(1_000_501).toISOString() });
    expect((await queryRegistrations(redis, window, `${key}:missing`)).coverage).toBe("unknown");
  });

  const doctor = (args: string[], url = REDIS_URL) =>
    spawnSync(process.execPath, [join(BIN, "gptqueue-doctor"), "registrations", ...args], { cwd: ROOT, env: { PATH: process.env.PATH, REDIS_URL: url }, encoding: "utf8", timeout: 120_000 });
  const digest = async (key: string): Promise<string> => createHash("sha256").update((await redis.dumpBuffer(key)) ?? Buffer.alloc(0)).digest("hex");

  it("CLI: read-only JSON report without raw session ids; exit codes 2 and 4; help", async () => {
    const since = new Date(Date.now() - 1_000).toISOString();
    const name = `audit-cli-${randomUUID().slice(0, 8)}`;
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const reg = await client.register("both", name);
    const until = new Date(Date.now() + 1_000).toISOString();
    const watched = [AUDIT, SESSION_KEYS.registry, SESSION_KEYS.agentSessions(name)];
    const before = await Promise.all(watched.map(digest));
    const dbsize = await redis.dbsize();
    const out = doctor(["--since", since, "--until", until, "--name", name, "--json"]);
    expect(out.status).toBe(0);
    const report = JSON.parse(out.stdout) as Record<string, unknown> & { events: Array<Record<string, unknown>> };
    expect(Object.keys(report).sort()).toEqual(["complete_from", "coverage", "events", "schema_version", "since", "truncated", "until"]);
    expect(report).toMatchObject({ schema_version: 1, truncated: false });
    expect(["complete", "incomplete", "unknown"]).toContain(report.coverage);
    expect(report.events).toEqual([expect.objectContaining({ name, session_tag: sessionTag(reg.session_id), runtime: "generic", pid: process.pid })]);
    expect(out.stdout).not.toContain(reg.session_id);
    expect(await Promise.all(watched.map(digest))).toEqual(before);
    expect(await redis.dbsize()).toBe(dbsize);

    expect(doctor(["--since", since, "--json"]).status).toBe(2);
    expect(doctor(["--since", since, "--until", until]).status).toBe(2);
    const down = doctor(["--since", since, "--until", until, "--json"], "redis://127.0.0.1:1/0");
    expect(down.status).toBe(4);
    const help = doctor(["--help"]);
    expect(help.status).toBe(0);
    expect(help.stdout).toMatch(/coverage: complete/);
  });

  // ---------------------------------------------------------------- gate fixes (F2-F4)

  const snapshot = async (): Promise<string[]> => Promise.all(
    [SESSION_KEYS.registry, SESSION_KEYS.registryPending, SESSION_KEYS.registryRestore].map(async (k) => JSON.stringify(Object.entries(await redis.hgetall(k)).sort())));

  it("F2: a wrong-type audit key fails the audited publish before any write", async () => {
    const name = `audit-f2-${randomUUID().slice(0, 8)}`;
    await redis.hset(SESSION_KEYS.registryPending, name, "pending-value");
    await redis.hset(SESSION_KEYS.registryRestore, name, "restore-value");
    const saved = await redis.dumpBuffer(AUDIT);
    await redis.del(AUDIT);
    await redis.set(AUDIT, "not-a-stream");
    try {
      const before = await snapshot();
      const store = new SessionStore(redis);
      await expect(store.publishRegistration(name, "{}", { name, session_tag: sessionTag("x"), runtime: "generic", pid: 1, cwd: "/", claude_session: "", via: "register" }))
        .rejects.toThrow(/WRONGTYPE/);
      expect(await snapshot()).toEqual(before);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
    } finally {
      await redis.del(AUDIT);
      if (saved) await redis.restore(AUDIT, 0, saved);
      await redis.hdel(SESSION_KEYS.registryPending, name);
      await redis.hdel(SESSION_KEYS.registryRestore, name);
    }
  });

  it("F2: register over a wrong-type audit key fails and leaves registry and bookkeeping as before", async () => {
    const name = `audit-f2r-${randomUUID().slice(0, 8)}`;
    const saved = await redis.dumpBuffer(AUDIT);
    await redis.del(AUDIT);
    await redis.set(AUDIT, "not-a-stream");
    try {
      const before = await snapshot();
      const client = new RedisClient(null, REDIS_URL);
      clients.push(client);
      await expect(client.register("both", name)).rejects.toThrow(/WRONGTYPE/);
      expect(await snapshot()).toEqual(before);
      expect(await redis.scard(SESSION_KEYS.agentSessions(name))).toBe(0);
      expect(client.agentName).toBeNull();
    } finally {
      await redis.del(AUDIT);
      if (saved) await redis.restore(AUDIT, 0, saved);
    }
  });

  const adoptionSetup = async () => {
    const target = `audit-target-${randomUUID().slice(0, 8)}`;
    const source = `audit-source-${randomUUID().slice(0, 8)}`;
    const binding = { client: "codex" as const, runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace/project" };
    const key = runtimeMailboxKey(binding);
    const mapping = JSON.stringify({ agent: target, working_directory: binding.working_directory });
    scratchKeys.push(key);
    await redis.hset(SESSION_KEYS.registry, target, JSON.stringify({ name: target }));
    await redis.set(key, mapping);
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    await client.register("both", source, undefined, { label: "l", uuid: null, client: "codex", working_directory: "/workspace/project" });
    return { client, source, target, binding, key, mapping };
  };

  it("F3: a runtime continuity adoption appends a tagged via=adoption event for the target; a refusal appends none", async () => {
    const since = Date.now() - 1_000;
    const ok = await adoptionSetup();
    const sid = ok.client.sessionId!;
    await ok.client.adoptIdentity(ok.source, ok.target, ok.key, ok.mapping, ok.binding.runtime_id);
    const report = await queryRegistrations(redis, { sinceMs: since, untilMs: Date.now() + 1_000, name: ok.target });
    expect(report.events).toEqual([expect.objectContaining({ name: ok.target, session_tag: sessionTag(sid), runtime: "codex", cwd: "/workspace/project", via: "adoption" })]);
    expect(JSON.stringify(report)).not.toContain(sid);
    const bySource = await queryRegistrations(redis, { sinceMs: since, untilMs: Date.now() + 1_000, name: ok.source });
    expect(bySource.events.map((e) => e.via)).toEqual(["register"]);

    const refused = await adoptionSetup();
    await redis.set(SESSION_KEYS.heartbeat(refused.target), "alive", "EX", 30);
    scratchKeys.push(SESSION_KEYS.heartbeat(refused.target));
    const before = await snapshot();
    const error = await refused.client.adoptIdentity(refused.source, refused.target, refused.key, refused.mapping, refused.binding.runtime_id).then(() => null, (e: unknown) => e);
    expect(error).toBeInstanceOf(AdoptionRefusedError);
    expect((error as AdoptionRefusedError).refusal.code).toBe(-5);
    expect(await snapshot()).toEqual(before);
    expect((await queryRegistrations(redis, { sinceMs: since, untilMs: Date.now() + 1_000, name: refused.target })).events).toEqual([]);
  });

  it("F3: a wrong-type audit key refuses the adoption before any write", async () => {
    const s = await adoptionSetup();
    const saved = await redis.dumpBuffer(AUDIT);
    await redis.del(AUDIT);
    await redis.set(AUDIT, "not-a-stream");
    try {
      const before = await snapshot();
      const mappingBefore = await redis.get(s.key);
      await expect(s.client.adoptIdentity(s.source, s.target, s.key, s.mapping, s.binding.runtime_id)).rejects.toThrow(/WRONGTYPE/);
      expect(await snapshot()).toEqual(before);
      expect(await redis.get(s.key)).toBe(mappingBefore);
      expect(await redis.xlen(SESSION_KEYS.continuityAudit).catch(() => -1)).toBeGreaterThanOrEqual(0);
      expect(await redis.hget(SESSION_KEYS.session(s.client.sessionId!), "agent_name")).toBe(s.source);
    } finally {
      await redis.del(AUDIT);
      if (saved) await redis.restore(AUDIT, 0, saved);
    }
  });

  it("F4: coverage is revalidated after the scan: an XDEL or trim between pages downgrades it", async () => {
    const fill = async () => {
      const key = `gptq:test:registration-audit:${randomUUID()}`;
      scratchKeys.push(key);
      for (let i = 0; i < 1_500; i += 1) {
        await redis.xadd(key, `${2_000_000 + i}-0`, "event", "registered", "name", i === 1_200 ? "victim" : "other", "session_tag", sessionTag(`s-${i}`),
          "runtime", "generic", "pid", "7", "cwd", "/c", "claude_session", "", "via", "register");
      }
      return key;
    };
    const window = { sinceMs: 2_000_000, untilMs: 2_001_499, name: "victim" };
    const clean = await fill();
    expect(await queryRegistrations(redis, window, clean)).toMatchObject({ coverage: "complete", events: [expect.objectContaining({ name: "victim" })] });

    const deleted = await fill();
    const afterXdel = await queryRegistrations(redis, window, deleted, { onPage: async (i) => { if (i === 0) await redis.xdel(deleted, "2001200-0"); } });
    expect(afterXdel.events).toEqual([]);
    expect(afterXdel).toMatchObject({ coverage: "incomplete", complete_from: new Date(2_001_201).toISOString() });

    const trimmed = await fill();
    const afterTrim = await queryRegistrations(redis, { ...window, name: "absent" }, trimmed, { onPage: async (i) => { if (i === 0) await redis.xtrim(trimmed, "MAXLEN", 100); } });
    expect(afterTrim.coverage).toBe("incomplete");
    expect(Date.parse(afterTrim.complete_from!)).toBeGreaterThan(window.sinceMs);
  });
});

