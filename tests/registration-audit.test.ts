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
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SessionStore } from "../src/core/session-store.js";
import { runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { AdoptionRefusedError } from "../src/core/adoption-refusal.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { sessionTag } from "../src/core/session-tag.js";
import { auditClaudeSession, auditRuntime, coverageOf, parseGap, parseRegistrationsArgs, queryRegistrations, withGap } from "../src/core/registration-audit.js";
import { startClaudeSessionRecord } from "../src/claude/mcp-record.js";
import { findSessionRecord, readProcess } from "../src/claude/session-record.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const ROOT = resolve(__dirname, "..");
const BIN = join(ROOT, "bin");
const AUDIT = SESSION_KEYS.registrationAudit;
/** The audit gap marker: the audit key plus ":gap" (SESSION_KEYS.registrationAuditGap). */
const GAP = `${AUDIT}:gap`;
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
  it("F8: parseGap accepts only safe integers whose instant and instant + 1 are valid Dates; anything else is invalid and coverage unknown", () => {
    const MAX = 8.64e15;
    expect(parseGap("string", String(MAX - 1))).toEqual({ kind: "at", ms: MAX - 1 });
    expect(parseGap("string", String(MAX))).toEqual({ kind: "invalid" });
    expect(parseGap("string", "9999999999999999")).toEqual({ kind: "invalid" });
    expect(parseGap("string", "0001234")).toEqual({ kind: "at", ms: 1234 });
    expect(parseGap("string", "0")).toEqual({ kind: "at", ms: 0 });
    expect(parseGap("string", "")).toEqual({ kind: "invalid" });
    expect(parseGap("string", "-1")).toEqual({ kind: "invalid" });
    expect(parseGap("string", null)).toEqual({ kind: "absent" });
    const base = { coverage: "complete" as const, complete_from: new Date(1_000).toISOString() };
    expect(withGap(base, parseGap("string", "9999999999999999"), 0)).toEqual({ coverage: "unknown", complete_from: null });
    expect(() => withGap(base, parseGap("string", String(MAX - 1)), 0)).not.toThrow();
    expect(withGap(base, parseGap("string", String(MAX - 1)), 0)).toEqual({ coverage: "incomplete", complete_from: new Date(MAX).toISOString() });
  });

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

  /**
   * Run `body` with the real audit key replaced by a non-stream value (and,
   * with gap "wrong-type", the gap marker by a hash); restores both after.
   */
  const withBrokenAudit = async (gap: "absent" | "wrong-type", body: () => Promise<void>) => {
    // Reads only before the try: every mutation of these global keys happens
    // inside it, so a failing setup step still restores them (F9).
    const saved = await Promise.all([AUDIT, GAP].map((k) => redis.dumpBuffer(k)));
    try {
      await redis.del(AUDIT, GAP);
      await redis.set(AUDIT, "not-a-stream");
      if (gap === "wrong-type") await redis.hset(GAP, "foreign", "value");
      await body();
    } finally {
      await redis.del(AUDIT, GAP);
      for (const [i, k] of [AUDIT, GAP].entries()) if (saved[i]) await redis.restore(k, 0, saved[i]!);
    }
  };
  const gapMs = async (): Promise<number> => Number(await redis.get(GAP));

  it("the gap marker key is the audit key plus :gap", () => {
    expect(SESSION_KEYS.registrationAuditGap).toBe(GAP);
  });

  it("a wrong-type audit key: registration still commits and publishes, writes no event, records a gap; the query reports incomplete", async () => {
    const name = `audit-gap-${randomUUID().slice(0, 8)}`;
    await withBrokenAudit("absent", async () => {
      const since = Date.now() - 1_000;
      const client = new RedisClient(null, REDIS_URL);
      clients.push(client);
      await expect(client.register("both", name)).resolves.toMatchObject({ name });
      expect(client.agentName).toBe(name);
      expect(JSON.parse((await redis.hget(SESSION_KEYS.registry, name)) ?? "null")).toMatchObject({ name });
      expect(await redis.sismember(SESSION_KEYS.agentSessions(name), client.sessionId!)).toBe(1);
      expect(await redis.get(AUDIT)).toBe("not-a-stream");
      expect(await gapMs()).toBeGreaterThanOrEqual(since);
      expect(await gapMs()).toBeLessThanOrEqual(Date.now() + 1_000);
      const report = await queryRegistrations(redis, { sinceMs: since, untilMs: Date.now() + 1_000, name });
      expect(report).toMatchObject({ coverage: "incomplete", complete_from: null, events: [], truncated: false });
    });
  });

  it("a wrong-type audit key: a rename with queued mail completes (mailbox moved, lease, heartbeat, identity record) and records a gap", async () => {
    const id = randomUUID().slice(0, 8);
    const oldName = `audit-ren-a-${id}`;
    const newName = `audit-ren-b-${id}`;
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const first = await client.register("both", oldName);
    const recordEnv = { GPTQ_CLAUDE_SESSION_RECORD: "1", GPTQ_CLAUDE_PID: String(process.pid), GPTQ_CLAUDE_RUNTIME_DIR: join(home, `rt-${id}`) };
    const record = startClaudeSessionRecord(client, recordEnv);
    expect(record).not.toBeNull();
    const queued = [1, 2, 3].map((i) => JSON.stringify({ id: `m${i}-${id}`, type: "task" }));
    await redis.rpush(SESSION_KEYS.queue(oldName), ...queued);
    const leases = vi.spyOn(client.sessions, "startLeaseRefresh");
    try {
      await withBrokenAudit("absent", async () => {
        const since = Date.now() - 1_000;
        await expect(client.register("both", newName)).resolves.toMatchObject({ name: newName });
        const sid = client.sessionId!;
        expect(sid).not.toBe(first.session_id);
        expect(client.agentName).toBe(newName);
        // Mailbox migrated, old identity retired, new one published.
        expect(await redis.lrange(SESSION_KEYS.queue(oldName), 0, -1)).toEqual([]);
        expect(await redis.lrange(SESSION_KEYS.queue(newName), 0, -1)).toEqual(queued);
        expect(await client.sessions.getSession(first.session_id)).toBeNull();
        expect(await redis.hexists(SESSION_KEYS.registry, oldName)).toBe(0);
        expect(JSON.parse((await redis.hget(SESSION_KEYS.registry, newName)) ?? "null")).toMatchObject({ name: newName });
        // Lease refreshed and heartbeat live for the new identity.
        expect(leases).toHaveBeenCalledWith(sid);
        expect(await redis.pttl(SESSION_KEYS.lease(sid))).toBeGreaterThan(0);
        await waitFor(async () => (await redis.get(SESSION_KEYS.heartbeat(newName))) === "alive", 5_000);
        expect(await redis.get(SESSION_KEYS.heartbeat(newName))).toBe("alive");
        // Identity observers notified: the Claude session record names the new identity.
        expect(findSessionRecord([readProcess(process.pid)!], recordEnv)).toMatchObject({ kind: "found", record: { agent: newName, gptqueue_session_id: sid } });
        // No event; a gap instead.
        expect(await redis.get(AUDIT)).toBe("not-a-stream");
        expect(await gapMs()).toBeGreaterThanOrEqual(since);
      });
    } finally {
      leases.mockRestore();
      record?.remove();
    }
  });

  it("wrong-type audit key and gap marker: registration still commits, the marker is left alone, the query reports unknown", async () => {
    const name = `audit-gap2-${randomUUID().slice(0, 8)}`;
    await withBrokenAudit("wrong-type", async () => {
      const since = Date.now() - 1_000;
      const client = new RedisClient(null, REDIS_URL);
      clients.push(client);
      await expect(client.register("both", name)).resolves.toMatchObject({ name });
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
      expect(await redis.get(AUDIT)).toBe("not-a-stream");
      expect(await redis.hgetall(GAP)).toEqual({ foreign: "value" });
      const report = await queryRegistrations(redis, { sinceMs: since, untilMs: Date.now() + 1_000, name });
      expect(report).toMatchObject({ coverage: "unknown", complete_from: null, events: [] });
    });
  });

  it("the registry bookkeeping keys still fail the publish on a wrong type, before any write, with no event or gap", async () => {
    const name = `audit-pend-${randomUUID().slice(0, 8)}`;
    const saved = await Promise.all([SESSION_KEYS.registryPending, GAP].map((k) => redis.dumpBuffer(k)));
    try {
      // Setup mutations inside the restoring try (F9).
      await redis.del(SESSION_KEYS.registryPending, GAP);
      await redis.set(SESSION_KEYS.registryPending, "not-a-hash");
      const events = (await redis.type(AUDIT)) === "stream" ? await redis.xlen(AUDIT) : 0;
      const store = new SessionStore(redis);
      await expect(store.publishRegistration(name, "{}", { name, session_tag: sessionTag("x"), runtime: "generic", pid: 1, cwd: "/", claude_session: "", via: "register" }))
        .rejects.toThrow(/WRONGTYPE/);
      expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
      expect((await redis.type(AUDIT)) === "stream" ? await redis.xlen(AUDIT) : 0).toBe(events);
      expect(await eventsFor(name)).toEqual([]);
      expect(await redis.exists(GAP)).toBe(0);
    } finally {
      await redis.del(SESSION_KEYS.registryPending, GAP);
      for (const [i, k] of [SESSION_KEYS.registryPending, GAP].entries()) if (saved[i]) await redis.restore(k, 0, saved[i]!);
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

  it("a wrong-type audit key: a runtime continuity adoption still completes and records a gap", async () => {
    const s = await adoptionSetup();
    const sid = s.client.sessionId!;
    await withBrokenAudit("absent", async () => {
      const since = Date.now() - 1_000;
      await expect(s.client.adoptIdentity(s.source, s.target, s.key, s.mapping, s.binding.runtime_id)).resolves.toBeUndefined();
      expect(s.client.agentName).toBe(s.target);
      expect(await redis.hget(SESSION_KEYS.session(sid), "agent_name")).toBe(s.target);
      expect(await redis.sismember(SESSION_KEYS.agentSessions(s.target), sid)).toBe(1);
      expect(await redis.hexists(SESSION_KEYS.registry, s.source)).toBe(0);
      expect(await redis.get(AUDIT)).toBe("not-a-stream");
      expect(await gapMs()).toBeGreaterThanOrEqual(since);
    });
  });

  it("adoption keeps the pre-audit failure conditions: target inbox events of a wrong type fail it only when the source queue holds work", async () => {
    // Empty source queue: the target's inbox events are never written, so a wrong type does not matter (as before).
    const idle = await adoptionSetup();
    scratchKeys.push(SESSION_KEYS.inboxEvents(idle.target));
    await redis.set(SESSION_KEYS.inboxEvents(idle.target), "not-a-stream");
    await expect(idle.client.adoptIdentity(idle.source, idle.target, idle.key, idle.mapping, idle.binding.runtime_id)).resolves.toBeUndefined();
    expect(idle.client.agentName).toBe(idle.target);

    // Work queued under the source: the forward would write them, so the adoption fails, now before any write.
    const busy = await adoptionSetup();
    scratchKeys.push(SESSION_KEYS.inboxEvents(busy.target), SESSION_KEYS.queue(busy.source));
    await redis.set(SESSION_KEYS.inboxEvents(busy.target), "not-a-stream");
    await redis.rpush(SESSION_KEYS.queue(busy.source), JSON.stringify({ id: "w1", type: "task" }));
    const before = await snapshot();
    await expect(busy.client.adoptIdentity(busy.source, busy.target, busy.key, busy.mapping, busy.binding.runtime_id)).rejects.toThrow(/WRONGTYPE/);
    expect(await snapshot()).toEqual(before);
    expect(await redis.hget(SESSION_KEYS.session(busy.client.sessionId!), "agent_name")).toBe(busy.source);
    expect(await redis.llen(SESSION_KEYS.queue(busy.source))).toBe(1);
  });

  it("audit gap marker: a gap at or after since makes coverage incomplete from gap + 1; an unreadable marker makes it unknown; reads only", async () => {
    const key = `gptq:test:registration-audit:${randomUUID()}`;
    const gap = `${key}:gap`;
    scratchKeys.push(key, gap);
    for (let i = 0; i < 10; i += 1) {
      await redis.xadd(key, `${3_000_000 + i}-0`, "event", "registered", "name", "n", "session_tag", sessionTag(`s-${i}`),
        "runtime", "generic", "pid", "7", "cwd", "/g", "claude_session", "", "via", "register");
    }
    const window = { sinceMs: 3_000_000, untilMs: 3_000_009 };
    const at = (ms: number) => new Date(ms).toISOString();
    expect(await queryRegistrations(redis, window, key)).toMatchObject({ coverage: "complete", complete_from: at(3_000_000) });
    expect(await redis.exists(gap)).toBe(0);
    // A gap inside the window: events from the gap instant may be missing.
    await redis.set(gap, "3000005");
    const digests = await Promise.all([key, gap].map(digest));
    const inside = await queryRegistrations(redis, window, key);
    expect(inside).toMatchObject({ coverage: "incomplete", complete_from: at(3_000_006) });
    expect(inside.events).toHaveLength(10);
    expect(await Promise.all([key, gap].map(digest))).toEqual(digests);
    expect((await queryRegistrations(redis, { sinceMs: 3_000_006, untilMs: 3_000_009 }, key)).coverage).toBe("complete");
    // A gap before since does not affect the window.
    await redis.set(gap, "2999999");
    expect(await queryRegistrations(redis, window, key)).toMatchObject({ coverage: "complete", complete_from: at(3_000_000) });
    // Unparsable or wrong-type marker: unknown.
    await redis.set(gap, "yesterday");
    expect(await queryRegistrations(redis, window, key)).toMatchObject({ coverage: "unknown", complete_from: null });
    await redis.del(gap);
    await redis.hset(gap, "foreign", "value");
    expect(await queryRegistrations(redis, window, key)).toMatchObject({ coverage: "unknown", complete_from: null });
    // No stream (missing or another type): a gap in the window is incomplete with no bound; otherwise unknown.
    const missing = `${key}:missing`;
    scratchKeys.push(missing, `${missing}:gap`);
    await redis.set(`${missing}:gap`, "3000005");
    expect(await queryRegistrations(redis, window, missing)).toMatchObject({ coverage: "incomplete", complete_from: null, events: [] });
    await redis.set(`${missing}:gap`, "2999999");
    expect(await queryRegistrations(redis, window, missing)).toMatchObject({ coverage: "unknown", complete_from: null, events: [] });
    await redis.set(missing, "not-a-stream");
    expect(await queryRegistrations(redis, window, missing)).toMatchObject({ coverage: "unknown", events: [] });
    expect(await redis.get(missing)).toBe("not-a-stream");
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

