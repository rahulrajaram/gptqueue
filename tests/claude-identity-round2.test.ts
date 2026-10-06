/**
 * Live-test follow-ups: SessionStart before the MCP record exists (ID-2),
 * name-free watcher (WK-1), generic-tool trace parity (AC-3/LT-0), bounded
 * AGENT_NAME_HELD fallback chain, old-format holder lease wait (ID-3) and the
 * old gptqueue-inbox-peek CLI contract.
 */
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { publishSessionRecord, readProcess } from "../src/claude/session-record.js";
import { waitForIdentity } from "../src/claude/inbox-probe.js";
import { outagePolicy, parsePeekArgs, watchInbox, WATCH_RECORD_WAIT_MS } from "../src/claude/peek.js";
import { GptQueueError } from "../src/mcp-server/errors.js";
import { fallbackCandidates, holdToolCallsUntil, NameExhaustedError, registerClaudeAware, type RegisterDeps } from "../src/claude/mcp-record.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const BIN = resolve(__dirname, "..", "bin");
let home = "";
let env: NodeJS.ProcessEnv = {};
let redis: Redis;
const clients: RedisClient[] = [];
const self = () => readProcess(process.pid)!;

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-claude-r2-"));
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off" };
  redis = new Redis(REDIS_URL);
});
afterEach(async () => {
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  publishSessionRecord(self(), null, null, env);
});
afterAll(async () => { redis.disconnect(); await rm(home, { recursive: true, force: true }); });

const run = (bin: string, args: string[], input: string, extra: NodeJS.ProcessEnv = {}, onSpawn?: () => void) =>
  new Promise<{ stdout: string; stderr: string; code: number | null }>((done) => {
    const child = spawn(join(BIN, bin), args, { env: { ...env, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.on("data", (d) => { stdout += d; });
    child.stderr.on("data", (d) => { stderr += d; });
    child.on("close", (code) => done({ stdout: stdout.trim(), stderr, code }));
    child.stdin.end(input);
    onSpawn?.();
  });
const agent = async (prefix: string) => {
  const client = new RedisClient(null, REDIS_URL);
  clients.push(client);
  return { client, ...(await client.register("both", `${prefix}-${randomUUID().slice(0, 8)}`)) };
};
const task = (to: string, content = "work") => JSON.stringify({ id: randomUUID(), from: "tester", to, timestamp: new Date().toISOString(), type: "task", payload: { content } });

describe("ID-2/WK-1: SessionStart and the watcher before the MCP record exists", { timeout: 300_000 }, () => {
  it("SessionStart with no record reports UNCONFIRMED, no guessed counts, and a name-free watcher command", async () => {
    const other = await agent("claude-other"); // another session's mailbox under the guessed name
    await redis.rpush(SESSION_KEYS.queue(other.name), task(other.name));
    const out = await run("gptqueue-claude-hook", ["session-start"], JSON.stringify({ session_id: randomUUID(), cwd: home }),
      { GPTQ_AGENT_NAME: other.name, GPTQ_CLAUDE_RECORD_WAIT_MS: "200" });
    const text = JSON.parse(out.stdout).hookSpecificOutput.additionalContext as string;
    expect(text).toMatch(/UNCONFIRMED/);
    expect(text).not.toMatch(/unclaimed/);
    expect(text).toMatch(/gptqueue-inbox-peek' --watch --session '[^']+'$/);
    expect(text).not.toContain("--name");
  });

  it("SessionStart waits for a record that appears after the hook started, then reports confirmed counts", async () => {
    const me = await agent("claude-late");
    await redis.rpush(SESSION_KEYS.queue(me.name), task(me.name));
    const out = await run("gptqueue-claude-hook", ["session-start"], JSON.stringify({ session_id: randomUUID(), cwd: home }),
      // Generous wait: module loads on the contended mount can take several seconds.
      { GPTQ_AGENT_NAME: "claude-guess", GPTQ_CLAUDE_RECORD_WAIT_MS: "60000" },
      () => { setTimeout(() => publishSessionRecord(self(), me.name, me.session_id, env), 300); });
    const text = JSON.parse(out.stdout).hookSpecificOutput.additionalContext as string;
    expect(text).toContain(`agent '${me.name}' (gptqueue session ${me.session_id}`);
    expect(text).toContain("1 unclaimed");
  });

  it("waitForIdentity resolves a record published after it started waiting", async () => {
    setTimeout(() => publishSessionRecord(self(), "claude-later", "s-later", env), 150);
    const id = await waitForIdentity(undefined, 5000, env, 20);
    expect(id).toMatchObject({ agent: "claude-later", source: "session_record" });
  });

  it("a name-free watcher refuses to watch a guessed mailbox when no record appears", async () => {
    const out = await run("gptqueue-inbox-peek", ["--watch", "--session", randomUUID()], "", { GPTQ_AGENT_NAME: "claude-guess", GPTQ_CLAUDE_RECORD_WAIT_MS: "200" });
    expect(out.code).toBe(5);
    expect(JSON.parse(out.stdout)).toMatchObject({ error: "session_identity_unconfirmed" });
  });

  it("a name-free watcher watches the recorded name, not the guessed one", async () => {
    const me = await agent("claude-watchname");
    publishSessionRecord(self(), me.name, me.session_id, env);
    await redis.rpush(SESSION_KEYS.queue(me.name), task(me.name));
    const out = await run("gptqueue-inbox-peek", ["--watch", "--session", randomUUID(), "--timeout", "60"], "", { GPTQ_AGENT_NAME: "claude-guess" });
    expect(out.code).toBe(0);
    expect(JSON.parse(out.stdout)).toMatchObject({ agent: me.name, pending: 1 });
  });
});

describe("old gptqueue-inbox-peek CLI contract", { timeout: 300_000 }, () => {
  it("--name prints the count, overriding any session record", async () => {
    const me = await agent("claude-compat");
    publishSessionRecord(self(), "claude-someone-else", "s-x", env);
    await redis.rpush(SESSION_KEYS.queue(me.name), task(me.name), task(me.name));
    const out = await run("gptqueue-inbox-peek", ["--name", me.name], "");
    expect(out).toMatchObject({ stdout: "2", code: 0 });
  });

  it("--name --watch prints exactly one {agent,pending,at} line and exits 0; timeout prints {agent,pending:0,timeout:true} and exits 3", async () => {
    const me = await agent("claude-compatw");
    await redis.rpush(SESSION_KEYS.queue(me.name), task(me.name));
    const hit = await run("gptqueue-inbox-peek", ["--name", me.name, "--watch", "--interval", "30"], "");
    expect(hit.code).toBe(0);
    expect(hit.stdout.split("\n")).toHaveLength(1);
    expect(hit.stdout).toMatch(new RegExp(`^\\{"agent":"${me.name}","pending":1,"at":"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ"\\}$`));
    const empty = `claude-empty-${randomUUID().slice(0, 8)}`;
    const timeout = await run("gptqueue-inbox-peek", ["--name", empty, "--watch", "--timeout", "1"], "");
    expect(timeout).toMatchObject({ code: 3, stdout: `{"agent":"${empty}","pending":0,"timeout":true}` });
  });

  it("bad arguments exit 2", async () => {
    const out = await run("gptqueue-inbox-peek", ["--bogus"], "");
    expect(out.code).toBe(2);
    expect(out.stderr).toMatch(/unknown argument: --bogus/);
  });
});

describe("watcher record wait and Redis-outage policy", () => {
  const DEAD = "redis://127.0.0.1:1/0";
  const args = (argv: string[]) => { const a = parsePeekArgs(argv); if (typeof a === "string") throw new Error(a); return a; };

  it("the name-free watcher waits 40 s for the record, outlasting the 32 s held-name registration wait", () => {
    expect(WATCH_RECORD_WAIT_MS).toBe(40_000);
    expect(WATCH_RECORD_WAIT_MS).toBeGreaterThan(32_000);
  });

  it("legacy --name mode retries at --interval and never gives up on Redis errors", async () => {
    const policy = outagePolicy(args(["--name", "x", "--watch", "--interval", "0.1"]));
    expect(policy).toEqual({ giveUpAfterMs: null, retryMs: 100 });
    expect(outagePolicy(args(["--name", "x", "--watch"])).retryMs).toBe(30_000); // old default --interval 30
    const controller = new AbortController();
    const started = Date.now();
    const outcome = watchInbox(DEAD, "x", { timeoutSec: 0, blockMs: 100, signal: controller.signal, ...policy, giveUpAfterMs: policy.giveUpAfterMs });
    let settled = false;
    void outcome.then(() => { settled = true; });
    await new Promise((r) => setTimeout(r, 1500));
    expect(settled).toBe(false); // still polling through the outage
    controller.abort();
    await expect(outcome).resolves.toEqual({ kind: "timeout" });
    expect(Date.now() - started).toBeGreaterThanOrEqual(1500);
  }, 20_000);

  it("session mode gives up with 'unavailable' (exit 4) after its give-up window", async () => {
    expect(outagePolicy(args(["--watch", "--session", "s"]))).toEqual({ giveUpAfterMs: 30_000, retryMs: 1000 });
    const outcome = await watchInbox(DEAD, "x", { timeoutSec: 0, blockMs: 100, giveUpAfterMs: 200, retryMs: 50 });
    expect(outcome.kind).toBe("unavailable");
  }, 20_000);
});

describe("AC-3/LT-0: generic MCP tools write the inbox trace stages", () => {
  it("claim_tasks, acknowledge_tasks and a correlated send_message trace task_claimed, task_acknowledged and reply_sent", async () => {
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const server = new McpServer({ name: "t", version: "0" });
    registerTools(server, client);
    const [st, ct] = InMemoryTransport.createLinkedPair();
    await server.connect(st);
    const mcp = new Client({ name: "c", version: "0" });
    await mcp.connect(ct);
    const call = async (name: string, args: Record<string, unknown>) => {
      const res = await mcp.callTool({ name, arguments: args });
      expect(res.isError ?? false, `${name}: ${JSON.stringify(res.content)}`).toBe(false);
      return res.structuredContent as Record<string, any>;
    };
    try {
      const name = `generic-trace-${randomUUID().slice(0, 8)}`;
      await call("register_agent", { name, role: "both", description: "trace parity test" });
      const id = randomUUID();
      await redis.rpush(SESSION_KEYS.queue(name), JSON.stringify({ id, from: "tester", to: name, timestamp: new Date().toISOString(), type: "task", payload: { content: "x" } }));
      const claimed = await call("claim_tasks", { max_batch: 1 });
      const claimId = String(claimed.claim.claim_id);
      const sent = await call("send_message", { to: name, content: "done", type: "result", in_reply_to: id });
      await call("acknowledge_tasks", { claim_id: claimId });
      const entries = await redis.xrange(SESSION_KEYS.inboxTrace(name), "-", "+");
      const stages = entries.map(([, f]) => Object.fromEntries(Array.from({ length: f.length / 2 }, (_, i) => [f[2 * i], f[2 * i + 1]])));
      expect(stages).toEqual(expect.arrayContaining([
        expect.objectContaining({ stage: "task_claimed", claim_id: claimId, message_id: id }),
        expect.objectContaining({ stage: "reply_sent", message_id: String(sent.message_id), in_reply_to: id }),
        expect.objectContaining({ stage: "task_acknowledged", claim_id: claimId }),
      ]));
    } finally { await mcp.close(); }
  });
});

describe("AGENT_NAME_HELD: bounded fallback chain and old-format lease wait", () => {
  const claudeEnv = { GPTQ_CLAUDE_SESSION_RECORD: "1", CLAUDE_CODE_SESSION_ID: "parent12-aaaa" } as NodeJS.ProcessEnv;
  const held = (details: Record<string, unknown> = { holder_liveness_verifiable: true }) => Object.assign(new Error("held"), { code: "AGENT_NAME_HELD", details });
  const fakeDeps = (budget = 1000): RegisterDeps & { slept: number[] } => {
    let clock = 0;
    const slept: number[] = [];
    let n = 0;
    return { slept, now: () => clock, sleep: async (ms) => { slept.push(ms); clock += ms; }, maxLeaseWaitMs: budget, random: () => `r${String(n++).padStart(7, "0")}` };
  };

  it("nested launch: an inherited session id that collides moves on to <name>-<pid>, then a random suffix, recording the origin", async () => {
    const calls: string[] = [];
    const origins: string[] = [];
    const taken = new Set(["n", "n-parent12", `n-${process.pid}`]);
    const registrar = { register: async (_r: string, name: string) => { calls.push(name); if (taken.has(name)) throw held(); return { name, session_id: "sid" }; } };
    const candidates = fallbackCandidates("n", claudeEnv, 4242, () => "abcd1234");
    expect(candidates.map((c) => c.name)).toEqual(["n-parent12", "n-4242", "n-abcd1234"]);
    const deps = fakeDeps();
    const result = await registerClaudeAware(registrar, "n", { ...claudeEnv, GPTQ_CLAUDE_PID: String(process.pid) }, deps, (o) => origins.push(o));
    expect(result.origin).toBe("random");
    expect(calls.slice(0, 2)).toEqual(["n", "n-parent12"]);
    expect(calls[2]).toBe(`n-${process.pid}`);
    expect(calls).toHaveLength(4);
    expect(origins.at(-1)).toBe("random");
    expect(deps.slept).toEqual([]); // verifiable holders: no lease wait
  });

  it("fails with a clear error after a bounded number of attempts", async () => {
    const calls: string[] = [];
    const registrar = { register: async (_r: string, name: string) => { calls.push(name); throw held(); } };
    const error = await registerClaudeAware(registrar, "n", claudeEnv, fakeDeps()).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(NameExhaustedError);
    expect((error as Error).message).toMatch(/AGENT_NAME_HELD: every candidate name is held/);
    expect(calls.length).toBeLessThanOrEqual(5);
  });

  it("ID-3: an old-format holder whose lease lapses during the wait yields the stable name", async () => {
    let attempts = 0;
    const registrar = { register: async (_r: string, name: string) => { attempts += 1; if (attempts < 3) throw held({ holder_liveness_verifiable: false, holder_lease_ttl_ms: 4000 }); return { name, session_id: "sid" }; } };
    const deps = fakeDeps(32_000);
    await expect(registerClaudeAware(registrar, "stable", claudeEnv, deps)).resolves.toMatchObject({ name: "stable", origin: "requested" });
    expect(deps.slept).toEqual([4100, 4100]);
  });

  it("ID-3: a live old-format holder that keeps refreshing gets the fallback after the bounded wait", async () => {
    const registrar = { register: async (_r: string, name: string) => { if (name === "stable") throw held({ holder_liveness_verifiable: false, holder_lease_ttl_ms: 30_000 }); return { name, session_id: "sid" }; } };
    const deps = fakeDeps(32_000);
    await expect(registerClaudeAware(registrar, "stable", claudeEnv, deps)).resolves.toMatchObject({ name: "stable-parent12", origin: "session_id" });
    expect(deps.slept.reduce((a, b) => a + b, 0)).toBe(32_000);
  });

  it("ID-3 against Redis: an old-format session (no pid/hostname) is waited out by lease, and the stable name is taken", async () => {
    const name = `claude-oldfmt-${randomUUID().slice(0, 8)}`;
    const oldSid = randomUUID();
    await redis.hset(SESSION_KEYS.session(oldSid), "session_id", oldSid, "agent_name", name, "role", "both", "created_at", new Date().toISOString(), "last_seen", new Date().toISOString());
    await redis.set(SESSION_KEYS.lease(oldSid), "1", "PX", 700);
    await redis.sadd(SESSION_KEYS.agentSessions(name), oldSid);
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const plain = await client.register("both", name).catch((e: unknown) => e);
    expect((plain as { details?: Record<string, unknown> }).details).toMatchObject({ holder_session_id: oldSid, holder_liveness_verifiable: false });
    expect(Number((plain as { details: Record<string, unknown> }).details.holder_lease_ttl_ms)).toBeGreaterThan(0);
    const started = Date.now();
    const result = await registerClaudeAware(client, name, claudeEnv, { sleep: (ms) => new Promise((r) => setTimeout(r, ms)), now: Date.now, maxLeaseWaitMs: 5000 });
    expect(result).toMatchObject({ name, origin: "requested" });
    expect(Date.now() - started).toBeLessThan(5000);
  }, 60_000);

  it("tool calls are held until background registration settles; other requests pass", async () => {
    const delivered: string[] = [];
    const transport: { onmessage?: (m: { method: string }) => void } = { onmessage: (m) => delivered.push(m.method) };
    let release!: () => void;
    holdToolCallsUntil(transport, new Promise<void>((r) => { release = r; }));
    transport.onmessage!({ method: "initialize" });
    transport.onmessage!({ method: "tools/call" });
    expect(delivered).toEqual(["initialize"]);
    release();
    await new Promise((r) => setTimeout(r, 0));
    expect(delivered).toEqual(["initialize", "tools/call"]);
  });

  type Msg = { method: string; id?: number };
  type Sent = { jsonrpc: "2.0"; id: string | number; result: Record<string, unknown> };
  const heldTransport = () => {
    const delivered: Msg[] = [];
    const sent: Sent[] = [];
    let closedByServer = 0;
    const transport: { onmessage?: (m: Msg) => void; onclose?: () => void; send: (m: Sent) => Promise<void> } = {
      onmessage: (m) => delivered.push(m),
      onclose: () => { closedByServer += 1; },
      send: async (m) => { sent.push(m); },
    };
    return { transport, delivered, sent, closedByServer: () => closedByServer };
  };
  const code = (s: Sent) => ((s.result.structuredContent as { error: { code: string } }).error.code);

  it("a registration that never settles answers held calls with AGENT_NOT_REGISTERED after the bound, in order", async () => {
    const t = heldTransport();
    holdToolCallsUntil(t.transport, new Promise<void>(() => undefined), 50);
    for (const id of [1, 2, 3]) t.transport.onmessage!({ method: "tools/call", id });
    await new Promise((r) => setTimeout(r, 120));
    expect(t.delivered).toEqual([]);
    expect(t.sent.map((s) => s.id)).toEqual([1, 2, 3]);
    expect(t.sent.map(code)).toEqual(["AGENT_NOT_REGISTERED", "AGENT_NOT_REGISTERED", "AGENT_NOT_REGISTERED"]);
    expect(t.sent[0]!.result.isError).toBe(true);
  });

  it("a registration failure answers every held call with the registration's own typed code", async () => {
    const t = heldTransport();
    let fail!: (e: unknown) => void;
    holdToolCallsUntil(t.transport, new Promise<void>((_, reject) => { fail = reject; }));
    for (const id of [7, 8]) t.transport.onmessage!({ method: "tools/call", id });
    fail(new GptQueueError("AGENT_NAME_HELD", "held", { name: "x", holder_session_id: "s" }));
    await new Promise((r) => setTimeout(r, 0));
    expect(t.sent.map((s) => [s.id, code(s)])).toEqual([[7, "AGENT_NAME_HELD"], [8, "AGENT_NAME_HELD"]]);
    expect(t.delivered).toEqual([]);
  });

  it("a closed transport drops held calls without any delivery or reply", async () => {
    const t = heldTransport();
    let release!: () => void;
    holdToolCallsUntil(t.transport, new Promise<void>((r) => { release = r; }), 50);
    t.transport.onmessage!({ method: "tools/call", id: 1 });
    t.transport.onclose!();
    expect(t.closedByServer()).toBe(1); // the server's own close handler still runs
    release();
    await new Promise((r) => setTimeout(r, 120));
    expect(t.delivered).toEqual([]);
    expect(t.sent).toEqual([]);
  });

  it("after a failure or a timeout, LATER tool calls get the same typed error; after success they pass", async () => {
    const failed = heldTransport();
    let fail!: (e: unknown) => void;
    holdToolCallsUntil(failed.transport, new Promise<void>((_, reject) => { fail = reject; }));
    fail(new GptQueueError("AGENT_NAME_HELD", "held", { name: "x", holder_session_id: "s" }));
    await new Promise((r) => setTimeout(r, 0));
    failed.transport.onmessage!({ method: "tools/call", id: 21 });
    failed.transport.onmessage!({ method: "tools/list", id: 22 });
    expect(failed.sent.map((s) => [s.id, code(s)])).toEqual([[21, "AGENT_NAME_HELD"]]);
    expect(failed.delivered.map((m) => m.id)).toEqual([22]);

    const slow = heldTransport();
    let late!: () => void;
    holdToolCallsUntil(slow.transport, new Promise<void>((r) => { late = r; }), 30);
    await new Promise((r) => setTimeout(r, 80));
    slow.transport.onmessage!({ method: "tools/call", id: 31 });
    expect(slow.sent.map((s) => [s.id, code(s)])).toEqual([[31, "AGENT_NOT_REGISTERED"]]);
    expect(slow.delivered).toEqual([]);
    // A registration that completes after the timeout lets later calls through.
    late();
    await new Promise((r) => setTimeout(r, 0));
    slow.transport.onmessage!({ method: "tools/call", id: 32 });
    expect(slow.delivered.map((m) => m.id)).toEqual([32]);

    const ok = heldTransport();
    holdToolCallsUntil(ok.transport, Promise.resolve());
    await new Promise((r) => setTimeout(r, 0));
    ok.transport.onmessage!({ method: "tools/call", id: 41 });
    expect(ok.delivered.map((m) => m.id)).toEqual([41]);
    expect(ok.sent).toEqual([]);
  });

  it("success delivers held calls in arrival order, ahead of later calls", async () => {
    const t = heldTransport();
    let release!: () => void;
    holdToolCallsUntil(t.transport, new Promise<void>((r) => { release = r; }));
    for (const id of [1, 2, 3]) t.transport.onmessage!({ method: "tools/call", id });
    release();
    await new Promise((r) => setTimeout(r, 0));
    t.transport.onmessage!({ method: "tools/call", id: 4 });
    expect(t.delivered.map((m) => m.id)).toEqual([1, 2, 3, 4]);
  });
});
