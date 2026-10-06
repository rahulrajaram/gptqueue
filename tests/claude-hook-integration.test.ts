/**
 * Spawns the real bin/ entry points (built dist) against the private test
 * Redis with HOME and the runtime dir pointed at temp dirs.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { publishSessionRecord, readProcess, watcherPidPath } from "../src/claude/session-record.js";
import { probeClient, probeInbox } from "../src/claude/inbox-probe.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const BIN = resolve(__dirname, "..", "bin");
let home = "";
let runtime = "";
let env: NodeJS.ProcessEnv = {};
const clients: RedisClient[] = [];
const children: ChildProcess[] = [];

beforeAll(async () => {
  // Spawned entry points load modules from /workspace, which can be slow under I/O contention.
  home = await mkdtemp(join(tmpdir(), "gptq-claude-hook-"));
  runtime = join(home, "rt");
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: runtime };
});
afterEach(async () => {
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  publishSessionRecord(readProcess(process.pid)!, null, null, env);
});
afterAll(async () => { await rm(home, { recursive: true, force: true }); });

const run = (bin: string, args: string[], input: string, extra: NodeJS.ProcessEnv = {}) =>
  new Promise<{ stdout: string; code: number | null }>((done) => {
    const child = spawn(join(BIN, bin), args, { env: { ...env, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    child.stdout.on("data", (d) => { stdout += d; });
    child.on("close", (code) => done({ stdout: stdout.trim(), code }));
    child.stdin.end(input);
  });
const hook = (event: string, json: Record<string, unknown>, extra: NodeJS.ProcessEnv = {}) =>
  run("gptqueue-claude-hook", [event], JSON.stringify({ cwd: home, ...json }), extra);

const agent = async (prefix: string) => {
  const client = new RedisClient(null, REDIS_URL);
  clients.push(client);
  return { client, ...(await client.register("both", `${prefix}-${randomUUID().slice(0, 8)}`)) };
};
const send = (sender: RedisClient, to: string, content = "work") =>
  sender.sendMessage({ id: randomUUID(), from: "tester", to, timestamp: new Date().toISOString(), type: "task", payload: { content } });
/** Publish a record as this session's MCP server would; the spawned hook is our child. */
const record = (name: string, sessionId: string) => publishSessionRecord(readProcess(process.pid)!, name, sessionId, env);

describe("Claude hook against Redis", { timeout: 300_000 }, () => {
  it("ID-2: gates the inbox the session's server registered, not the cwd-derived one", async () => {
    const held = await agent("claude-held");
    await send(held.client, held.name);
    const before = await hook("stop", { session_id: randomUUID() });
    // No record: today's resolution (claude-<basename of cwd>) is used, and said to be unconfirmed.
    // No record: the guessed name's counts are not presented as this session's own.
    expect(JSON.parse(before.stdout).systemMessage).toMatch(/UNCONFIRMED \(no session record/);
    expect(before.stdout).not.toContain("unclaimed");
    expect(before.stdout).not.toContain(held.name);
    record(held.name, held.session_id);
    const after = await hook("stop", { session_id: randomUUID() });
    expect(JSON.parse(after.stdout)).toMatchObject({ decision: "block" });
    expect(after.stdout).toContain(`agent '${held.name}' (gptqueue session ${held.session_id}`);
    expect(after.stdout).toContain("1 unclaimed");
  });

  it("AC-4: counts this session's claimed-but-unacked tasks separately (hook and peek)", async () => {
    const me = await agent("claude-claims");
    await send(me.client, me.name);
    const claim = await me.client.taskClaim.claim({ actor_id: me.name, session_id: me.session_id, max_batch: 1, ttl_seconds: 120, now: new Date().toISOString() });
    expect(claim.ok).toBe(true);
    record(me.name, me.session_id);
    const stop = await hook("stop", { session_id: randomUUID() });
    expect(JSON.parse(stop.stdout).reason).toMatch(/0 unclaimed; claimed by this session but not acknowledged: 1; claimed by other sessions: 0/);
    const peek = await run("gptqueue-inbox-peek", ["--json"], "");
    expect(JSON.parse(peek.stdout)).toMatchObject({ agent: me.name, source: "session_record", unclaimed: 0, claimed_by_session: 1, claimed_by_others: 0 });
    expect((await run("gptqueue-inbox-peek", ["--name", me.name], "")).stdout).toBe("0");
  });

  it("AC-4: fails closed but visible when Redis is unreachable", async () => {
    const down = { REDIS_URL: "redis://127.0.0.1:1/0" };
    record("claude-x", "s-down");
    const sid = randomUUID();
    const stop = await hook("stop", { session_id: sid }, { ...down, GPTQ_AGENT_NAME: "claude-x" });
    expect(JSON.parse(stop.stdout)).toMatchObject({ decision: "block" });
    expect(stop.stdout).toMatch(/FAILED \(redis_unavailable/);
    const again = await hook("stop", { session_id: sid, stop_hook_active: true }, { ...down, GPTQ_AGENT_NAME: "claude-x" });
    expect(JSON.parse(again.stdout).systemMessage).toMatch(/still failing/);
    const start = await hook("session-start", { session_id: sid }, { ...down, GPTQ_AGENT_NAME: "claude-x" });
    expect(start.stdout).toMatch(/Inbox check FAILED/);
    const peek = await run("gptqueue-inbox-peek", ["--name", "claude-x"], "", down);
    expect(peek.code).toBe(1);
    expect(peek.stdout).toBe("");
  });

  it("stop_hook_active: does not loop on the same mail but re-blocks for mail that arrived meanwhile", async () => {
    const me = await agent("claude-loop");
    record(me.name, me.session_id);
    const sid = randomUUID();
    await send(me.client, me.name, "one");
    expect(JSON.parse((await hook("stop", { session_id: sid })).stdout).decision).toBe("block");
    const same = await hook("stop", { session_id: sid, stop_hook_active: true });
    expect(JSON.parse(same.stdout).systemMessage).toMatch(/not re-blocking/);
    await send(me.client, me.name, "two");
    expect(JSON.parse((await hook("stop", { session_id: sid })).stdout).decision).toBe("block");
    await send(me.client, me.name, "three");
    const meanwhile = await hook("stop", { session_id: sid, stop_hook_active: true });
    expect(JSON.parse(meanwhile.stdout)).toMatchObject({ decision: "block" });
  });

  it("LT-3/WK-1: the event-driven watcher exits within 300 ms of a send; the Stop hook sees it via its pidfile", async () => {
    const me = await agent("claude-watch");
    record(me.name, me.session_id);
    const sid = randomUUID();
    await mkdir(runtime, { recursive: true, mode: 0o700 });
    const pidfile = watcherPidPath(runtime, sid);

    const noWatcher = await hook("stop", { session_id: sid });
    expect(JSON.parse(noWatcher.stdout).reason).toMatch(/no background inbox watcher[\s\S]*gptqueue-inbox-peek' --watch --session '[^']+' Then stop\.$/);

    const child = spawn(join(BIN, "gptqueue-inbox-peek"), ["--watch", "--name", me.name, "--session", sid], { env, stdio: ["ignore", "pipe", "pipe"] });
    children.push(child);
    let out = "";
    child.stdout!.on("data", (d) => { out += d; });
    const exited = new Promise<number>((done) => child.on("exit", () => done(performance.now())));
    const deadline = Date.now() + 120_000;
    while (!existsSync(pidfile) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25));
    expect(existsSync(pidfile)).toBe(true);
    await new Promise((r) => setTimeout(r, 300)); // let it reach XREAD BLOCK

    expect((await hook("stop", { session_id: sid })).stdout).toBe(""); // watcher running, inbox empty: allow
    expect(child.exitCode).toBeNull(); // the hook's own peek did not consume or wake anything

    const sentAt = performance.now();
    await send(me.client, me.name);
    const latency = (await exited) - sentAt;
    expect(latency).toBeLessThan(300);
    expect(child.exitCode).toBe(0);
    expect(JSON.parse(out)).toMatchObject({ agent: me.name, pending: 1 });
    expect(existsSync(pidfile)).toBe(false);
    expect(await me.client.getQueueDepth(me.name)).toBe(1); // non-consuming
  });

  it("ID-2: bin/gptqueue-claude-mcp publishes the registered name for its Claude parent and removes it on exit", async () => {
    const name = `claude-mcp-${randomUUID().slice(0, 8)}`;
    const recordFile = join(runtime, `session-${process.pid}-${readProcess(process.pid)!.start}.json`);
    const child = spawn(join(BIN, "gptqueue-claude-mcp"), [], { env: { ...env, GPTQ_AGENT_NAME: name }, stdio: ["pipe", "ignore", "pipe"] });
    children.push(child);
    const exited = new Promise<void>((done) => child.on("exit", () => done()));
    const deadline = Date.now() + 120_000;
    while (!existsSync(recordFile) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
    const written = JSON.parse(await readFile(recordFile, "utf8")) as Record<string, unknown>;
    expect(written).toMatchObject({ version: 1, agent: name, claude_pid: process.pid, server_pid: child.pid });
    expect(typeof written.gptqueue_session_id).toBe("string");
    const peek = await run("gptqueue-inbox-peek", ["--json"], "");
    expect(JSON.parse(peek.stdout)).toMatchObject({ agent: name, source: "session_record" });
    child.stdin!.end();
    await exited;
    expect(existsSync(recordFile)).toBe(false);
  });

  it("AC-4: the probe snapshot is atomic: queued + claimed work is conserved while claims race it", async () => {
    // Oracle: a claim moves messages from the list to the claim index in one
    // Lua call and nothing acknowledges here, so in every consistent snapshot
    // unclaimed + claimed_by_session + claimed_by_others == N. Split reads
    // (LLEN, then ZRANGE/HMGET) can straddle a claim and miscount.
    const me = await agent("claude-race");
    const N = 400;
    const probe = probeClient(REDIS_URL);
    try {
      await probe.rpush(SESSION_KEYS.queue(me.name), ...Array.from({ length: N }, (_, i) =>
        JSON.stringify({ id: `m-${i}`, from: "tester", to: me.name, timestamp: new Date().toISOString(), type: "task", payload: { content: `t${i}` } })));
      let claiming = true;
      const claimer = (async () => {
        for (let i = 0; i < N; i += 1) {
          const r = await me.client.taskClaim.claim({ actor_id: me.name, session_id: me.session_id, max_batch: 1, ttl_seconds: 600, now: new Date().toISOString() });
          expect(r.ok).toBe(true);
        }
        claiming = false;
      })();
      const totals = new Set<number>();
      let snapshots = 0;
      while (claiming) {
        const res = await probeInbox(probe, me.name, me.session_id);
        if (!res.ok) throw new Error(res.message);
        totals.add(res.counts.unclaimed + (res.counts.claimed_by_session ?? 0) + res.counts.claimed_by_others);
        snapshots += 1;
      }
      await claimer;
      expect(snapshots).toBeGreaterThan(10);
      expect([...totals]).toEqual([N]);
      const end = await probeInbox(probe, me.name, me.session_id);
      expect(end.ok && end.counts).toMatchObject({ unclaimed: 0, claimed_by_session: N, claimed_by_others: 0 });
    } finally { probe.disconnect(); }
  });

  it("AC-4: a claim landing between any two probe commands cannot produce a zero-work snapshot (deterministic interleaving)", async () => {
    // The probe gets a real Redis client wrapped so that, before every command
    // after its first, one real claim runs. A probe that splits its reads sees
    // the message leave the list but not yet appear in the claim index (or the
    // reverse); a single EVAL has no gap to interleave into.
    const me = await agent("claude-interleave");
    const real = probeClient(REDIS_URL);
    try {
      await real.rpush(SESSION_KEYS.queue(me.name), JSON.stringify({ id: "m-1", from: "tester", to: me.name, timestamp: new Date().toISOString(), type: "task", payload: { content: "x" } }));
      let commands = 0;
      const commandNames = new Set(["eval", "llen", "zrange", "hmget", "hget", "xrevrange", "multi", "exec"]);
      const interleaved = new Proxy(real, {
        get(target, prop, receiver) {
          const value = Reflect.get(target, prop, receiver) as unknown;
          if (typeof prop !== "string" || !commandNames.has(prop.toLowerCase()) || typeof value !== "function") return value;
          return async (...args: unknown[]) => {
            if (commands++ >= 1) {
              await me.client.taskClaim.claim({ actor_id: me.name, session_id: me.session_id, max_batch: 1, ttl_seconds: 600, now: new Date().toISOString() });
            }
            return (value as (...a: unknown[]) => unknown).apply(target, args);
          };
        },
      });
      const res = await probeInbox(interleaved, me.name, me.session_id);
      expect(res.ok).toBe(true);
      const c = res.ok ? res.counts : null;
      expect((c!.unclaimed) + (c!.claimed_by_session ?? 0) + c!.claimed_by_others).toBe(1);
    } finally { real.disconnect(); }
  });
});
