/**
 * Worker mode (f-1965) and the AskUserQuestion guard (f-1915) through the real
 * bin/ entry points (built dist) against the private test Redis (db 15).
 */
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, readdirSync } from "node:fs";
import { chmod, mkdtemp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { probeClient } from "../src/claude/inbox-probe.js";
import { publishSessionRecord, readProcess } from "../src/claude/session-record.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { WORKER_SESSION_CONTEXT } from "../src/claude/worker-mode.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const DOWN = "redis://127.0.0.1:1/0";
const BIN = resolve(__dirname, "..", "bin");
let home = "";
let runtime = "";
let env: NodeJS.ProcessEnv = {};
const clients: RedisClient[] = [];
const children: ChildProcess[] = [];

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-claude-worker-"));
  runtime = join(home, "rt");
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: runtime, GPTQ_CLAUDE_LEGACY_SCAN: "off" };
});
afterEach(async () => {
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  publishSessionRecord(readProcess(process.pid)!, null, null, env);
});
afterAll(async () => { await rm(home, { recursive: true, force: true }); });

const run = (bin: string, args: string[], input: string, extra: NodeJS.ProcessEnv = {}) =>
  new Promise<{ stdout: string; code: number | null; ms: number }>((done) => {
    const started = Date.now();
    const child = spawn(join(BIN, bin), args, { env: { ...env, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    child.stdout.on("data", (d) => { stdout += d; });
    child.on("close", (code) => done({ stdout: stdout.trim(), code, ms: Date.now() - started }));
    child.stdin.end(input);
  });
const hook = (event: string, json: Record<string, unknown>, extra: NodeJS.ProcessEnv = {}) =>
  run("gptqueue-claude-hook", [event], JSON.stringify({ cwd: home, ...json }), extra);
const ask = (sid: string, extra: NodeJS.ProcessEnv = {}, tool = "AskUserQuestion") =>
  hook("pre-tool-use", { session_id: sid, hook_event_name: "PreToolUse", tool_name: tool, tool_input: { questions: [] } }, extra);

const agent = async (prefix: string) => {
  const client = new RedisClient(null, REDIS_URL);
  clients.push(client);
  return { client, ...(await client.register("both", `${prefix}-${randomUUID().slice(0, 8)}`)) };
};
const send = (sender: RedisClient, to: string, content = "work") =>
  sender.sendMessage({ id: randomUUID(), from: "tester", to, timestamp: new Date().toISOString(), type: "task", payload: { content } });
const record = (name: string, sessionId: string) => publishSessionRecord(readProcess(process.pid)!, name, sessionId, env);
const runtimeFiles = (): string[] => (existsSync(runtime) ? readdirSync(runtime) : []);

describe("headless worker mode", { timeout: 300_000 }, () => {
  it("all four hook events are no-ops without touching Redis or writing state", async () => {
    const me = await agent("claude-worker");
    record(me.name, me.session_id);
    await send(me.client, me.name);
    const sid = randomUUID();
    const before = runtimeFiles();
    // Unreachable Redis: a non-worker run would report FAILED / block; a worker run never connects.
    for (const worker of [{ CLAUDE_CODE_ENTRYPOINT: "sdk-cli" }, { GPTQUEUE_WORKER: "1", CLAUDE_CODE_ENTRYPOINT: "cli" }]) {
      const extra = { ...worker, REDIS_URL: DOWN };
      const start = await hook("session-start", { session_id: sid }, extra);
      expect(start.code).toBe(0);
      expect(JSON.parse(start.stdout)).toEqual({ hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: WORKER_SESSION_CONTEXT } });
      for (const result of [
        await hook("stop", { session_id: sid }, extra),
        await hook("stop", { session_id: sid, stop_hook_active: true }, extra),
        await hook("user-prompt-submit", { session_id: sid }, extra),
        await ask(sid, extra),
      ]) {
        expect(result).toMatchObject({ code: 0, stdout: "" });
      }
    }
    expect(runtimeFiles().filter((f) => !before.includes(f))).toEqual([]);
    const redis = probeClient(REDIS_URL);
    try { expect(await redis.exists(`gptq:turn-state:${me.name}`)).toBe(0); } finally { redis.disconnect(); }
    // Control: the same session record and unreachable Redis outside worker mode fails closed.
    const control = await hook("stop", { session_id: randomUUID() }, { CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "0", REDIS_URL: DOWN });
    expect(JSON.parse(control.stdout)).toMatchObject({ decision: "block" });
  });

  const mcp = async (extra: NodeJS.ProcessEnv) => {
    const project = join(home, `wk-${randomUUID().slice(0, 8)}`);
    await mkdir(project, { recursive: true });
    const derived = `claude-${project.split("/").pop()!}`;
    const child = spawn(join(BIN, "gptqueue-claude-mcp"), [], { env: { ...env, CLAUDE_PROJECT_DIR: project, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
    children.push(child);
    let stderr = "";
    let stdout = "";
    child.stderr!.on("data", (d) => { stderr += d; });
    child.stdout!.on("data", (d) => { stdout += d; });
    const recordFile = join(runtime, `session-${process.pid}-${readProcess(process.pid)!.start}.json`);
    const rpc = (id: number, method: string, params: unknown) => child.stdin!.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
    const response = async (id: number): Promise<Record<string, unknown>> => {
      const deadline = Date.now() + 120_000;
      while (Date.now() < deadline) {
        const line = stdout.split("\n").find((l) => l.includes(`"id":${id}`));
        if (line) return JSON.parse(line) as Record<string, unknown>;
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error(`no response ${id}; stderr: ${stderr}`);
    };
    rpc(1, "initialize", { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "test", version: "0" } });
    await response(1);
    child.stdin!.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n`);
    return { child, derived, recordFile, rpc, response, stderr: () => stderr };
  };

  it("the MCP server does not auto-register a print-mode worker, yet explicit register_agent works", async () => {
    const s = await mcp({ CLAUDE_CODE_ENTRYPOINT: "sdk-cli" });
    expect(s.stderr()).toContain("worker_session_unregistered");
    s.rpc(2, "tools/call", { name: "get_queue_status", arguments: {} });
    await s.response(2);
    const redis = probeClient(REDIS_URL);
    try {
      expect(await redis.hexists(SESSION_KEYS.registry, s.derived)).toBe(0);
      expect(existsSync(s.recordFile)).toBe(false);
      const explicit = `claude-explicit-${randomUUID().slice(0, 8)}`;
      s.rpc(3, "tools/call", { name: "register_agent", arguments: { name: explicit, role: "both", description: "explicit worker registration" } });
      const reg = await s.response(3);
      expect(JSON.stringify(reg)).toContain(explicit);
      expect((reg.result as { isError?: boolean }).isError ?? false).toBe(false);
      expect(await redis.hexists(SESSION_KEYS.registry, explicit)).toBe(1);
      expect(existsSync(s.recordFile)).toBe(false);
      s.rpc(4, "tools/call", { name: "unregister_agent", arguments: {} });
      await s.response(4);
    } finally { redis.disconnect(); }
  });

  it.each([
    ["GPTQUEUE_WORKER=0", { CLAUDE_CODE_ENTRYPOINT: "sdk-cli", GPTQUEUE_WORKER: "0" }, false],
    ["GPTQ_AGENT_NAME", { CLAUDE_CODE_ENTRYPOINT: "sdk-cli" }, true],
  ])("the MCP server still auto-registers a print-mode session with %s", async (_label, extra, named) => {
    const name = `claude-named-${randomUUID().slice(0, 8)}`;
    const s = await mcp(named ? { ...extra, GPTQ_AGENT_NAME: name } : extra);
    const expected = named ? name : s.derived;
    const deadline = Date.now() + 120_000;
    while (!existsSync(s.recordFile) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
    const written = JSON.parse(await readFile(s.recordFile, "utf8")) as Record<string, unknown>;
    expect(written).toMatchObject({ agent: expected, server_pid: s.child.pid });
    expect(s.stderr()).not.toContain("worker_session_unregistered");
    const exited = new Promise<void>((done) => s.child.on("exit", () => done()));
    s.child.stdin!.end();
    await exited;
  });
});

describe("plain MCP server entry under worker mode", { timeout: 300_000 }, () => {
  it("skips an inherited GPTQ_AGENT_NAME under GPTQUEUE_WORKER=1 but registers a command-line name", async () => {
    const inherited = `worker-env-${randomUUID().slice(0, 8)}`;
    const argvName = `worker-argv-${randomUUID().slice(0, 8)}`;
    const root = resolve(__dirname, "..");
    const start = (args: string[], extra: NodeJS.ProcessEnv) => {
      const child = spawn(process.execPath, ["dist/mcp-server/index.js", ...args], { cwd: root, env: { ...env, ...extra }, stdio: ["pipe", "ignore", "pipe"] });
      children.push(child);
      return child;
    };
    start([], { GPTQUEUE_WORKER: "1", GPTQ_AGENT_NAME: inherited });
    start([argvName], { GPTQUEUE_WORKER: "1" });
    const redis = probeClient(REDIS_URL);
    try {
      const deadline = Date.now() + 120_000;
      while (!(await redis.hexists(SESSION_KEYS.registry, argvName)) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
      expect(await redis.hexists(SESSION_KEYS.registry, argvName)).toBe(1);
      await new Promise((r) => setTimeout(r, 1_000)); // both started together; give the env-named one time it would need
      expect(await redis.hexists(SESSION_KEYS.registry, inherited)).toBe(0);
    } finally { redis.disconnect(); }
  });
});

describe("AskUserQuestion guard (pre-tool-use)", { timeout: 300_000 }, () => {
  const askFile = (sid: string) => join(runtime, `ask-${sid}.json`);

  it("ignores other tools, passes with no mail, denies once per mail event", async () => {
    const me = await agent("claude-ask");
    record(me.name, me.session_id);
    const sid = randomUUID();
    expect(await ask(sid, {}, "Bash")).toMatchObject({ code: 0, stdout: "" });
    expect(await ask(sid)).toMatchObject({ code: 0, stdout: "" });
    expect(existsSync(askFile(sid))).toBe(false);
    await send(me.client, me.name, "one");
    const denied = await ask(sid);
    const out = JSON.parse(denied.stdout) as { hookSpecificOutput: Record<string, string> };
    expect(out.hookSpecificOutput).toMatchObject({ hookEventName: "PreToolUse", permissionDecision: "deny" });
    expect(out.hookSpecificOutput.permissionDecisionReason).toContain(me.name);
    expect(out.hookSpecificOutput.permissionDecisionReason).toContain("1 unclaimed");
    expect((await stat(askFile(sid))).mode & 0o777).toBe(0o600);
    // Same mail: the retry goes through (no output, never an explicit "allow").
    expect(await ask(sid)).toMatchObject({ code: 0, stdout: "" });
    expect(await ask(sid, {}, "Read")).toMatchObject({ code: 0, stdout: "" });
    await send(me.client, me.name, "two");
    const again = JSON.parse((await ask(sid)).stdout) as { hookSpecificOutput: Record<string, string> };
    expect(again.hookSpecificOutput.permissionDecision).toBe("deny");
    expect(again.hookSpecificOutput.permissionDecisionReason).toContain("2 unclaimed");
    // Another session with the same inbox gets its own denial.
    expect(JSON.parse((await ask(randomUUID())).stdout).hookSpecificOutput.permissionDecision).toBe("deny");
  });

  it("never denies on a failed probe or an unconfirmed identity", async () => {
    const me = await agent("claude-ask-safe");
    await send(me.client, me.name);
    const sid = randomUUID();
    // No session record: the cwd-derived / env name is a guess.
    expect(await ask(sid, { GPTQ_AGENT_NAME: me.name })).toMatchObject({ code: 0, stdout: "" });
    record(me.name, me.session_id);
    const down = await ask(sid, { REDIS_URL: DOWN });
    expect(down.code).toBe(0);
    const out = JSON.parse(down.stdout) as Record<string, unknown>;
    expect(out).not.toHaveProperty("hookSpecificOutput");
    expect(out.systemMessage).toMatch(/FAILED \(redis_unavailable/);
    expect(existsSync(askFile(sid))).toBe(false);
  });

  // F1: the guard fails open whenever it cannot record the denial, so it never denies twice for one event.
  const denies = (r: { stdout: string }) => r.stdout.includes('"permissionDecision":"deny"');
  const inactive = (r: { stdout: string }) => (r.stdout ? String((JSON.parse(r.stdout) as Record<string, unknown>).systemMessage ?? "") : "");

  it("F1: no usable session_id never denies", async () => {
    const me = await agent("claude-ask-nosid");
    record(me.name, me.session_id);
    await send(me.client, me.name);
    for (const sid of [undefined, "", "unknown", "../x"]) {
      for (let i = 0; i < 2; i += 1) {
        const r = await hook("pre-tool-use", { ...(sid === undefined ? {} : { session_id: sid }), tool_name: "AskUserQuestion", tool_input: {} });
        expect(r.code).toBe(0);
        expect(denies(r)).toBe(false);
        expect(inactive(r)).toMatch(/guard inactive \(no usable session_id\)/);
      }
    }
  });

  it("F1: an unsafe runtime dir never denies", async () => {
    const me = await agent("claude-ask-unsafe");
    record(me.name, me.session_id);
    await send(me.client, me.name);
    const sid = randomUUID();
    await chmod(runtime, 0o755);
    try {
      for (let i = 0; i < 2; i += 1) {
        const r = await ask(sid);
        expect(r.code).toBe(0);
        expect(denies(r)).toBe(false);
      }
    } finally { await chmod(runtime, 0o700); }
  });

  it("F1: a denial that cannot be recorded is not emitted; an existing record is forced to 0600", async () => {
    const me = await agent("claude-ask-wfail");
    record(me.name, me.session_id);
    await send(me.client, me.name);
    const blocked = randomUUID();
    await mkdir(askFile(blocked)); // not a regular file: the record cannot be written
    for (let i = 0; i < 2; i += 1) {
      const r = await ask(blocked);
      expect(denies(r)).toBe(false);
      expect(inactive(r)).toMatch(/could not record the denial/);
    }
    const readOnly = randomUUID();
    await writeFile(askFile(readOnly), JSON.stringify({ event_id: "0-1" }), { mode: 0o400 });
    if (process.getuid?.() !== 0) {
      for (let i = 0; i < 2; i += 1) expect(denies(await ask(readOnly))).toBe(false);
    }
    const loose = randomUUID();
    await writeFile(askFile(loose), JSON.stringify({ event_id: "0-1" }), { mode: 0o644 });
    await chmod(askFile(loose), 0o644);
    expect(denies(await ask(loose))).toBe(true);
    expect((await stat(askFile(loose))).mode & 0o777).toBe(0o600);
    expect(denies(await ask(loose))).toBe(false);
  });
});

