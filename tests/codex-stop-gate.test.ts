/**
 * Loop adapter S1: the Codex Stop gate. Pure decisions, the runner against
 * the private test Redis, and the built hook entry point end to end.
 */
import { testRedisUrl, testRedisTarget } from "./helpers/test-redis.js";
import { spawn } from "node:child_process";
import { createServer, type Server, type Socket } from "node:net";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { Redis } from "ioredis";
import { decideCodexStop, runCodexStop, type CodexStopInput } from "../src/registered-shell/codex-stop-gate.js";
import { recordRunning } from "../src/registered-shell/codex-hook.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { TurnStateStore } from "../src/core/turn-state.js";
import { probeClient, type ProbeResult } from "../src/claude/inbox-probe.js";
import { assertPrivateTestRedis } from "./helpers/redis-test-utils.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "codex-stop-gate");

const counts = (unclaimed: number, latest: string | null = "1-0", expired = 0): ProbeResult =>
  ({ ok: true, counts: { unclaimed, claimed_by_session: null, claimed_by_others: 0, expired_by_others: expired, latest_event_id: latest } });
const input = (overrides: Partial<CodexStopInput> = {}): CodexStopInput =>
  ({ agent: "lead-a", probe: counts(0), stopHookActive: false, prior: null, ...overrides });

describe("Codex Stop gate decisions", () => {
  it("allows an empty inbox and records idle", () => {
    expect(decideCodexStop(input())).toEqual({ kind: "allow", systemMessage: null, idle: true });
  });

  it("blocks pending mail once, naming the agent and the obligation", () => {
    const decision = decideCodexStop(input({ probe: counts(2, "7-0") }));
    expect(decision.kind).toBe("block");
    if (decision.kind !== "block") return;
    expect(decision.record).toEqual({ reason: "mail", event_id: "7-0" });
    expect(decision.reason).toContain('"lead-a"');
    expect(decision.reason).toContain("2 unclaimed");
    expect(decision.reason).toContain("claim_tasks");
  });

  it("gates on expired claims too", () => {
    expect(decideCodexStop(input({ probe: counts(0, "3-0", 1) })).kind).toBe("block");
  });

  it("does not re-block a continued turn when no new mail arrived (no spin trap)", () => {
    const decision = decideCodexStop(input({ probe: counts(2, "7-0"), stopHookActive: true, prior: { reason: "mail", event_id: "7-0" } }));
    expect(decision.kind).toBe("allow");
    if (decision.kind === "allow") {
      expect(decision.systemMessage).toContain("not re-blocking");
      expect(decision.idle).toBe(true);
    }
  });

  it("blocks again when new mail arrived during the continued turn", () => {
    const decision = decideCodexStop(input({ probe: counts(3, "9-0"), stopHookActive: true, prior: { reason: "mail", event_id: "7-0" } }));
    expect(decision.kind === "block" && decision.record.event_id).toBe("9-0");
  });

  it("blocks once on a failed inbox check, then lets the stop through without claiming idle", () => {
    const failed: ProbeResult = { ok: false, code: "redis_unavailable", message: "connection refused" } as ProbeResult;
    const first = decideCodexStop(input({ probe: failed }));
    expect(first.kind === "block" && first.record).toEqual({ reason: "error", event_id: null });
    const second = decideCodexStop(input({ probe: failed, stopHookActive: true, prior: { reason: "error", event_id: null } }));
    expect(second).toMatchObject({ kind: "allow", idle: false });
  });
});

describe("Codex Stop gate against Redis", { timeout: 60_000 }, () => {
  let redis: Redis;
  let logDir = "";
  const clients: RedisClient[] = [];
  const keys: string[] = [];

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
    logDir = await mkdtemp(join(tmpdir(), "gptq-stop-gate-"));
  });
  afterAll(async () => {
    for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
    if (keys.length > 0) await redis.del(...keys);
    redis.disconnect();
    await rm(logDir, { recursive: true, force: true });
  });

  /** A registered agent bound to a fresh Codex thread through the runtime-mailbox mapping. */
  const boundThread = async () => {
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const { name } = await client.register("both", `codex-gate-${randomUUID().slice(0, 8)}`);
    const thread = randomUUID();
    const mapping = SESSION_KEYS.runtimeMailbox("codex", thread);
    await redis.set(mapping, JSON.stringify({ agent: name, working_directory: "/workspace/project" }));
    keys.push(mapping, SESSION_KEYS.stopGate("codex", thread), SESSION_KEYS.turnState(name));
    const send = () => client.sendMessage({ id: randomUUID(), from: "tester", to: name, timestamp: new Date().toISOString(), type: "task", payload: { content: "work" } });
    return { name, thread, send };
  };

  it("returns {} for a thread not bound to gptqueue, and writes nothing", async () => {
    const thread = randomUUID();
    expect(await runCodexStop({ session_id: thread }, redis)).toBe("{}");
    expect(await redis.exists(SESSION_KEYS.stopGate("codex", thread))).toBe(0);
  });

  it("R1: a Stop with no mapping on a reachable Redis emits one observable stderr event", async () => {
    // A mis-probe (the hook on a different Redis than the sidecar) looks exactly
    // like an unbound thread: no mapping, Redis reachable. One stderr event makes it diagnosable.
    const thread = randomUUID();
    const writes: string[] = [];
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation((chunk: Uint8Array | string) => {
      writes.push(String(chunk));
      return true;
    });
    try {
      expect(await runCodexStop({ session_id: thread }, redis)).toBe("{}");
    } finally {
      stderr.mockRestore();
    }
    const events = writes.flatMap((line) => {
      try { return [JSON.parse(line) as Record<string, unknown>]; } catch { return []; }
    });
    expect(events).toContainEqual(expect.objectContaining({ event: "codex_stop_gate_no_mapping", session_id: thread }));
  });

  it("R1: recordRunning reports through the configured URL, not REDIS_URL, when they differ", async () => {
    const { name, thread } = await boundThread();
    const wrongDb = `${testRedisTarget().path}?db=14`; // same private server, no mapping there
    vi.stubEnv("REDIS_URL", wrongDb);
    try {
      await recordRunning(
        { session_id: thread, cwd: "/workspace/project", hook_event_name: "UserPromptSubmit" },
        new Date().toISOString(), undefined, REDIS_URL,
      );
    } finally {
      vi.unstubAllEnvs();
    }
    expect((await new TurnStateStore(redis).read(name))?.phase).toBe("running");
  });

  it("fails open with {} when the binding cannot be read", async () => {
    const broken = { get: async () => { throw new Error("ECONNREFUSED"); } } as unknown as Redis;
    expect(await runCodexStop({ session_id: randomUUID() }, broken)).toBe("{}");
  });

  it("blocks once per new mail, then allows and records idle", async () => {
    const { name, thread, send } = await boundThread();
    await send();
    const first = JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: false }, redis));
    expect(first.decision).toBe("block");
    expect(first.reason).toContain(name);

    const continued = JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: true, turn_id: "t-1" }, redis));
    expect(continued.decision).toBeUndefined();
    expect(continued.systemMessage).toContain("not re-blocking");
    expect(await new TurnStateStore(redis).read(name)).toMatchObject({ phase: "idle", runtime: "codex", session_id: thread, turn_id: "t-1" });

    await send();
    const again = JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: true }, redis));
    expect(again.decision).toBe("block");
  });

  it("allows an empty inbox with {} and records idle", async () => {
    const { name, thread } = await boundThread();
    expect(await runCodexStop({ session_id: thread }, redis)).toBe("{}");
    expect((await new TurnStateStore(redis).read(name))?.phase).toBe("idle");
  });

  /** The real client, except that the named commands throw (bookkeeping failure). */
  const failing = (...methods: string[]): Redis => new Proxy(redis, {
    get(target, prop, receiver) {
      if (typeof prop === "string" && methods.includes(prop)) return async () => { throw new Error(`injected ${prop} failure`); };
      const value = Reflect.get(target, prop, receiver) as unknown;
      return typeof value === "function" ? (value as (...a: unknown[]) => unknown).bind(target) : value;
    },
  });

  it("F2: does not block when the block marker cannot be recorded (no re-block loop)", async () => {
    const { thread, send } = await boundThread();
    await send();
    const out = JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: false }, failing("set")));
    expect(out.decision).toBeUndefined();
    expect(out.systemMessage).toContain("could not be recorded");
  });

  it("F2: keeps the allow path's systemMessage when its bookkeeping fails", async () => {
    const { thread, send } = await boundThread();
    await send();
    expect(JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: false }, redis)).decision).toBe("block");
    const out = JSON.parse(await runCodexStop({ session_id: thread, stop_hook_active: true }, failing("del")));
    expect(out.systemMessage).toContain("not re-blocking");
  });

  it("S2: a prompt hook that completes after Stop leaves the agent idle", async () => {
    const { name, thread } = await boundThread();
    const promptAt = new Date(Date.now() - 5_000).toISOString(); // captured when the prompt hook process started
    expect(await runCodexStop({ session_id: thread }, redis, () => new Date(Date.now() - 1_000))).toBe("{}");
    await recordRunning({ session_id: thread, cwd: "/workspace/project", hook_event_name: "UserPromptSubmit" }, promptAt, redis);
    expect((await new TurnStateStore(redis).read(name))?.phase).toBe("idle");
  });

  it("works end to end through the built gptqueue-codex-hook", async () => {
    const { thread, send } = await boundThread();
    await send();
    const hook = (payload: Record<string, unknown>) => new Promise<{ stdout: string; code: number | null }>((done) => {
      const child = spawn(process.execPath, [resolve(__dirname, "..", "bin", "gptqueue-codex-hook")], {
        env: { PATH: process.env.PATH, HOME: process.env.HOME, REDIS_URL, GPTQ_LOG_DIR: logDir }, stdio: ["pipe", "pipe", "ignore"],
      });
      let stdout = "";
      child.stdout.on("data", (d) => { stdout += d; });
      child.on("close", (code) => done({ stdout: stdout.trim(), code }));
      child.stdin.end(JSON.stringify(payload));
    });
    const base = { cwd: "/workspace/project", hook_event_name: "Stop", model: "gpt-6-luna", permission_mode: "default", transcript_path: null, last_assistant_message: "DONE", turn_id: "t-9" };
    const bound = await hook({ ...base, session_id: thread, stop_hook_active: false });
    expect(bound.code).toBe(0);
    expect(JSON.parse(bound.stdout)).toMatchObject({ decision: "block" });
    const unbound = await hook({ ...base, session_id: randomUUID(), stop_hook_active: false });
    expect(unbound.code).toBe(0);
    expect(unbound.stdout).toBe("{}");
  });

  it("R1: the built hook probes --redis-url even when REDIS_URL points elsewhere", async () => {
    const { thread, send } = await boundThread();
    await send();
    const wrongDb = `${testRedisTarget().path}?db=14`;
    const out = await new Promise<{ stdout: string; code: number | null }>((done) => {
      const child = spawn(process.execPath, [resolve(__dirname, "..", "bin", "gptqueue-codex-hook"), "--redis-url", REDIS_URL], {
        env: { PATH: process.env.PATH, HOME: process.env.HOME, REDIS_URL: wrongDb, GPTQ_LOG_DIR: logDir }, stdio: ["pipe", "pipe", "ignore"],
      });
      let stdout = "";
      child.stdout.on("data", (d) => { stdout += d; });
      child.on("close", (code) => done({ stdout: stdout.trim(), code }));
      child.stdin.end(JSON.stringify({ session_id: thread, cwd: "/workspace/project", hook_event_name: "Stop", stop_hook_active: false }));
    });
    expect(out.code).toBe(0);
    // Without the URL the hook probes REDIS_URL (db 14: no mapping) and silently allows every Stop.
    expect(JSON.parse(out.stdout)).toMatchObject({ decision: "block" });
  });
});

describe("Codex Stop gate command timeout (F8)", () => {
  let server: Server;
  let hungDir: string;
  let hungSocket: string;
  const sockets: Socket[] = [];
  beforeAll(async () => {
    // Accepts connections and never answers: a hung Redis, on a private unix socket (no TCP).
    hungDir = await mkdtemp(join(tmpdir(), "gptq-hung-redis-"));
    hungSocket = join(hungDir, "hung.sock");
    server = createServer((socket) => { sockets.push(socket); });
    await new Promise<void>((resolveListen) => server.listen(hungSocket, resolveListen));
  });
  afterAll(async () => {
    for (const s of sockets) s.destroy();
    await new Promise((resolveClose) => server.close(resolveClose));
    await rm(hungDir, { recursive: true, force: true });
  });

  it("returns {} within about 3.5 s when Redis never answers", async () => {
    const client = probeClient(`${hungSocket}?db=15`, { commandTimeoutMs: 3000 });
    try {
      const started = Date.now();
      expect(await runCodexStop({ session_id: randomUUID() }, client)).toBe("{}");
      expect(Date.now() - started).toBeLessThan(3500);
    } finally {
      client.disconnect();
    }
  }, 10_000);
});

describe("Codex Stop gate end-to-end deadline (S1)", () => {
  it("fails open with {} within about 4.5 s when every Redis reply takes 1.5 s", async () => {
    const slow = <T>(value: T) => new Promise<T>((resolveSlow) => setTimeout(() => resolveSlow(value), 1_500));
    let disconnected = false;
    const fake = {
      get: (key: string) => slow(key.startsWith("gptq:runtime-mailbox:") ? JSON.stringify({ agent: "lead-slow", working_directory: "/w" }) : null),
      eval: () => slow([0, "", [], []]),
      set: () => slow("OK"),
      del: () => slow(1),
      disconnect: () => { disconnected = true; },
    } as unknown as Redis;
    const started = Date.now();
    expect(await runCodexStop({ session_id: randomUUID() }, fake)).toBe("{}");
    expect(Date.now() - started).toBeLessThan(4_500);
    expect(disconnected).toBe(true);
  }, 10_000);
});
