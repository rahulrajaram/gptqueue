import { mkdtemp, rm, stat, writeFile, mkdir, chmod } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { matchNamesFile, resolveAgentName } from "../src/claude/agent-name.js";
import { decideHook, type HookInput } from "../src/claude/hook.js";
import { tallyClaims, resolveIdentity, type ProbeResult } from "../src/claude/inbox-probe.js";
import { fallbackName, registerClaudeAware } from "../src/claude/mcp-record.js";
import { ancestry, ensureRuntimeDir, findSessionRecord, parseProcStat, publishSessionRecord, readProcess, RuntimeDirError } from "../src/claude/session-record.js";

const dirs: string[] = [];
afterEach(async () => { await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true }))); });
const temp = async () => { const d = await mkdtemp(join(tmpdir(), "gptq-claude-unit-")); dirs.push(d); return d; };

const ok = (unclaimed: number, mine: number | null = 0, others = 0, latest: string | null = "1-0"): ProbeResult =>
  ({ ok: true, counts: { unclaimed, claimed_by_session: mine, claimed_by_others: others, latest_event_id: latest } });
const recorded = { agent: "claude-held", gptqueue_session_id: "s-1", source: "session_record", note: null } as const;
const base = (over: Partial<HookInput> = {}): HookInput => ({
  event: "stop", identity: recorded, probe: ok(0), stopHookActive: false, prior: null,
  watcherRunning: true, watchPolicy: "require", watchCommand: "peek --watch", ...over,
});

describe("Claude agent name resolution (port of gptqueue-claude-name)", () => {
  it("prefers GPTQ_AGENT_NAME, then the longest names-file prefix, then claude-<git top>", async () => {
    const home = await temp();
    await mkdir(join(home, ".config", "gptqueue"), { recursive: true });
    await writeFile(join(home, ".config", "gptqueue", "claude-agent-names.tsv"), "# comment\n/work\tshort\n/work/proj/\tlonger\n");
    const env = { HOME: home } as NodeJS.ProcessEnv;
    expect(resolveAgentName("/work/proj/sub", { ...env, GPTQ_AGENT_NAME: "explicit" })).toBe("explicit");
    expect(resolveAgentName("/work/proj/sub", env)).toBe("longer");
    expect(resolveAgentName("/work/other", env)).toBe("short");
    expect(resolveAgentName(join(home, "My_Dir"), env)).toBe("claude-my-dir");
    expect(matchNamesFile("/workx", "/work\tw\n")).toBeNull();
  });
});

describe("session record (ID-2)", () => {
  it("parses /proc stat with spaces and parens in comm", () => {
    const fields = Array.from({ length: 50 }, (_, i) => String(i));
    fields[1] = "77"; fields[19] = "123456";
    expect(parseProcStat(5, `5 (a) b (c) ${fields.join(" ")}`)).toEqual({ pid: 5, ppid: 77, start: "123456" });
  });

  it("is found through the reader's ancestry and ignored when the server is gone", async () => {
    const env = { GPTQ_CLAUDE_RUNTIME_DIR: join(await temp(), "rt") } as NodeJS.ProcessEnv;
    const parent = readProcess(process.ppid)!;
    publishSessionRecord(parent, "claude-held", "s-1", env);
    expect((await stat(env.GPTQ_CLAUDE_RUNTIME_DIR!)).mode & 0o777).toBe(0o700);
    const chain = ancestry(process.pid);
    const found = findSessionRecord(chain, env);
    expect(found.kind === "found" && found.record.agent).toBe("claude-held");
    expect(resolveIdentity("/somewhere/else", { ...env, GPTQ_AGENT_NAME: "derived" }).agent).toBe("claude-held");
    expect(findSessionRecord(chain, env, () => false).kind).toBe("stale");
    const fallback = resolveIdentity("/x", { ...env, GPTQ_AGENT_NAME: "derived" }, { kind: "none" });
    expect(fallback).toMatchObject({ agent: "derived", source: "env", gptqueue_session_id: null });
    expect(fallback.note).toMatch(/no session record/);
    publishSessionRecord(parent, null, null, env);
    expect(findSessionRecord(chain, env).kind).toBe("none");
  });

  it("treats a record whose server pid is live but has a different start time as stale (pid reuse)", async () => {
    const env = { GPTQ_CLAUDE_RUNTIME_DIR: join(await temp(), "rt") } as NodeJS.ProcessEnv;
    const parent = readProcess(process.ppid)!;
    const dir = ensureRuntimeDir(env);
    const fabricated = { version: 1, agent: "claude-stale", gptqueue_session_id: "s-old", claude_pid: parent.pid, claude_start: parent.start,
      server_pid: process.pid, server_start: `${readProcess(process.pid)!.start}9`, written_at: new Date().toISOString() };
    await writeFile(join(dir, `session-${parent.pid}-${parent.start}.json`), JSON.stringify(fabricated));
    const found = findSessionRecord(ancestry(process.pid), env);
    expect(found.kind).toBe("stale");
    expect(found.kind === "stale" && found.reason).toMatch(/not running/);
    await writeFile(join(dir, `session-${parent.pid}-${parent.start}.json`), JSON.stringify({ ...fabricated, server_start: readProcess(process.pid)!.start }));
    expect(findSessionRecord(ancestry(process.pid), env).kind).toBe("found");
  });

  it("refuses a runtime dir accessible to group or other", async () => {
    const dir = join(await temp(), "rt");
    await mkdir(dir); await chmod(dir, 0o755);
    expect(() => ensureRuntimeDir({ GPTQ_CLAUDE_RUNTIME_DIR: dir })).toThrow(RuntimeDirError);
  });
});

describe("claim tally (AC-4)", () => {
  it("splits this session's claimed tasks from other sessions' and ignores corrupt entries", () => {
    const claims = [JSON.stringify({ session_id: "me", tasks: ["a", "b"] }), JSON.stringify({ session_id: "other", tasks: ["c"] }), "{bad", null];
    expect(tallyClaims(claims, "me")).toEqual({ mine: 2, others: 1 });
    expect(tallyClaims(claims, null)).toEqual({ mine: null, others: 3 });
  });
});

describe("hook decisions", () => {
  it("blocks on claimed-but-unacked work even when the inbox list is empty (AC-4)", () => {
    const d = decideHook(base({ probe: ok(0, 2) }));
    expect(d.kind).toBe("block");
    expect(d.kind === "block" && d.reason).toMatch(/claimed by this session but not acknowledged: 2/);
  });

  it("fails closed but visible on a Redis error, then lets the re-entered stop through (AC-4)", () => {
    const err: ProbeResult = { ok: false, code: "redis_unavailable", message: "ECONNREFUSED" };
    const first = decideHook(base({ probe: err }));
    expect(first.kind === "block" && first.reason).toMatch(/FAILED \(redis_unavailable/);
    const again = decideHook(base({ probe: err, stopHookActive: true, prior: { reason: "error", event_id: null } }));
    expect(again.kind === "allow" && again.systemMessage).toMatch(/still failing/);
    const start = decideHook(base({ event: "session-start", probe: err }));
    expect(start.kind === "context" && start.text).toMatch(/do not assume the inbox is empty/);
  });

  it("re-blocks a re-entered stop only when new mail arrived since the last block", () => {
    const prior = { reason: "mail", event_id: "5-0" } as const;
    const same = decideHook(base({ probe: ok(1, 0, 0, "5-0"), stopHookActive: true, prior }));
    expect(same.kind === "allow" && same.systemMessage).toMatch(/not re-blocking/);
    const newer = decideHook(base({ probe: ok(2, 0, 0, "6-0"), stopHookActive: true, prior }));
    expect(newer.kind).toBe("block");
  });

  it("asks for the background watcher when none is running, once (WK-1)", () => {
    const d = decideHook(base({ watcherRunning: false }));
    expect(d.kind === "block" && d.reason).toMatch(/no background inbox watcher.*peek --watch/s);
    expect(decideHook(base({ watcherRunning: false, stopHookActive: true })).kind).toBe("allow");
    expect(decideHook(base({ watcherRunning: false, watchPolicy: "off" })).kind).toBe("allow");
  });

  it("states when the name is unconfirmed (no session record)", () => {
    const d = decideHook(base({ event: "session-start", identity: { agent: "claude-x", gptqueue_session_id: null, source: "derived", note: "no session record" } }));
    expect(d.kind === "context" && d.text).toMatch(/UNCONFIRMED: no session record/);
  });
});

describe("AGENT_NAME_HELD fallback (D2 contract, faked at the boundary)", () => {
  const held = Object.assign(new Error("held"), { code: "AGENT_NAME_HELD", details: { name: "claude-p", holder_session_id: "s-old" } });
  const registrar = (failFor: readonly string[]) => {
    const calls: string[] = [];
    return { calls, register: async (_role: string, name: string) => { calls.push(name); if (failFor.includes(name)) throw held; return { name, session_id: `sid-${name}` }; } };
  };
  const claudeEnv = { GPTQ_CLAUDE_SESSION_RECORD: "1", CLAUDE_SESSION_ID: "abcdef12-3456" } as NodeJS.ProcessEnv;

  it("registers <name>-<session id prefix> when the name is held", async () => {
    const r = registrar(["claude-p"]);
    await expect(registerClaudeAware(r, "claude-p", claudeEnv)).resolves.toEqual({ name: "claude-p-abcdef12", session_id: "sid-claude-p-abcdef12" });
    expect(r.calls).toEqual(["claude-p", "claude-p-abcdef12"]);
    expect(fallbackName("n", { GPTQ_CLAUDE_SESSION_RECORD: "1" }, 4242)).toBe("n-4242");
  });

  it("does not fall back when sharing is requested, outside Claude, or for other errors", async () => {
    await expect(registerClaudeAware(registrar(["n"]), "n", { ...claudeEnv, GPTQ_AGENT_SHARED: "1" })).rejects.toBe(held);
    await expect(registerClaudeAware(registrar(["n"]), "n", {})).rejects.toBe(held);
    const boom = { register: async () => { throw new Error("redis down"); } };
    await expect(registerClaudeAware(boom, "n", claudeEnv)).rejects.toThrow("redis down");
  });
});
