/**
 * Legacy identity confirmation: a running Claude session whose MCP server
 * predates session records is identified from that server's environment.
 * Fake process trees through an injected /proc reader, plus one real /proc check.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
import { findLegacyIdentity, type ProcReader } from "../src/claude/legacy-identity.js";
import { resolveIdentity } from "../src/claude/inbox-probe.js";
import { decideHook } from "../src/claude/hook.js";
import { readProcess, type ProcessInfo } from "../src/claude/session-record.js";

type FakeProc = { pid: number; ppid: number; uid?: number; argv?: string[]; env?: Record<string, string> };
const ME = 1000;
const fakeReader = (procs: readonly FakeProc[]): ProcReader => {
  const byPid = new Map(procs.map((p) => [p.pid, p]));
  return {
    pids: () => procs.map((p) => p.pid),
    stat: (pid) => { const p = byPid.get(pid); return p ? { pid, ppid: p.ppid, start: "1" } : null; },
    uid: (pid) => byPid.get(pid)?.uid ?? ME,
    cmdline: (pid) => byPid.get(pid)?.argv ?? [],
    environ: (pid) => {
      const p = byPid.get(pid);
      if (p && (p.uid ?? ME) !== ME) throw new Error(`read another user's environ (pid ${pid})`);
      return p?.env ?? {};
    },
  };
};
const MCP = ["node", "<path-to-gptqueue>/dist/mcp-server/index.js"];
// hook 300 -> sh 200 -> claude 100 -> zsh 50 -> tmux 10
const tree: FakeProc[] = [
  { pid: 10, ppid: 1 }, { pid: 50, ppid: 10 }, { pid: 100, ppid: 50, argv: ["claude"] }, { pid: 200, ppid: 100 }, { pid: 300, ppid: 200 },
];
const chainOf = (reader: ProcReader, pid: number): ProcessInfo[] => {
  const chain: ProcessInfo[] = [];
  for (let p = reader.stat(pid); p && p.pid > 1; p = reader.stat(p.ppid)) chain.push(p);
  return chain;
};
const lookup = (procs: FakeProc[]) => { const reader = fakeReader(procs); return findLegacyIdentity(chainOf(reader, 300), reader, ME); };
const recordNone = { kind: "none" } as const;

describe("legacy identity from the old MCP server's environment", () => {
  it("one legacy MCP child of this Claude: confirmed (legacy_env) and the Stop hook gates on it", () => {
    const found = lookup([...tree, { pid: 110, ppid: 100, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-a" } }]);
    expect(found).toEqual({ kind: "confirmed", agent: "claude-a", server_pid: 110, claude_pid: 100 });
    const identity = resolveIdentity("/x", { GPTQ_AGENT_NAME: "guess" }, recordNone, () => found);
    expect(identity).toMatchObject({ agent: "claude-a", source: "legacy_env" });
    const decision = decideHook({ event: "stop", identity, stopHookActive: false, prior: null, watcherRunning: true, watchPolicy: "require", watchCommand: "w",
      probe: { ok: true, counts: { unclaimed: 2, claimed_by_session: null, claimed_by_others: 0, expired_by_others: 0, latest_event_id: "1-0" } } });
    expect(decision.kind).toBe("block");
    expect(decision.kind === "block" && decision.reason).toMatch(/gptqueue inbox for agent "claude-a" \(confirmed from the environment of this session's pre-record gptqueue MCP server \(pid 110\)\): 2 unclaimed/);
  });

  it("two candidates under this Claude: ambiguous, so the identity stays UNCONFIRMED and the hook does not gate", () => {
    const found = lookup([...tree,
      { pid: 110, ppid: 100, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-a" } },
      { pid: 111, ppid: 100, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-b" } }]);
    expect(found).toMatchObject({ kind: "ambiguous", agents: ["claude-a", "claude-b"] });
    const identity = resolveIdentity("/x", { GPTQ_AGENT_NAME: "guess" }, recordNone, () => found);
    expect(identity.source).toBe("env");
    expect(identity.note).toMatch(/2 legacy gptqueue MCP servers/);
    const decision = decideHook({ event: "stop", identity, stopHookActive: false, prior: null, watcherRunning: true, watchPolicy: "require", watchCommand: "w",
      probe: { ok: true, counts: { unclaimed: 2, claimed_by_session: null, claimed_by_others: 0, expired_by_others: 0, latest_event_id: "1-0" } } });
    expect(decision.kind).toBe("allow");
  });

  it("ignores another Claude's MCP server (not a child of our ancestor), non-MCP processes, unnamed servers and other users' processes", () => {
    expect(lookup([...tree,
      { pid: 120, ppid: 50, argv: ["claude"] }, // a sibling Claude under the same shell
      { pid: 121, ppid: 120, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-other" } },
      { pid: 130, ppid: 100, argv: ["node", "other.js"], env: { GPTQ_AGENT_NAME: "x" } },
      { pid: 131, ppid: 100, argv: MCP, env: {} },
      { pid: 132, ppid: 100, uid: 4242, argv: MCP, env: { GPTQ_AGENT_NAME: "root-x" } },
    ])).toEqual({ kind: "none" });
  });

  it("rejects a candidate whose pid was reused between reads (start time changes between the first and second stat)", () => {
    const procs = [...tree, { pid: 110, ppid: 100, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-new-process" } }];
    const base = fakeReader(procs);
    let statReads = 0;
    const reused: ProcReader = { ...base, stat: (pid) => {
      const info = base.stat(pid);
      if (pid !== 110 || !info) return info;
      statReads += 1;
      return { ...info, start: statReads === 1 ? "1" : "2" }; // pid 110 exited and was reused after the first stat
    } };
    expect(findLegacyIdentity(chainOf(base, 300), reused, ME)).toEqual({ kind: "none" });
    expect(statReads).toBe(2);
    // Same pid, unchanged start time: still accepted.
    expect(findLegacyIdentity(chainOf(base, 300), base, ME)).toMatchObject({ kind: "confirmed", agent: "claude-new-process" });
  });

  it("the nearest ancestor decides: our Claude's server wins over one under a farther ancestor", () => {
    expect(lookup([...tree,
      { pid: 110, ppid: 100, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-mine" } },
      { pid: 60, ppid: 10, argv: MCP, env: { GPTQ_AGENT_NAME: "claude-far" } }])).toMatchObject({ kind: "confirmed", agent: "claude-mine" });
  });

  it("a session record takes precedence over the legacy environment", () => {
    const record = { kind: "found", path: "p", record: { version: 1, agent: "claude-rec", gptqueue_session_id: "s-1", claude_pid: 100, claude_start: "1", server_pid: 1, server_start: "1", written_at: "t" } } as const;
    let scanned = false;
    const identity = resolveIdentity("/x", {}, record, () => { scanned = true; return { kind: "confirmed", agent: "claude-a", server_pid: 110, claude_pid: 100 }; });
    expect(identity).toMatchObject({ agent: "claude-rec", source: "session_record" });
    expect(scanned).toBe(false);
  });
});

describe("legacy identity through the real /proc", () => {
  const children: ChildProcess[] = [];
  afterEach(() => { for (const c of children.splice(0)) c.kill("SIGKILL"); });

  it("finds a fake legacy MCP server (node child with GPTQ_AGENT_NAME) under a fake Claude parent", async () => {
    // Fake Claude: a node process that spawns the fake MCP server as its direct child and reports its pid.
    const parentScript = `
      const { spawn } = require("node:child_process");
      const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)", "/fake/gptqueue/dist/mcp-server/index.js"],
        { env: { ...process.env, GPTQ_AGENT_NAME: "claude-real-legacy" }, stdio: "ignore" });
      process.stdout.write(String(child.pid) + "\\n");
      setInterval(() => {}, 1000);`;
    const parent = spawn(process.execPath, ["-e", parentScript], { stdio: ["ignore", "pipe", "inherit"] });
    children.push(parent);
    const childPid = await new Promise<number>((done) => parent.stdout!.once("data", (d) => done(Number(String(d).trim()))));
    // Wait until the child has exec'd node (its cmdline names the MCP path).
    for (let i = 0; i < 400; i += 1) {
      const proc = readProcess(childPid);
      if (proc && proc.ppid === parent.pid) break;
      await new Promise((r) => setTimeout(r, 25));
    }
    // Resolve as a hook would if it were a descendant of the fake Claude: chain = [hook (us), fake Claude].
    const chain = [readProcess(process.pid)!, readProcess(parent.pid!)!];
    expect(findLegacyIdentity(chain)).toEqual({ kind: "confirmed", agent: "claude-real-legacy", server_pid: childPid, claude_pid: parent.pid });
    process.kill(childPid, "SIGKILL");
  }, 60_000);
});
