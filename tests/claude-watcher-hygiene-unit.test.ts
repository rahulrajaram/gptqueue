/**
 * Watcher hygiene, pure decisions: a watcher started with `&` or disown
 * (its launcher exited, so its exit wakes nobody) is orphaned, a second live
 * watcher for the same session is a duplicate, and the Stop hook never counts
 * an orphaned watcher as running. All against a fake process table.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { decideHook, type HookInput } from "../src/claude/hook.js";
import {
  adoptedAtStart, duplicateWatcher, launcherIsParent, parentOf, readWatcherPid, watcherLiveness, watcherPidPath, watcherRunning, writeWatcherPid,
  type ProcessInfo, type ProcessReader, type WatcherPid,
} from "../src/claude/session-record.js";

const dirs: string[] = [];
afterEach(async () => { await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true }))); });
const temp = async () => { const d = await mkdtemp(join(tmpdir(), "gptq-watch-hygiene-unit-")); dirs.push(d); return d; };

/** A fake /proc: each entry is one live process. */
const table = (...procs: ProcessInfo[]): ProcessReader => (pid) => procs.find((p) => p.pid === pid) ?? null;
const proc = (pid: number, ppid: number, start: string): ProcessInfo => ({ pid, ppid, start });

const CLAUDE = { pid: 100, start: "1000" } as const;
const SHELL = proc(200, CLAUDE.pid, "2000"); // the zsh -c that run_in_background starts
const INIT = proc(1, 0, "1");
const record = (over: Partial<WatcherPid> = {}): WatcherPid => ({
  pid: 300, start: "3000", agent: "claude-a", claude_pid: CLAUDE.pid, claude_start: CLAUDE.start,
  launcher_pid: SHELL.pid, launcher_start: SHELL.start, ...over,
});
const claudeProc = proc(CLAUDE.pid, 1, CLAUDE.start);
/** The watcher still under its launching shell (run_in_background). */
const attached = table(INIT, claudeProc, SHELL, proc(300, SHELL.pid, "3000"));
/** The launching shell exited and init adopted the watcher (`cmd &` in a foreground call). */
const orphaned = table(INIT, claudeProc, proc(300, 1, "3000"));

describe("launcher decisions", () => {
  it("parentOf names the current parent by pid and start time, null when unreadable", () => {
    expect(parentOf(300, attached)).toEqual({ pid: SHELL.pid, start: SHELL.start });
    expect(parentOf(300, orphaned)).toEqual({ pid: 1, start: "1" });
    expect(parentOf(999, attached)).toBeNull();
    expect(parentOf(300, table(proc(300, 555, "3000")))).toBeNull(); // parent unreadable
  });

  it("launcherIsParent holds only while the recorded launcher (same pid and start) is still the parent", () => {
    const launcher = { pid: SHELL.pid, start: SHELL.start };
    expect(launcherIsParent(300, launcher, attached)).toBe(true);
    expect(launcherIsParent(300, launcher, orphaned)).toBe(false);
    // The launcher's pid was recycled by an unrelated process that is now the parent.
    expect(launcherIsParent(300, launcher, table(proc(SHELL.pid, 1, "9999"), proc(300, SHELL.pid, "3000")))).toBe(false);
    expect(launcherIsParent(300, launcher, table(SHELL))).toBe(false); // watcher itself gone
  });

  it("adoptedAtStart: a watcher whose parent is already init when it starts was launched with & from a shell that has exited", () => {
    expect(adoptedAtStart({ pid: 1, start: "1" })).toBe(true);
    expect(adoptedAtStart({ pid: SHELL.pid, start: SHELL.start })).toBe(false);
    expect(adoptedAtStart(null)).toBe(false); // unknown parent: not enforced
  });
});

describe("watcherLiveness", () => {
  it("dead when the pid is gone or has another start time", () => {
    expect(watcherLiveness(record(), table(INIT, SHELL))).toBe("dead");
    expect(watcherLiveness(record(), table(INIT, SHELL, proc(300, SHELL.pid, "4000")))).toBe("dead"); // recycled pid
  });

  it("orphaned when alive but its parent is no longer the recorded launcher", () => {
    expect(watcherLiveness(record(), orphaned)).toBe("orphaned");
    expect(watcherLiveness(record(), table(INIT, proc(SHELL.pid, 1, "9999"), proc(300, SHELL.pid, "3000")))).toBe("orphaned"); // launcher pid recycled
  });

  it("live when alive and still under its launcher", () => {
    expect(watcherLiveness(record(), attached)).toBe("live");
  });

  it("a pidfile written before launcher tracking is live whenever its process is", () => {
    const legacy = record({ launcher_pid: undefined, launcher_start: undefined });
    expect(watcherLiveness(legacy, orphaned)).toBe("live");
    expect(watcherLiveness(record({ launcher_pid: null, launcher_start: null }), orphaned)).toBe("live");
    expect(watcherLiveness(legacy, table(INIT))).toBe("dead");
  });
});

describe("duplicateWatcher (exit 8)", () => {
  const self = { pid: 301, agent: "claude-a", claude: CLAUDE };

  it("defers to a live watcher for the same agent and Claude process that is still under its launcher", () => {
    expect(duplicateWatcher(record(), self, attached)).toEqual(record());
  });

  it("a dead or orphaned recorded watcher does not count", () => {
    expect(duplicateWatcher(record(), self, table(INIT, SHELL))).toBeNull();
    expect(duplicateWatcher(record(), self, orphaned)).toBeNull();
  });

  it("another agent, another Claude process, an unknown Claude, itself, or no pidfile does not count", () => {
    expect(duplicateWatcher(record({ agent: "claude-b" }), self, attached)).toBeNull();
    expect(duplicateWatcher(record({ claude_start: "1001" }), self, attached)).toBeNull();
    expect(duplicateWatcher(record(), { ...self, claude: null }, attached)).toBeNull();
    expect(duplicateWatcher(record(), { ...self, pid: 300 }, attached)).toBeNull();
    expect(duplicateWatcher(null, self, attached)).toBeNull();
  });

  it("a live pre-launcher pidfile counts, exactly as the Stop hook counts it", () => {
    expect(duplicateWatcher(record({ launcher_pid: null, launcher_start: null }), self, orphaned)).not.toBeNull();
  });
});

describe("Stop hook: an orphaned watcher is not running", () => {
  const recorded = { agent: "claude-a", gptqueue_session_id: "s-1", source: "session_record", note: null, claude: CLAUDE } as const;
  const stopInput = (running: boolean): HookInput => ({
    event: "stop", identity: recorded, probe: { ok: true, counts: { unclaimed: 0, claimed_by_session: 0, claimed_by_others: 0, expired_by_others: 0, latest_event_id: "1-0" } },
    stopHookActive: false, prior: null, watcherRunning: running, watchPolicy: "require", watchCommand: "peek --watch --session s",
  });

  it("watcherRunning counts the attached watcher, not the orphaned one, and keeps the Claude-process rules", async () => {
    const path = watcherPidPath(await temp(), "s");
    writeWatcherPid(path, record());
    expect(readWatcherPid(path)).toMatchObject({ launcher_pid: SHELL.pid, launcher_start: SHELL.start });
    expect(watcherRunning(path, "claude-a", CLAUDE, attached)).toBe(true);
    expect(watcherRunning(path, "claude-a", CLAUDE, orphaned)).toBe(false);
    expect(watcherRunning(path, "claude-a", { pid: CLAUDE.pid, start: "1001" }, attached)).toBe(false);
    expect(watcherRunning(path, "claude-a", null, attached)).toBe(false);
  });

  it("the Stop-hook decision reminds to start a watcher when the only watcher is orphaned", async () => {
    const path = watcherPidPath(await temp(), "s");
    writeWatcherPid(path, record());
    expect(decideHook(stopInput(watcherRunning(path, "claude-a", CLAUDE, attached))).kind).toBe("allow");
    const decision = decideHook(stopInput(watcherRunning(path, "claude-a", CLAUDE, orphaned)));
    expect(decision.kind === "block" && decision.reason).toMatch(/no background inbox watcher.*run_in_background.*peek --watch --session s/s);
  });

  it("reads a pidfile without launcher fields as null launcher (compatibility)", async () => {
    const path = watcherPidPath(await temp(), "old");
    writeWatcherPid(path, { pid: 300, start: "3000", agent: "claude-a", claude_pid: CLAUDE.pid, claude_start: CLAUDE.start });
    expect(readWatcherPid(path)).toMatchObject({ launcher_pid: null, launcher_start: null });
    expect(watcherRunning(path, "claude-a", CLAUDE, orphaned)).toBe(true);
  });
});
