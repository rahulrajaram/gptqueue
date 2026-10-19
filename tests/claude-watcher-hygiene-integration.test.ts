/**
 * Watcher hygiene against the built dist and the private test Redis: a
 * watcher started with `&` from a short-lived shell exits 7 and removes its
 * pidfile, the Stop hook stops counting it as soon as its launcher is gone,
 * and a second watcher for a session with a live one exits 8.
 *
 * A watcher whose launcher exits is adopted by init, so this process cannot
 * collect its exit status. The orphan cases run it through a small harness
 * that calls runPeek (as bin/gptqueue-inbox-peek does) and writes the code
 * it returns to a file.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { publishSessionRecord, readProcess, watcherPidPath } from "../src/claude/session-record.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const ROOT = resolve(__dirname, "..");
const BIN = join(ROOT, "bin");
let home = "";
let runtime = "";
let harness = "";
let env: NodeJS.ProcessEnv = {};
const clients: RedisClient[] = [];
const children: ChildProcess[] = [];
/** Processes this test started that are not its children (adopted by init): killed only while pid and start still match. */
const adopted: { pid: number; start: string }[] = [];
const me = () => readProcess(process.pid)!;

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-watch-hygiene-"));
  runtime = join(home, "rt");
  await mkdir(runtime, { recursive: true, mode: 0o700 });
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: runtime, GPTQ_CLAUDE_LEGACY_SCAN: "off" };
  harness = join(home, "peek-exit-code.mjs");
  writeFileSync(harness, [
    `import { writeFileSync } from "node:fs";`,
    `import { runPeek } from ${JSON.stringify(pathToFileURL(join(ROOT, "dist", "claude", "peek.js")).href)};`,
    `const [codeFile, ...argv] = process.argv.slice(2);`,
    `const code = await runPeek(argv);`,
    `writeFileSync(codeFile, String(code));`,
    `process.exitCode = code;`,
  ].join("\n"));
});
afterEach(async () => {
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const p of adopted.splice(0)) if (readProcess(p.pid)?.start === p.start) process.kill(p.pid, "SIGKILL");
  for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
  publishSessionRecord(me(), null, null, env);
});
afterAll(async () => { await rm(home, { recursive: true, force: true }); });

const waitFor = async (cond: () => boolean, ms: number) => { const end = Date.now() + ms; while (!cond() && Date.now() < end) await new Promise((r) => setTimeout(r, 10)); return cond(); };
const exited = (child: ChildProcess) => new Promise<number | null>((done) => {
  if (child.exitCode !== null || child.signalCode !== null) done(child.exitCode); else child.on("exit", (code) => done(code));
});
const run = (path: string, args: string[], input: string, extra: NodeJS.ProcessEnv = {}) =>
  new Promise<{ stdout: string; code: number | null }>((done) => {
    const child = spawn(path, args, { env: { ...env, ...extra }, stdio: ["pipe", "pipe", "pipe"] });
    children.push(child);
    let stdout = "";
    child.stdout.on("data", (d) => { stdout += d; });
    child.on("close", (code) => done({ stdout: stdout.trim(), code }));
    child.stdin.end(input);
  });
const stopHook = (sid: string) => run(join(BIN, "gptqueue-claude-hook"), ["stop"], JSON.stringify({ cwd: home, session_id: sid }));

/** A registered agent and this process's session record naming it, as this session's MCP server would publish. */
const session = async () => {
  const client = new RedisClient(null, REDIS_URL);
  clients.push(client);
  const reg = await client.register("both", `claude-hygiene-${randomUUID().slice(0, 8)}`);
  publishSessionRecord(me(), reg.name, reg.session_id, env);
  return reg.name;
};

/**
 * Start the watcher with `&` from `sh -c`. With `hold`, the shell stays until
 * the test creates the go file (so the watcher records it as its launcher and
 * arms first); without, the shell exits at once.
 */
const launchWithAmpersand = (sid: string, blockMs: number, hold: boolean) => {
  const files = { code: join(home, `code-${sid}`), out: join(home, `out-${sid}`), pid: join(home, `pid-${sid}`), go: join(home, `go-${sid}`) };
  const wait = hold ? `i=0; while [ ! -e "$6" ] && [ $i -lt 3000 ]; do sleep 0.02; i=$((i+1)); done` : "";
  const shell = spawn("sh", ["-c", `node "$0" "$1" --watch --session "$2" --block-ms "$3" > "$4" 2>&1 & echo $! > "$5"; ${wait}`,
    harness, files.code, sid, String(blockMs), files.out, files.pid, files.go], { env, stdio: "ignore" });
  children.push(shell);
  return { shell, files };
};
/** Record the watcher (the shell's background job) so cleanup can kill it while it is still the same process. */
const trackWatcher = async (pidFile: string) => {
  expect(await waitFor(() => existsSync(pidFile) && readFileSync(pidFile, "utf8").trim() !== "", 30_000)).toBe(true);
  const pid = Number(readFileSync(pidFile, "utf8").trim());
  const info = readProcess(pid);
  if (info) adopted.push({ pid, start: info.start });
  return pid;
};

describe("watcher hygiene: & / disown and duplicates", { timeout: 180_000 }, () => {
  it("a watcher started with & exits 7 within about one block period once its launcher exits, and removes its pidfile", async () => {
    const name = await session();
    const sid = randomUUID();
    const pidfile = watcherPidPath(runtime, sid);
    const { shell, files } = launchWithAmpersand(sid, 200, true);
    const watcherPid = await trackWatcher(files.pid);
    expect(await waitFor(() => existsSync(pidfile), 60_000)).toBe(true);
    const shellInfo = readProcess(shell.pid!)!;
    const armed = JSON.parse(readFileSync(pidfile, "utf8")) as Record<string, unknown>;
    expect(armed).toMatchObject({ pid: watcherPid, agent: name });

    writeFileSync(files.go, "");
    await exited(shell);
    const launcherGone = performance.now();
    expect(await waitFor(() => existsSync(files.code), 15_000)).toBe(true);
    const latency = performance.now() - launcherGone;
    expect(readFileSync(files.code, "utf8")).toBe("7");
    expect(JSON.parse(readFileSync(files.out, "utf8"))).toEqual({
      agent: name, error: "watcher_orphaned", message: "launched outside the session's background-task mechanism (& or disown); re-arm with run_in_background",
    });
    expect(existsSync(pidfile)).toBe(false);
    expect(latency).toBeLessThan(1_500); // block period 200 ms, plus process start-up noise under load
    expect(armed).toMatchObject({ launcher_pid: shell.pid, launcher_start: shellInfo.start }); // its launcher was the shell
  });

  it("the Stop hook stops counting a watcher as soon as its launcher has exited, before the watcher notices", async () => {
    await session();
    const sid = randomUUID();
    const pidfile = watcherPidPath(runtime, sid);
    const { shell, files } = launchWithAmpersand(sid, 120_000, true); // it will not notice for 120 s
    await trackWatcher(files.pid);
    expect(await waitFor(() => existsSync(pidfile), 60_000)).toBe(true);
    expect((await stopHook(sid)).stdout).toBe(""); // still under its launcher: counted, so the stop is allowed

    writeFileSync(files.go, "");
    await exited(shell);
    const after = await stopHook(sid);
    expect(JSON.parse(after.stdout)).toMatchObject({ decision: "block" });
    expect(JSON.parse(after.stdout).reason).toMatch(/no background inbox watcher[\s\S]*run_in_background/);
    expect(existsSync(pidfile)).toBe(true); // the orphan has not noticed yet: only the hook's rule changed
    expect(existsSync(files.code)).toBe(false);
  });

  it("a watcher already adopted by init when it starts exits 7 at once, before the 40 s record wait, and never writes a pidfile", async (ctx) => {
    // Where a child subreaper adopts orphans instead of init, this startup check cannot apply.
    const probe = spawn("sh", ["-c", "sleep 5 & echo $!"], { stdio: ["ignore", "pipe", "ignore"] });
    children.push(probe);
    let out = "";
    probe.stdout!.on("data", (d) => { out += d; });
    await exited(probe);
    const sleepPid = Number(out.trim());
    const sleepInfo = readProcess(sleepPid);
    if (sleepInfo) adopted.push({ pid: sleepPid, start: sleepInfo.start });
    ctx.skip(sleepInfo?.ppid !== 1, `orphans here are adopted by pid ${sleepInfo?.ppid}, not init`);

    await session();
    const sid = randomUUID();
    const started = performance.now();
    const { shell, files } = launchWithAmpersand(sid, 200, false);
    await exited(shell);
    await trackWatcher(files.pid);
    expect(await waitFor(() => existsSync(files.code), 15_000)).toBe(true);
    expect(performance.now() - started).toBeLessThan(10_000);
    expect(readFileSync(files.code, "utf8")).toBe("7");
    expect(JSON.parse(readFileSync(files.out, "utf8"))).toMatchObject({ agent: null, error: "watcher_orphaned" });
    expect(existsSync(watcherPidPath(runtime, sid))).toBe(false);
  });

  it("a second watcher for a session with a live one exits 8 and leaves the first and its pidfile untouched", async () => {
    const name = await session();
    const sid = randomUUID();
    const pidfile = watcherPidPath(runtime, sid);
    const first = spawn(join(BIN, "gptqueue-inbox-peek"), ["--watch", "--session", sid, "--block-ms", "200"], { env, stdio: ["ignore", "pipe", "pipe"] });
    children.push(first);
    expect(await waitFor(() => existsSync(pidfile), 60_000)).toBe(true);
    const before = readFileSync(pidfile, "utf8");
    expect(JSON.parse(before)).toMatchObject({ pid: first.pid });

    // --timeout bounds the second watcher should it ever start watching.
    const second = await run(join(BIN, "gptqueue-inbox-peek"), ["--watch", "--session", sid, "--block-ms", "200", "--timeout", "20"], "");
    expect(second.code).toBe(8);
    expect(JSON.parse(second.stdout)).toEqual({ agent: name, error: "watcher_already_running", pid: first.pid });
    expect(readFileSync(pidfile, "utf8")).toBe(before);
    expect(first.exitCode).toBeNull();
    expect(readProcess(first.pid!)).not.toBeNull();
    expect(JSON.parse(before)).toMatchObject({ launcher_pid: process.pid }); // spawned directly: this process launched it

    // Once the recorded watcher is dead, a new one proceeds and replaces the pidfile.
    first.kill("SIGKILL");
    await exited(first);
    const third = spawn(join(BIN, "gptqueue-inbox-peek"), ["--watch", "--session", sid, "--block-ms", "200"], { env, stdio: ["ignore", "pipe", "pipe"] });
    children.push(third);
    expect(await waitFor(() => existsSync(pidfile) && JSON.parse(readFileSync(pidfile, "utf8")).pid === third.pid, 60_000)).toBe(true);
    expect(third.exitCode).toBeNull();
  });
});
