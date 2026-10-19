// LC-F4: no redis-server started by the owned-Redis scripts may outlive its
// owner. A stub GPTQ_TEST_REDIS_SERVER records its pid and then execs the real
// redis-server (same pid, so the privacy check still passes), so the test can
// check whether the server is still alive after the owner is gone.
import { afterAll, describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const REPO = process.cwd();
const scratch = mkdtempSync(join(tmpdir(), "gptq-owned-redis-"));
const stub = join(scratch, "redis-server-stub");
const realServer = process.env.GPTQ_TEST_REDIS_SERVER || "/usr/bin/redis-server";
writeFileSync(stub, `#!/bin/sh\necho $$ > "$STUB_PIDFILE"\nexec ${JSON.stringify(realServer)} "$@"\n`);
chmodSync(stub, 0o755);
// A git that hangs briefly keeps install-runtime inside its install step.
const slowBin = join(scratch, "bin");
const slowGit = join(slowBin, "git");

const started: number[] = [];
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
const waitFor = async (check: () => boolean, ms: number) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (check()) return true; await new Promise((r) => setTimeout(r, 50)); }
  return check();
};

/** Run node with args under the stub; resolves with exit code and the stub's pid. */
const runOwner = async (args: string[], { env = {}, afterReady }: { env?: Record<string, string>; afterReady?: (pid: number) => void } = {}) => {
  const pidfile = join(scratch, `pid-${started.length}-${Date.now()}`);
  const child = spawn(process.execPath, args, {
    cwd: REPO, stdio: ["ignore", "ignore", "pipe"],
    env: { ...process.env, GPTQ_TEST_REDIS_SERVER: stub, STUB_PIDFILE: pidfile, ...env },
  });
  let stderr = "";
  child.stderr.on("data", (c) => { stderr += c; });
  const exited = new Promise<number | null>((r) => child.once("exit", (code, signal) => r(code ?? (signal ? -1 : null))));
  expect(await waitFor(() => existsSync(pidfile) && readFileSync(pidfile, "utf8").trim() !== "", 10_000)).toBe(true);
  const pid = Number(readFileSync(pidfile, "utf8").trim());
  started.push(pid);
  afterReady?.(child.pid!);
  return { code: await exited, pid, stderr: () => stderr };
};

afterAll(() => {
  for (const pid of started) { try { process.kill(pid, "SIGKILL"); } catch { /* gone */ } }
  rmSync(scratch, { recursive: true, force: true });
});

describe("LC-F4 owned Redis lifetime", () => {
  it("with-owned-redis: a command that cannot be spawned exits non-zero and leaves no redis-server", async () => {
    const run = await runOwner(["scripts/with-owned-redis.mjs", "--", join(scratch, "no-such-command")]);
    expect(run.code).toBe(127);
    expect(run.stderr()).toContain("cannot run");
    expect(await waitFor(() => !alive(run.pid), 5_000)).toBe(true);
  }, 20_000);

  it("startOwnedRedis: an owner that exits without calling stop() still stops Redis", async () => {
    const script = `import { startOwnedRedis } from ${JSON.stringify(join(REPO, "scripts/lib/owned-redis.mjs"))};
await startOwnedRedis(); process.exit(0);`;
    const run = await runOwner(["--input-type=module", "-e", script]);
    expect(run.code).toBe(0);
    expect(await waitFor(() => !alive(run.pid), 5_000)).toBe(true);
  }, 20_000);

  it("install-runtime: SIGTERM to the installer's pid alone stops its Redis", async () => {
    mkdirSync(slowBin, { recursive: true });
    writeFileSync(slowGit, "#!/bin/sh\nsleep 2\nexit 1\n");
    chmodSync(slowGit, 0o755);
    const run = await runOwner(
      ["scripts/install-runtime.mjs", "install", "--prefix", join(scratch, "prefix"), "--source", scratch],
      { env: { PATH: `${slowBin}:${process.env.PATH}` }, afterReady: (pid) => setTimeout(() => process.kill(pid, "SIGTERM"), 200) },
    );
    expect(run.code).not.toBe(0);
    expect(await waitFor(() => !alive(run.pid), 10_000)).toBe(true);
  }, 30_000);
});
