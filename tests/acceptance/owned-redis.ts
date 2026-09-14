import { createServer } from "node:net";
import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { Redis } from "ioredis";

export type OwnedRedis = Readonly<{ url: string; close: () => Promise<void> }>;

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", () => resolve()); });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("owned Redis did not expose an ephemeral port");
  const port = address.port;
  await new Promise<void>(resolve => server.close(() => resolve()));
  return port;
};

const redisBinary = (): string => process.env.REDIS_SERVER_BIN ?? execFileSync("sh", ["-c", "command -v redis-server"], { encoding: "utf8" }).trim();
const processIdFromInfo = (info: string): number | undefined => {
  const value = /^process_id:(\d+)$/mu.exec(info)?.[1];
  return value === undefined ? undefined : Number(value);
};

const killOwned = async (child: ChildProcess): Promise<void> => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { if (child.pid) process.kill(-child.pid, "SIGTERM"); } catch { child.kill("SIGTERM"); }
  const deadline = Date.now() + 5_000;
  while (child.exitCode === null && child.signalCode === null && Date.now() < deadline) await sleep(50);
  if (child.exitCode === null && child.signalCode === null) {
    try { if (child.pid) process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
  }
  if (child.exitCode === null && child.signalCode === null) {
    await new Promise<void>(resolve => {
      const timer = setTimeout(resolve, 1_000);
      child.once("close", () => { clearTimeout(timer); resolve(); });
    });
  }
};

export const startOwnedRedis = async (): Promise<OwnedRedis> => {
  const root = await mkdtemp(join(tmpdir(), "gptq-owned-redis-"));
  await mkdir(root, { recursive: true });
  const port = await freePort();
  const child = spawn(redisBinary(), ["--bind", "127.0.0.1", "--port", String(port), "--save", "", "--appendonly", "no", "--dir", root, "--dbfilename", "gptqueue-test.rdb"], { detached: true, stdio: ["ignore", "ignore", "pipe"] });
  let startupDiagnostic = "";
  child.stderr?.on("data", chunk => { startupDiagnostic = (startupDiagnostic + String(chunk)).slice(-4_000); });
  let spawnError: Error | undefined;
  child.once("error", error => { spawnError = error; });
  const url = `redis://127.0.0.1:${port}/15`;
  let probe: Redis | undefined;
  try {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      if (spawnError) throw new Error(`owned redis spawn failed: ${spawnError.message}; ${startupDiagnostic}`);
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`owned redis exited before readiness (${child.exitCode ?? child.signalCode}); ${startupDiagnostic}`);
      try {
        probe = new Redis(url, { connectTimeout: 500, maxRetriesPerRequest: 1, retryStrategy: () => null });
        probe.on("error", () => undefined);
        await probe.ping();
        const processId = processIdFromInfo(await probe.info("server"));
        if (processId !== child.pid) throw new Error(`owned Redis identity mismatch: expected pid ${child.pid}, got ${processId ?? "missing"}`);
        break;
      } catch { probe?.disconnect(); probe = undefined; await sleep(100); }
    }
    if (!probe || probe.status !== "ready") throw new Error(`owned Redis did not become ready before deadline; ${startupDiagnostic}`);
    await probe.quit(); probe = undefined;
    return { url, close: async () => { await killOwned(child); await rm(root, { recursive: true, force: true }); } };
  } catch (error) {
    probe?.disconnect(); await killOwned(child); await rm(root, { recursive: true, force: true }); throw error;
  }
};
