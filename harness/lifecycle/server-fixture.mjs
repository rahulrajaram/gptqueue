/**
 * Isolated GPTQueue server fixture for the sandboxed lifecycle runners.
 *
 * Spawns an in-sandbox `/usr/bin/redis-server` on a Unix domain socket
 * (`--port 0`, no persistence — nothing leaves the sandbox tmpfs) and the
 * compiled `dist/transports/http.js` listening on its own Unix domain socket
 * via the opt-in `GPTQUEUE_HTTP_SOCKET` mode. No TCP, no host network, no
 * host state: every path lives under a fresh `/tmp` directory.
 */

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { rawGetHealth } from "./lifecycle-client.mjs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Poll an observable condition with an explicit deadline (no fixed sleeps). */
export async function pollUntil(condition, { timeoutMs, intervalMs = 100, label }) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      if (await condition()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  throw new Error(
    `deadline exceeded waiting for ${label} (${timeoutMs}ms)` +
      (lastError ? `; last error: ${String(lastError)}` : "")
  );
}

export class ServerFixture {
  constructor({ queueBound } = {}) {
    this.dir = mkdtempSync(join(tmpdir(), "gptqueue-lifecycle-"));
    this.redisSocketPath = join(this.dir, "redis.sock");
    this.httpSocketPath = join(this.dir, "http.sock");
    this.queueBound = queueBound;
    this.redis = null;
    this.server = null;
    this.started = false;
  }

  /*
   * Why `--unhandled-rejections=warn` on the server child: inside the
   * MetaBuilder sandboxed command adapter the address-space limit is 4 GiB,
   * and node 18's bundled undici eagerly instantiates its llhttp WASM (whose
   * trap-handler memory reservation does not fit), producing a rejected
   * promise at module load. The HTTP server never calls fetch (its clients
   * use node:http over a Unix socket), and Request/Response construction
   * needs no WASM, so the rejection is an inert environmental artifact;
   * downgrading it to a warning keeps the server alive. Verified by the full
   * MCP wire flow under the exact sandbox limit.
   */

  async start() {
    this.redis = spawn(
      "/usr/bin/redis-server",
      [
        "--unixsocket", this.redisSocketPath,
        "--unixsocketperm", "700",
        "--port", "0",
        "--save", "",
        "--appendonly", "no",
        "--dir", this.dir,
      ],
      { stdio: ["ignore", "ignore", "pipe"] }
    );
    this.redis.stderr.on("data", () => {});
    await pollUntil(
      async () => {
        const { existsSync } = await import("node:fs");
        return existsSync(this.redisSocketPath);
      },
      { timeoutMs: 10000, label: "redis unix socket" }
    );

    await this.spawnServer();
    this.started = true;
  }

  async spawnServer() {
    const { existsSync, unlinkSync } = await import("node:fs");
    if (existsSync(this.httpSocketPath)) unlinkSync(this.httpSocketPath);
    const env = {
      ...process.env,
      REDIS_URL: this.redisSocketPath,
      GPTQUEUE_HTTP_SOCKET: this.httpSocketPath,
    };
    if (this.queueBound !== undefined) {
      env.GPTQ_QUEUE_BOUND = String(this.queueBound);
    }
    this.server = spawn(
      process.execPath,
      [
        "--unhandled-rejections=warn",
        join(process.cwd(), "dist", "transports", "http.js"),
      ],
      { cwd: process.cwd(), env, stdio: ["ignore", "ignore", "pipe"] }
    );
    let stderr = "";
    this.server.stderr.on("data", (d) => (stderr += d));
    this.serverExit = null;
    this.server.on("exit", (code) => (this.serverExit = code));
    await pollUntil(
      async () => {
        if (this.serverExit !== null) {
          throw new Error(`server exited early (code ${this.serverExit}): ${stderr.slice(0, 400)}`);
        }
        const health = await rawGetHealth(this.httpSocketPath);
        return health.status === 200 && health.body.status === "ok";
      },
      { timeoutMs: 20000, label: "http server health over UDS" }
    );
  }

  async health() {
    const res = await rawGetHealth(this.httpSocketPath);
    if (res.status !== 200) throw new Error(`/health returned ${res.status}`);
    return res.body;
  }

  /** Hard kill (SIGKILL) the HTTP server; Redis and its data stay up. */
  async killServer() {
    if (!this.server) return;
    this.server.kill("SIGKILL");
    await pollUntil(
      () => this.serverExit !== null || this.server.exitCode !== null || this.server.killed,
      { timeoutMs: 10000, label: "server SIGKILL observed" }
    );
    this.server = null;
  }

  async stop() {
    if (this.server) {
      this.server.kill("SIGKILL");
      this.server = null;
    }
    if (this.redis) {
      this.redis.kill("SIGKILL");
      this.redis = null;
    }
    try {
      rmSync(this.dir, { recursive: true, force: true });
    } catch {
      /* tmp cleanup is best-effort */
    }
    this.started = false;
  }
}

/** Run one round with a fresh isolated server; always tears the fixture down. */
export async function withFixture(roundLabel, fn, { queueBound } = {}) {
  const fixture = new ServerFixture({ queueBound });
  try {
    await fixture.start();
    return await fn(fixture);
  } catch (error) {
    throw new Error(`[${roundLabel}] ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    await fixture.stop();
  }
}
