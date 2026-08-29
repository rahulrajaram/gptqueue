/**
 * Shared integration-test server harness.
 *
 * Builds the project once (module-level guard), spawns the real HTTP MCP server
 * (`dist/transports/http.js`) on port 8199 against an ISOLATED Redis database
 * (db15), and returns handles for talking to it over the wire plus a direct
 * ioredis client on the same db for raw assertions.
 *
 * Isolation contract: every key we read/write lives under `gptq:*` on db15.
 * We NEVER use db0 (that is the live server's database) and we never call
 * FLUSHDB — only key deletions scoped to `gptq:*` via SCAN.
 *
 * The server child and the direct redis handle are shared across suite files
 * through a process-global singleton with a reference count so two files can
 * `setupIntegrationServer()` independently and tear down only when the last
 * caller is done.
 */

import { execSync, spawn, type ChildProcess } from "node:child_process";
import { Redis } from "ioredis";

// ---------------------------------------------------------------------------
// Constants (kept together so the isolation story is auditable in one place)
// ---------------------------------------------------------------------------
export const INTEGRATION_PORT = 8199;
export const INTEGRATION_REDIS_URL = "redis://127.0.0.1:6379/15";
const BASE = `http://127.0.0.1:${INTEGRATION_PORT}/mcp`;
const HEALTH = `http://127.0.0.1:${INTEGRATION_PORT}/health`;
const ROOT = new URL("../..", import.meta.url).pathname;

// Module-level build guard so the whole vitest run builds exactly once.
let buildPromise: Promise<void> | null = null;
function ensureBuilt(): Promise<void> {
  if (!buildPromise) {
    buildPromise = (async () => {
      execSync("npm run build", { cwd: ROOT, stdio: "inherit", env: process.env });
    })();
  }
  return buildPromise;
}

export interface IntegrationServer {
  /** MCP endpoint base URL for the spawned server. */
  baseUrl: string;
  /** Direct ioredis client on db15 for raw assertions / Lua evals. */
  redis: Redis;
  /** Scan+del every `gptq:*` key on db15 (never FLUSHDB, never db0). */
  flushGptqKeys: () => Promise<void>;
  /** Tear down: drops one ref; kills the child + closes redis at ref 0. */
  cleanup: () => Promise<void>;
}

interface Singleton {
  server: ChildProcess;
  redis: Redis;
  refs: number;
  closing: boolean;
  stdout: string;
  stderr: string;
}

const GLOBAL_KEY = "__GPTQ_INTEGRATION_SERVER_V1__";
type Glob = typeof globalThis & { [GLOBAL_KEY]?: Singleton };

/** Scan-based deletion of gptq:* keys on a given client + db. Never FLUSHDB. */
export async function flushGptqKeys(redis: Redis): Promise<void> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = (await redis.scan(
      cursor,
      "MATCH",
      "gptq:*",
      "COUNT",
      500
    )) as [string, string[]];
    cursor = next;
    keys.push(...batch);
  } while (cursor !== "0");
  if (keys.length > 0) {
    // Chunked DEL to stay well under any command-size limits.
    for (let i = 0; i < keys.length; i += 200) {
      await redis.del(...keys.slice(i, i + 200));
    }
  }
}

/**
 * Start (once) and return the shared integration server. Idempotent across
 * suite files via a process-global singleton + refcount.
 */
export async function setupIntegrationServer(): Promise<IntegrationServer> {
  await ensureBuilt();

  const global = globalThis as Glob;
  const existing = global[GLOBAL_KEY];
  if (existing && !existing.closing) {
    existing.refs += 1;
    return makeHandle(existing);
  }

  // ---- Fresh startup -----------------------------------------------------
  const server = spawn(
    process.execPath,
    ["dist/transports/http.js", "--port", String(INTEGRATION_PORT)],
    {
      cwd: ROOT,
      env: { ...process.env, REDIS_URL: INTEGRATION_REDIS_URL },
      stdio: ["ignore", "pipe", "pipe"],
    }
  );

  const singleton: Singleton = {
    server,
    redis: new Redis(INTEGRATION_REDIS_URL, { maxRetriesPerRequest: 3 }),
    refs: 1,
    closing: false,
    stdout: "",
    stderr: "",
  };
  server.stdout?.on("data", (d: Buffer) => {
    singleton.stdout = (singleton.stdout + d.toString()).slice(-4000);
  });
  server.stderr?.on("data", (d: Buffer) => {
    singleton.stderr = (singleton.stderr + d.toString()).slice(-4000);
  });

  // Safety net: never leak a child across the process even if cleanup is skipped.
  const exitGuard = () => {
    if (!singleton.closing && !singleton.server.killed) {
      try {
        singleton.server.kill("SIGKILL");
      } catch {
        /* ignore */
      }
    }
  };
  process.once("exit", exitGuard);

  // Wait for health (up to 15s).
  const deadline = Date.now() + 15_000;
  let healthy = false;
  while (Date.now() < deadline) {
    if (singleton.server.exitCode !== null) {
      break; // child died early
    }
    try {
      const res = await fetch(HEALTH, { signal: AbortSignal.timeout(1000) });
      if (res.ok) {
        const body = (await res.json()) as { status: string };
        if (body.status === "ok") {
          healthy = true;
          break;
        }
      }
    } catch {
      /* server not ready yet */
    }
    await new Promise((r) => setTimeout(r, 250));
  }

  if (!healthy) {
    try {
      singleton.server.kill("SIGKILL");
    } catch {
      /* ignore */
    }
    await singleton.redis.quit().catch(() => {});
    throw new Error(
      `integration server on :${INTEGRATION_PORT} failed to become healthy.\n` +
        `exit=${singleton.server.exitCode}\nSTDOUT:\n${singleton.stdout}\nSTDERR:\n${singleton.stderr}`
    );
  }

  global[GLOBAL_KEY] = singleton;
  return makeHandle(singleton);
}

function makeHandle(s: Singleton): IntegrationServer {
  return {
    baseUrl: BASE,
    redis: s.redis,
    flushGptqKeys: () => flushGptqKeys(s.redis),
    cleanup: async () => {
      s.refs -= 1;
      if (s.refs > 0 || s.closing) return;
      s.closing = true;
      try {
        s.server.kill("SIGTERM");
      } catch {
        /* ignore */
      }
      // Give the child a beat to shut down, then force-kill if needed.
      await new Promise((r) => setTimeout(r, 300));
      if (s.server.exitCode === null) {
        try {
          s.server.kill("SIGKILL");
        } catch {
          /* ignore */
        }
      }
      try {
        await s.redis.quit();
      } catch {
        /* ignore */
      }
      const global = globalThis as Glob;
      if (global[GLOBAL_KEY] === s) delete global[GLOBAL_KEY];
    },
  };
}