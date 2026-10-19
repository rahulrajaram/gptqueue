/**
 * Shared integration-test server harness.
 *
 * Builds the project once (module-level guard), spawns the real HTTP MCP server
 * (`dist/transports/http.js`) on an OS-assigned loopback port (`--port 0`; the
 * bound port is read from the server's startup line, so concurrent runs never
 * collide) against the run's PRIVATE socket-only Redis, and returns handles for
 * talking to it over the wire plus a direct ioredis client on the same
 * database for raw assertions.
 *
 * Isolation contract: the Redis target is the run's private instance
 * (tests/helpers/test-redis.ts; any TCP URL is refused), every key we
 * read/write lives under `gptq:*`, and we never call FLUSHDB — only key
 * deletions scoped to `gptq:*` via SCAN.
 *
 * The server child and the direct redis handle are shared across suite files
 * through a process-global singleton with a reference count so two files can
 * `setupIntegrationServer()` independently and tear down only when the last
 * caller is done.
 */

import { execSync, spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, writeFileSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { flushTestKeys } from "./redis-test-utils.js";
import { privateRedisEnv, testRedisUrl } from "./test-redis.js";
import { WAKE_SLEEPY_SCRIPT, WAKE_EXIT_SCRIPT } from "./wake-launch.js";

// ---- Launch-allowlist scaffold for the spawned server --------------------
// The server child enforces the operator launch allowlist fail-closed at
// admission + dispatch (v2: EXACT argv templates). Scaffold a temp allowlist
// permitting process.execPath pointed at the fixed wake fixtures (interpreter
// inline-code flags like `node -e` are rejected unconditionally) and the test
// dead binary, and pass GPTQUEUE_LAUNCH_ALLOWLIST so the server resolves it
// regardless of cwd.
const allowlistPath = join(
  mkdtempSync(join(tmpdir(), "gptqueue-integration-allowlist-")),
  "launch-allowlist.json"
);
writeFileSync(
  allowlistPath,
  JSON.stringify(
    {
      version: 2,
      commands: [
        {
          command: process.execPath,
          allowed_args: [[WAKE_SLEEPY_SCRIPT], [WAKE_EXIT_SCRIPT]],
          comment: "integration test node launcher (fixed fixture scripts)",
        },
        {
          command: "/nonexistent/definitely-not-a-binary-987654",
          allowed_args: [[]],
          comment: "integration test dead binary (launch failure path)",
        },
      ],
    },
    null,
    2
  )
);
// Explicitly owner-only: on group-writable-by-default temp
// filesystems (e.g. virtiofs CI runners) the launch policy correctly
// rejects group/world-writable allowlists; tests must not weaken it.
chmodSync(allowlistPath, 0o600);

// ---- Constants (kept together so the isolation story is auditable) -------
/** The run's private socket Redis (validated; refuses TCP). */
export const INTEGRATION_REDIS_URL = testRedisUrl();
/** The server's startup line; with --port 0 it names the OS-assigned port. */
const LISTENING = /gptqueue HTTP server listening on 127\.0\.0\.1:(\d+)/u;
const ROOT = new URL("../..", import.meta.url).pathname;

// Module-level build guard so the whole vitest run builds exactly once.
let buildPromise: Promise<void> | null = null;
function ensureBuilt(): Promise<void> {
  if (!buildPromise) {
    buildPromise = (async () => {
      execSync("npm run build", { cwd: ROOT, stdio: "inherit", env: privateRedisEnv(process.env) });
    })();
  }
  return buildPromise;
}

export interface IntegrationServer {
  /** MCP endpoint base URL for the spawned server (OS-assigned port). */
  baseUrl: string;
  /** The OS-assigned loopback port the server bound. */
  port: number;
  /** Direct ioredis client on the private test Redis for raw assertions / Lua evals. */
  redis: Redis;
  /** Scan+del every `gptq:*` key on the private test Redis (never FLUSHDB). */
  flushGptqKeys: () => Promise<void>;
  /** Tear down: drops one ref; kills the child + closes redis at ref 0. */
  cleanup: () => Promise<void>;
}

interface Singleton {
  server: ChildProcess;
  port: number | undefined;
  redis: Redis;
  refs: number;
  closing: boolean;
  stdout: string;
  stderr: string;
}

const GLOBAL_KEY = "__GPTQ_INTEGRATION_SERVER_V2__";
type Glob = typeof globalThis & { [GLOBAL_KEY]?: Singleton };

/** Scan-based deletion of gptq:* keys on a given client + db. Never FLUSHDB. */
export async function flushGptqKeys(redis: Redis): Promise<void> {
  // Route through the shared guarded flush: it accepts only the run's private
  // socket Redis and checks the client's actual connection (M11).
  await flushTestKeys(redis, INTEGRATION_REDIS_URL);
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
    ["dist/transports/http.js", "--port", "0"],
    {
      cwd: ROOT,
      env: privateRedisEnv({
        ...process.env,
        GPTQUEUE_LAUNCH_ALLOWLIST: allowlistPath,
        // TCP mode on loopback only; never inherit a socket or host override.
        GPTQUEUE_HTTP_SOCKET: "",
        GPTQUEUE_HOST: "127.0.0.1",
      }, INTEGRATION_REDIS_URL),
      stdio: ["ignore", "pipe", "pipe"],
    }
  );

  const singleton: Singleton = {
    server,
    port: undefined,
    redis: new Redis(INTEGRATION_REDIS_URL, { maxRetriesPerRequest: 3 }),
    refs: 1,
    closing: false,
    stdout: "",
    stderr: "",
  };
  server.stdout?.on("data", (d: Buffer) => {
    singleton.stdout = (singleton.stdout + d.toString()).slice(-4000);
    const bound = LISTENING.exec(singleton.stdout)?.[1];
    if (singleton.port === undefined && bound !== undefined) singleton.port = Number(bound);
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
    if (singleton.port === undefined) {
      await new Promise((r) => setTimeout(r, 50));
      continue; // not listening yet
    }
    try {
      const res = await fetch(`http://127.0.0.1:${singleton.port}/health`, { signal: AbortSignal.timeout(1000) });
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
      `integration server (port ${singleton.port ?? "not bound"}) failed to become healthy.\n` +
        `exit=${singleton.server.exitCode}\nSTDOUT:\n${singleton.stdout}\nSTDERR:\n${singleton.stderr}`
    );
  }

  global[GLOBAL_KEY] = singleton;
  return makeHandle(singleton);
}

function makeHandle(s: Singleton): IntegrationServer {
  if (s.port === undefined) throw new Error("integration server has no bound port");
  return {
    baseUrl: `http://127.0.0.1:${s.port}/mcp`,
    port: s.port,
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
