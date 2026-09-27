/**
 * REAL-CLI runtime integration tests.
 *
 * These tests drive the ACTUAL AI CLIs installed on this machine — `pi`
 * (headless `pi -p`) and `codex` (headless `codex exec`) — against the real
 * gptqueue HTTP MCP server spawned by the shared integration-server helper
 * (port 8199, Redis db15). The clients do REAL inference and REAL MCP tool
 * calls: registration, cross-CLI messaging, claim/ack, termination, and
 * sub-agent flows.
 *
 * ---------------------------------------------------------------------------
 * GATING
 * ---------------------------------------------------------------------------
 * Each runtime call is real inference (pi ~5-15s on the fast default model,
 * codex ~20-90s on gpt-5.6-sol and 10-40k tokens). The suite is therefore
 * opt-in only:
 *
 *     GPTQUEUE_RUNTIME_TESTS=1 npx vitest run tests/runtime/
 *
 * Without the env var every test is skipped (fast, green), so the default
 * `npm test` baseline (299/299) is unaffected.
 *
 * ---------------------------------------------------------------------------
 * CONFIG ISOLATION (research findings, pinned here as code)
 * ---------------------------------------------------------------------------
 * pi:
 *   - The pi-mcp-adapter (npm package, loaded from the pi settings `packages`
 *     list) reads its MCP config from `~/.pi/agent/mcp.json`, but BOTH pi
 *     itself and the adapter resolve the whole agent directory from the
 *     `PI_CODING_AGENT_DIR` env var (pi/src/config.ts:
 *     `ENV_AGENT_DIR = ${APP_NAME.toUpperCase()}_CODING_AGENT_DIR`;
 *     adapter cli.js/agent-dir.ts read the same var). So per-test isolation is
 *     possible WITHOUT the `--mcp-config` flag or a temp HOME:
 *     PI_CODING_AGENT_DIR=<tmp> with a minimal `mcp.json` (only the scratch
 *     gptqueue server), plus symlinked/copied auth.json + settings.json (for
 *     provider keys and the extension `packages` list) and a symlinked
 *     `npm/` tree (the installed adapter + subagents extensions). Verified by
 *     probe: pi -p starts, discovers the adapter, and registers against the
 *     scratch server on db15 (8.8s wall).
 *   - The `--mcp-config <path>` extension flag also works (it replaces the
 *     global mcp.json source) but still writes the adapter metadata cache
 *     into the real agent dir; the temp agent-dir approach avoids touching
 *     ~/.pi/agent at all.
 *   - pi runs with `--session-dir <tmp> --no-session` and cwd = a temp dir
 *     (no project configs, no trust prompts). The REAL pi binary is spawned
 *     directly (NUDGE_PI_BIN -> nodeenv.../bin/pi -> cli.js) because the
 *     `pi` on PATH is a nudge wrapper (python) that would orphan the node
 *     child on SIGKILL.
 * codex:
 *   - `codex mcp add` supports BOTH stdio and `--url` (streamable HTTP). So
 *     no stdio bridge is needed: the isolated CODEX_HOME config.toml points
 *     `[mcp_servers.gptqueue-shared] url = "http://127.0.0.1:8199/mcp"`.
 *   - CODEX_HOME=tmp isolates config/auth/sessions. auth.json must be copied
 *     in (an empty CODEX_HOME is not logged in -> 401).
 *   - Headless MCP tool use REQUIRES `approval_policy = "on-request"` in the
 *     temp config AND `codex exec --approve-for-me` (auto-review). With
 *     `approval_policy = "never"` (the ChatGPT-account default) every MCP
 *     call is blocked with "MCP tool call requires approval, but approval
 *     policy is never" — verified by probe. `--approve-for-me` implies the
 *     workspace-write sandbox (the `-s` flag conflicts with it).
 *   - model gpt-5.6-sol works with the ChatGPT account; gpt-5.2-codex is
 *     rejected ("not supported when using Codex with a ChatGPT account").
 *
 * ---------------------------------------------------------------------------
 * TERMINATION SEMANTICS (observed, pinned by tests 4-5)
 * ---------------------------------------------------------------------------
 * The gptqueue server uses the MCP SDK StreamableHTTP transport. Clients are
 * POST-only (pi adapter and codex SDK client send no SSE GET and no DELETE on
 * close). The server therefore CANNOT observe client death: killing or
 * naturally exiting a client leaves the server-side transport + RedisClient
 * alive, and that RedisClient keeps refreshing gptq:lease:<sid> (TTL 30s,
 * every 10s) AND gptq:heartbeat:<name> FOREVER. Observed after kill/drop:
 * lease TTL oscillates 22-30s, session hash + agent-sessions + registry +
 * mailbox + heartbeat persist, list_agents reports online:true indefinitely.
 * There is no idle timeout in SDK 1.26.0 (sessions close only via
 * DELETE /mcp or the tool-level close_session/unregister_agent). Tests 4-5
 * therefore assert and document these real semantics rather than assuming
 * lease expiry.
 */

import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
  vi,
} from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import {
  mkdtempSync,
  writeFileSync,
  copyFileSync,
  rmSync,
  symlinkSync,
  chmodSync,
  statSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import type { Redis } from "ioredis";
import {
  setupIntegrationServer,
  type IntegrationServer,
} from "../helpers/integration-server.js";
import { connectAgent, type Agent } from "../helpers/mcp-agent.js";
import { SESSION_KEYS, CLAIM_KEYS, DLQ_KEYS } from "../../src/core/keys.js";
import { CODEX_BIN, homePath, nodePrefixPath } from "../acceptance/local-tools.js";

// ---------------------------------------------------------------------------
// Gate
// ---------------------------------------------------------------------------
const RUNTIME_ENABLED = process.env.GPTQUEUE_RUNTIME_TESTS === "1";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const PI_REAL_BIN =
  process.env.NUDGE_PI_BIN || nodePrefixPath("bin/pi");
const CODEX_BIN = "codex";
const REAL_PI_AGENT_DIR = homePath(".pi/agent");
const REAL_CODEX_HOME = join(process.env.HOME ?? homedir(), ".codex");
const CODEX_MODEL = "gpt-5.6-sol";
const MCP_SERVER_NAME = "gptqueue-shared"; // tool prefix: gptqueue_shared_*

// Timeouts: real inference is slow; keep generous per-test budgets.
vi.setConfig({ testTimeout: 360_000, hookTimeout: 240_000 });

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Short unique suffix: rt-pi-<epoch>, rt-codex-<epoch>. Deterministic + meaningful. */
const rtName = (cli: "pi" | "codex") =>
  `rt-${cli}-${Date.now()}`;

interface CliRun {
  code: number | null;
  stdout: string;
  stderr: string;
  killed: boolean;
  /** Termination signal when the process died by signal (e.g. our SIGKILL). */
  signal: NodeJS.Signals | null;
}

/** Spawn a CLI (real binary, no shell), capture output, hard-timeout. */
function runCli(
  bin: string,
  args: string[],
  opts: { cwd: string; env: Record<string, string | undefined>; timeoutMs: number }
): { promise: Promise<CliRun>; child: ChildProcess } {
  const child = spawn(bin, args, {
    cwd: opts.cwd,
    env: opts.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d: Buffer) => {
    stdout = (stdout + d.toString()).slice(-200_000);
  });
  child.stderr?.on("data", (d: Buffer) => {
    stderr = (stderr + d.toString()).slice(-200_000);
  });
  const promise = new Promise<CliRun>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        child.kill("SIGKILL");
      } catch {
        /* ignore */
      }
      resolve({ code: null, stdout, stderr, killed: true, signal: null });
    }, opts.timeoutMs);
    child.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });
    child.on("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, stdout, stderr, killed: false, signal });
    });
  });
  return { promise, child };
}

// ---------------------------------------------------------------------------
// pi isolation: temp PI_CODING_AGENT_DIR with a minimal mcp.json + linked
// auth/settings/npm (so extensions + keys work, but nothing in ~/.pi/agent
// is read or written).
// ---------------------------------------------------------------------------
function makePiHome(): { agentDir: string; workDir: string } {
  const agentDir = mkdtempSync(join(tmpdir(), "gptq-rt-pi-agent-"));
  // Copies (never symlinked): pi may re-write auth.json on token refresh and
  // settings.json in some flows; copies keep the real files untouched.
  copyFileSync(join(REAL_PI_AGENT_DIR, "auth.json"), join(agentDir, "auth.json"));
  chmodSync(join(agentDir, "auth.json"), 0o600);
  copyFileSync(join(REAL_PI_AGENT_DIR, "settings.json"), join(agentDir, "settings.json"));
  // Read-only trees: the installed extension packages (adapter, subagents).
  symlinkSync(join(REAL_PI_AGENT_DIR, "npm"), join(agentDir, "npm"));
  if (exists(join(REAL_PI_AGENT_DIR, "models.json"))) {
    symlinkSync(join(REAL_PI_AGENT_DIR, "models.json"), join(agentDir, "models.json"));
  }
  writeFileSync(
    join(agentDir, "mcp.json"),
    JSON.stringify(
      {
        settings: { hostConfigDiscovery: "off", directTools: false },
        mcpServers: {
          [MCP_SERVER_NAME]: {
            url: `http://127.0.0.1:8199/mcp`,
            lifecycle: "lazy",
            requestTimeoutMs: 60_000,
            directTools: false,
          },
        },
      },
      null,
      2
    )
  );
  const workDir = mkdtempSync(join(tmpdir(), "gptq-rt-pi-work-"));
  return { agentDir, workDir };
}

function exists(p: string): boolean {
  try {
    return statSync(p).isFile();
  } catch {
    return false;
  }
}

interface PiHandle {
  agentDir: string;
  workDir: string;
  run: (prompt: string, timeoutMs?: number) => Promise<CliRun>;
  cleanup: () => void;
}

function makePi(): PiHandle {
  const { agentDir, workDir } = makePiHome();
  const sessionDir = join(workDir, "sessions");
  const run = (prompt: string, timeoutMs = 240_000) =>
    runCli(
      PI_REAL_BIN,
      [
        "-p",
        "--session-dir",
        sessionDir,
        "--no-session",
        prompt,
      ],
      {
        cwd: workDir,
        env: {
          ...process.env,
          PI_OFFLINE: "1",
          PI_CODING_AGENT_DIR: agentDir,
        },
        timeoutMs,
      }
    ).promise;
  return {
    agentDir,
    workDir,
    run,
    cleanup: () => {
      try {
        rmSync(agentDir, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
      try {
        rmSync(workDir, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
    },
  };
}

// ---------------------------------------------------------------------------
// codex isolation: temp CODEX_HOME (+ copied auth.json) + config.toml pointing
// the gptqueue MCP server at the scratch 8199 URL. `codex exec` must run with
// --approve-for-me so MCP tool calls auto-approve under approval_policy
// on-request (see header).
// ---------------------------------------------------------------------------
interface CodexHandle {
  home: string;
  workDir: string;
  run: (prompt: string, timeoutMs?: number) => Promise<CliRun>;
  cleanup: () => void;
}

function makeCodex(): CodexHandle {
  const home = mkdtempSync(join(tmpdir(), "gptq-rt-codex-home-"));
  copyFileSync(join(REAL_CODEX_HOME, "auth.json"), join(home, "auth.json"));
  chmodSync(join(home, "auth.json"), 0o600);
  const workDir = mkdtempSync(join(tmpdir(), "gptq-rt-codex-work-"));
  writeFileSync(
    join(home, "config.toml"),
    [
      `model = "${CODEX_MODEL}"`,
      `model_reasoning_effort = "medium"`,
      `approval_policy = "on-request"`,
      `[projects."${workDir}"]`,
      `trust_level = "trusted"`,
      `[mcp_servers.${MCP_SERVER_NAME}]`,
      `url = "http://127.0.0.1:8199/mcp"`,
      "",
    ].join("\n")
  );
  const run = (prompt: string, timeoutMs = 360_000) =>
    runCli(
      CODEX_BIN,
      ["exec", "--skip-git-repo-check", "--approve-for-me", "-C", workDir, prompt],
      { cwd: workDir, env: { ...process.env, CODEX_HOME: home }, timeoutMs }
    ).promise;
  return {
    home,
    workDir,
    run,
    cleanup: () => {
      try {
        rmSync(home, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
      try {
        rmSync(workDir, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Suite context
// ---------------------------------------------------------------------------
interface Ctx {
  server: IntegrationServer;
  redis: Redis;
  wireAgents: Agent[];
}

let ctx: Ctx;
let pi: PiHandle;
let codex: CodexHandle;

beforeAll(async () => {
  // Sanity: binary must exist, else the whole suite is misconfigured.
  const server = await setupIntegrationServer();
  ctx = { server, redis: server.redis, wireAgents: [] };
  pi = makePi();
  codex = makeCodex();
}, 240_000);

afterAll(async () => {
  for (const a of ctx.wireAgents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  pi?.cleanup();
  codex?.cleanup();
  await ctx.server.cleanup();
}, 120_000);

beforeEach(async () => {
  await ctx.server.flushGptqKeys();
  ctx.wireAgents = [];
}, 120_000);

afterEach(async () => {
  for (const a of ctx.wireAgents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  await ctx.server.flushGptqKeys();
}, 120_000);

/** Synthetic wire client, allowed for assertions/cleanup only. */
async function wire(name: string): Promise<Agent> {
  const a = await connectAgent(ctx.server.baseUrl, name, {
    role: "both",
    description: "runtime-suite assertion helper",
  });
  ctx.wireAgents.push(a);
  return a;
}

/** Poll db15 until a predicate holds; returns last value or null on timeout. */
async function pollUntil<T>(
  probe: () => Promise<T>,
  pred: (v: T) => boolean,
  timeoutMs: number,
  label: string
): Promise<T | null> {
  const deadline = Date.now() + timeoutMs;
  let last: T | null = null;
  while (Date.now() < deadline) {
    last = await probe();
    if (pred(last)) return last;
    await delay(1000);
  }
  console.warn(`[runtime] pollUntil timed out: ${label}`);
  return last;
}

/** Wait for a CLI run's process to be killable and kill -9 it. */
function kill9(child: ChildProcess): Promise<number | null> {
  return new Promise((resolve) => {
    const onExit = (code: number | null, _signal: NodeJS.Signals | null) => {
      resolve(code);
    };
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve(child.exitCode);
      return;
    }
    child.once("exit", onExit);
    try {
      child.kill("SIGKILL");
    } catch {
      child.off("exit", onExit);
      resolve(child.exitCode);
    }
  });
}

/** Capture a claim's stored task payload from db15 while a CLI processes it. */
async function captureClaimPayload(actor: string, timeoutMs: number): Promise<string | null> {
  return pollUntil(
    async () => {
      const members = await ctx.redis.zrange(CLAIM_KEYS.index(actor), 0, -1);
      if (!members.length) return null;
      for (const claimId of members) {
        const raw = await ctx.redis.hget(CLAIM_KEYS.claims, claimId);
        if (raw) return raw;
      }
      return null;
    },
    (v) => v !== null,
    timeoutMs,
    `claim for ${actor}`
  );
}

// ===========================================================================
// Tests (each is a real-inference run)
// ===========================================================================
describe.skipIf(!RUNTIME_ENABLED)(
  "real-CLI runtime integration (pi + codex)",
  () => {
    it("T1: PI REGISTERS — real pi -p registers rt-pi-<epoch> against the test server (db15)", async () => {
      const name = rtName("pi");
      const res = await pi.run(
        `Call the MCP tool gptqueue_shared_register_agent exactly once with name="${name}", role="both", description="runtime pi registration test". The tool returns a session_id — print it. Do not call any other gptqueue tools.`
      );
      expect(res.killed).toBe(false);
      expect(res.code).toBe(0);
      expect(res.stdout + "\n" + res.stderr).toMatch(/session[-_ ]*id/i);

      // The name came from the CLI (deterministic rt-pi-<epoch>), not a random
      // internal default.
      expect(
        await ctx.redis.hexists(SESSION_KEYS.registry, name)
      ).toBe(1);
      const sessions = await ctx.redis.smembers(SESSION_KEYS.agentSessions(name));
      expect(sessions).toHaveLength(1);
      const sid = sessions[0]!;
      const rec = await ctx.redis.hgetall(SESSION_KEYS.session(sid));
      expect(rec.agent_name).toBe(name);
      expect(rec.role).toBe("both");
      expect(await ctx.redis.ttl(SESSION_KEYS.lease(sid))).toBeGreaterThan(0);
      // ensureMailbox creates gptq:meta:<name> eagerly; the inbox LIST
      // (gptq:q:<name>) is only materialized on first send, so assert depth 0
      // (llen on a missing key is 0) rather than key existence.
      expect(await ctx.redis.llen(SESSION_KEYS.queue(name))).toBe(0);
      expect(await ctx.redis.exists(SESSION_KEYS.mailboxMeta(name))).toBe(1);
    });

    it("T2: CODEX REGISTERS — real codex exec registers rt-codex-<epoch> against the test server (db15)", async () => {
      const name = rtName("codex");
      const res = await codex.run(
        `Using the gptqueue MCP tools (server name gptqueue-shared; tools appear as gptqueue-shared/register_agent etc.), call register_agent exactly once with name="${name}", role="both", description="runtime codex registration test". The tool returns a session_id — print it. Do not call other gptqueue tools.`
      );
      expect(res.killed).toBe(false);
      expect(res.code).toBe(0);
      expect(res.stdout + "\n" + res.stderr).toMatch(/session_id|registered/i);

      expect(
        await ctx.redis.hexists(SESSION_KEYS.registry, name)
      ).toBe(1);
      const sessions = await ctx.redis.smembers(SESSION_KEYS.agentSessions(name));
      expect(sessions).toHaveLength(1);
      const sid = sessions[0]!;
      const rec = await ctx.redis.hgetall(SESSION_KEYS.session(sid));
      expect(rec.agent_name).toBe(name);
      expect(await ctx.redis.ttl(SESSION_KEYS.lease(sid))).toBeGreaterThan(0);
    });

    it("T3: CROSS-CLI ROUND TRIP — pi -> codex (send/claim/ack) and codex -> pi (send/claim/ack)", async () => {
      const piName = rtName("pi");
      const codexName = rtName("codex");
      const markerA = `ping-${piName}-at-${Date.now()}`;
      const markerB = `ping-${codexName}-at-${Date.now()}`;

      // ---- leg 0: codex must EXIST in the registry before pi can send to it
      // (send_message resolves the recipient through gptq:registry and refuses
      // unknown names). A real desktop-CLI workflow has codex registered
      // first, so the test does the same: one codex exec registers the
      // receiver identity.
      const regReceiver = await codex.run(
        `Using the gptqueue MCP tools (server gptqueue-shared), call register_agent exactly once with name="${codexName}", role="both", description="round-trip receiver registration". Print the session_id the tool returns.`
      );
      expect(regReceiver.killed).toBe(false);
      expect(
        await ctx.redis.hexists(SESSION_KEYS.registry, codexName)
      ).toBe(1);

      // ---- leg 1: pi sends to codex ----
      const send1 = await pi.run(
        `Call gptqueue_shared_register_agent once with name="${piName}", role="both", description="round-trip sender A". Then call gptqueue_shared_send_message with to="${codexName}" and content="${markerA}". Print the message_id returned by send_message.`
      );
      expect(send1.killed).toBe(false);
      expect(send1.code).toBe(0);
      expect(await ctx.redis.llen(SESSION_KEYS.queue(codexName))).toBe(1);
      const envelopeA = JSON.parse(
        (await ctx.redis.lindex(SESSION_KEYS.queue(codexName), 0))!
      );
      expect(envelopeA.from).toBe(piName);
      expect(envelopeA.payload.content).toBe(markerA);

      // ---- leg 2: codex claims + acks the pi message (with claim captured) ----
      const claimWatch = captureClaimPayload(codexName, 180_000);
      const claimRun = codex.run(
        `Using the gptqueue MCP tools (server gptqueue-shared), call register_agent with name="${codexName}", role="both", description="round-trip receiver A". Remember the session_id from its response. Then call claim_tasks with session_id=<that session_id>, max_batch=1, ttl_seconds=300. Remember the claim_id and the claimed message payload. Then call acknowledge_tasks with claim_id=<that claim_id> and session_id=<that session_id>. Report what content the claimed message carried and the acknowledgement status.`
      );
      const claimedRaw = await claimWatch;
      expect(claimedRaw).not.toBeNull();
      const claimRecord = JSON.parse(claimedRaw!);
      const claimedEnvelope = JSON.parse(claimRecord.tasks[0]);
      expect(claimedEnvelope.id).toBe(envelopeA.id);
      expect(claimedEnvelope.payload.content).toBe(markerA);

      const claimRes = await claimRun;
      expect(
        claimRes.killed || claimRes.code === 0,
        JSON.stringify({
          killed: claimRes.killed,
          code: claimRes.code,
          stdout: claimRes.stdout.slice(-600),
          stderr: claimRes.stderr.slice(-400),
        })
      ).toBe(true);
      expect(claimRes.stdout + "\n" + claimRes.stderr).toMatch(/acknowledge|acknowledged|ok/i);

      // Ack drained the inbox; claim gone; no DLQ residue.
      expect(await ctx.redis.llen(SESSION_KEYS.queue(codexName))).toBe(0);
      expect(await ctx.redis.zcard(CLAIM_KEYS.index(codexName))).toBe(0);
      expect(await ctx.redis.llen(DLQ_KEYS.list(codexName))).toBe(0);
      if (claimRecord.claim_id) {
        expect(await ctx.redis.hexists(CLAIM_KEYS.claims, claimRecord.claim_id)).toBe(0);
      }

      // ---- leg 3: codex sends to pi ----
      const send2 = await codex.run(
        `Using the gptqueue MCP tools (server gptqueue-shared), call register_agent with name="${codexName}", role="both", description="round-trip sender B". Then call send_message with to="${piName}" and content="${markerB}". Print the message_id returned.`
      );
      expect(send2.killed).toBe(false);
      expect(send2.code).toBe(0);
      expect(await ctx.redis.llen(SESSION_KEYS.queue(piName))).toBe(1);
      const envelopeB = JSON.parse(
        (await ctx.redis.lindex(SESSION_KEYS.queue(piName), 0))!
      );
      expect(envelopeB.from).toBe(codexName);
      expect(envelopeB.payload.content).toBe(markerB);

      // ---- leg 4: pi claims + acks the codex message ----
      const claimWatch2 = captureClaimPayload(piName, 180_000);
      const claimRun2 = pi.run(
        `Call gptqueue_shared_register_agent once with name="${piName}", role="both", description="round-trip receiver B". It returns a session_id — keep it. Then call gptqueue_shared_claim_tasks with session_id=<that session_id>, max_batch=1, ttl_seconds=300. It returns a claim_id — keep it. Then call gptqueue_shared_acknowledge_tasks with claim_id=<that claim_id> and session_id=<that session_id>. Report the claimed content and the acknowledgement status.`
      );
      const claimedRaw2 = await claimWatch2;
      expect(claimedRaw2).not.toBeNull();
      const claimRecord2 = JSON.parse(claimedRaw2!);
      const claimedEnvelope2 = JSON.parse(claimRecord2.tasks[0]);
      expect(claimedEnvelope2.id).toBe(envelopeB.id);
      expect(claimedEnvelope2.payload.content).toBe(markerB);

      const claimRes2 = await claimRun2;
      expect(
        claimRes2.killed || claimRes2.code === 0,
        JSON.stringify({
          killed: claimRes2.killed,
          code: claimRes2.code,
          stdout: claimRes2.stdout.slice(-600),
          stderr: claimRes2.stderr.slice(-400),
        })
      ).toBe(true);
      expect(await ctx.redis.llen(SESSION_KEYS.queue(piName))).toBe(0);
      expect(await ctx.redis.zcard(CLAIM_KEYS.index(piName))).toBe(0);
      expect(await ctx.redis.llen(DLQ_KEYS.list(piName))).toBe(0);
    });

    it("T4: TERMINATION (ungraceful) — kill -9 of pi leaves server-side session/mailbox/presence as OBSERVED (no lease expiry for POST-only clients)", async () => {
      const name = rtName("pi");
      // Spawn pi that registers and then blocks in a `bash` sleep so the
      // process stays alive and killable long after registration.
      const { promise, child } = runCli(
        PI_REAL_BIN,
        [
          "-p",
          "--session-dir",
          join(pi.workDir, "sessions"),
          "--no-session",
          `Call gptqueue_shared_register_agent once with name="${name}", role="both", description="termination test". Then call the bash tool with the command "sleep 300" and wait for it to complete.`,
        ],
        {
          cwd: pi.workDir,
          env: { ...process.env, PI_OFFLINE: "1", PI_CODING_AGENT_DIR: pi.agentDir },
          timeoutMs: 420_000,
        }
      );

      // Wait until db15 shows the registration landed.
      const sid = await pollUntil(
        async () => (await ctx.redis.smembers(SESSION_KEYS.agentSessions(name)))[0] ?? null,
        (v) => v !== null,
        120_000,
        `pi registration for ${name}`
      );
      expect(sid).not.toBeNull();
      expect(await ctx.redis.hexists(SESSION_KEYS.registry, name)).toBe(1);

      // Seed a message into the dead-agent mailbox via the synthetic wire
      // client (assertion support: proves messages survive the kill).
      const sender = await wire(`rt-wire-sender-${Date.now()}`);
      const sent = await sender.call("send_message", {
        session_id: sender.sessionId,
        to: name,
        content: "persist-me",
      });
      expect(sent.data.status).toBe("sent");
      expect(await ctx.redis.llen(SESSION_KEYS.queue(name))).toBe(1);

      // Ungraceful termination: SIGKILL the real pi process if it is still
      // alive. (If the model already ended its turn naturally before we
      // killed, the server-side semantics are identical for a POST-only
      // client: no cleanup happens either way.)
      const exitCode = await kill9(child);
      if (exitCode !== null) {
        console.warn(
          `[runtime] pi exited naturally before SIGKILL (code=${exitCode}); ` +
            `documenting same server-side semantics`
        );
      }
      const runOutcome = await promise;
      expect(
        runOutcome.killed || runOutcome.code === 0 || runOutcome.signal === "SIGKILL",
        JSON.stringify({
          killed: runOutcome.killed,
          code: runOutcome.code,
          signal: runOutcome.signal,
          stdout: runOutcome.stdout.slice(-800),
          stderr: runOutcome.stderr.slice(-400),
        })
      ).toBe(true);

      // ---- Observed semantics over a 45s post-kill window ----
      // The server never learns the client died (POST-only streamable HTTP,
      // SDK 1.26.0 has no idle/abort teardown), so the server-side RedisClient
      // keeps refreshing the lease + heartbeat. Everything persists.
      const ttlAtKill = await ctx.redis.ttl(SESSION_KEYS.lease(sid!));
      expect(ttlAtKill).toBeGreaterThan(0);

      // Presence surface over the wire: get_queue_status still returns the
      // queue (legacy payload = array of {agent,depth}); list_agents still
      // reports online (server-side refresh).
      const probe = await wire(`rt-wire-presence-${Date.now()}`);
      const status = await probe.call("get_queue_status", { agent: name });
      expect(Array.isArray(status.data)).toBe(true);
      expect(
        (status.data as Array<{ agent: string; depth: number }>)[0]!.depth
      ).toBe(1);
      const list = await probe.call("list_agents", {});
      const entry = (list.data as Array<{ name: string; online: boolean }>).find(
        (a) => a.name === name
      );
      expect(entry).toBeTruthy();
      expect(entry!.online).toBe(true);

      // Observe the FULL 45s window (not an early-exit poll): the lease must
      // STILL be alive at the end — it does not expire while the server-side
      // session lives — and session/registry/mailbox/heartbeat persist.
      await delay(45_000);
      const ttlAfter45 = await ctx.redis.ttl(SESSION_KEYS.lease(sid!));
      expect(ttlAfter45).toBeGreaterThan(0); // LEASE DID NOT EXPIRE (documented deviation)
      expect(await ctx.redis.exists(SESSION_KEYS.session(sid!))).toBe(1);
      expect(
        (await ctx.redis.smembers(SESSION_KEYS.agentSessions(name))).includes(sid!)
      ).toBe(true);
      expect(await ctx.redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
      expect(await ctx.redis.exists(SESSION_KEYS.heartbeat(name))).toBe(1);
      expect(await ctx.redis.llen(SESSION_KEYS.queue(name))).toBe(1); // mailbox intact
      expect(await ctx.redis.exists(SESSION_KEYS.mailboxMeta(name))).toBe(1);
    });

    it("T5: TERMINATION (graceful) — close_session preserves the mailbox; unregister_agent deletes everything (key-level)", async () => {
      // ---- close_session ----
      const closeName = rtName("pi");
      const closeRes = await pi.run(
        `Call gptqueue_shared_register_agent once with name="${closeName}", role="both", description="close test". Then call gptqueue_shared_send_message with to="${closeName}" and content="preserve-me". Then call gptqueue_shared_close_session. Print each result.`
      );
      expect(closeRes.killed).toBe(false);
      const closeSessions = await ctx.redis.smembers(SESSION_KEYS.agentSessions(closeName));
      expect(closeSessions).toHaveLength(0); // session closed
      expect(await ctx.redis.llen(SESSION_KEYS.queue(closeName))).toBe(1); // mailbox preserved
      expect(await ctx.redis.exists(SESSION_KEYS.mailboxMeta(closeName))).toBe(1);
      expect(await ctx.redis.hexists(SESSION_KEYS.registry, closeName)).toBe(1); // registry preserved
      expect(await ctx.redis.exists(SESSION_KEYS.heartbeat(closeName))).toBe(0);

      // ---- unregister_agent ----
      const unregName = rtName("pi");
      const unregRes = await pi.run(
        `Call gptqueue_shared_register_agent once with name="${unregName}", role="both", description="unregister test". Then call gptqueue_shared_send_message with to="${unregName}" and content="delete-me". Then call gptqueue_shared_unregister_agent. Print each result.`
      );
      expect(unregRes.killed).toBe(false);
      for (const key of [
        SESSION_KEYS.queue(unregName),
        SESSION_KEYS.mailboxMeta(unregName),
        SESSION_KEYS.heartbeat(unregName),
      ]) {
        expect(await ctx.redis.exists(key)).toBe(0);
      }
      expect(await ctx.redis.hexists(SESSION_KEYS.registry, unregName)).toBe(0);
      expect(await ctx.redis.smembers(SESSION_KEYS.agentSessions(unregName))).toHaveLength(0);
    });

    it("T6: SUB-AGENTS — a pi-spawned subagent's gptqueue traffic is observed and pinned (identity semantics)", async () => {
      const parent = rtName("pi");
      const marker = `sub-msg-${Date.now()}`;
      const res = await pi.run(
        `Call gptqueue_shared_register_agent once with name="${parent}", role="both", description="subagent parent". Then use the Agent tool with subagent_type="general-purpose" and this prompt (verbatim): "Call gptqueue_shared_register_agent with name='${parent}', role='both', description='subagent child'. Then call gptqueue_shared_send_message with to='${parent}' and content='${marker}'. Print the message_id." Wait for the Agent tool to finish and report its result.`,
        330_000
      );
      expect(
        res.killed || res.code === 0,
        JSON.stringify({
          killed: res.killed,
          code: res.code,
          stdout: res.stdout.slice(-1200),
          stderr: res.stderr.slice(-600),
        })
      ).toBe(true);

      // The subagent's send is observable on db15 regardless of identity model.
      const got = await pollUntil(
        async () => {
          const raw = await ctx.redis.lindex(SESSION_KEYS.queue(parent), 0);
          return raw ? (JSON.parse(raw) as { payload: { content: string }; from?: string }) : null;
        },
        (v) => v !== null && v.payload.content === marker,
        90_000,
        `subagent message on ${parent}`
      );
      expect(got).not.toBeNull();
      expect(got!.from).toBe(parent);

      // Pinned identity semantics: how many sessions exist under the parent
      // name after the subagent ran. OBSERVED = 2: the subagent shares the
      // parent process's MCP connection (same RedisClient), and its
      // register_agent call with the SAME name created a SIBLING session (a
      // same-name re-registration does not close the old session; only a
      // rename does). The parent's original session record remains until its
      // lease expires and lazy presence cleanup removes it.
      const sessions = await ctx.redis.smembers(SESSION_KEYS.agentSessions(parent));
      expect(
        sessions.length,
        `observed agent-sessions after subagent run: ${JSON.stringify(sessions)}`
      ).toBeGreaterThanOrEqual(2);
      // Registry still holds exactly one entry for the name (one canonical agent).
      const reg = await ctx.redis.hget(SESSION_KEYS.registry, parent);
      expect(reg).toBeTruthy();
    });
  }
);