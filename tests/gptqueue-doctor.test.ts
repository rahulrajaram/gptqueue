/**
 * F4 regression suite: subprocess-level coverage of the doctor CLI
 * (scripts/gptqueue-doctor.mjs) — every command, option-parsing failures,
 * missing required options, Redis close-on-error, continuity-plan file
 * creation (wx + 0600), --apply gating, and continuity-apply outcomes.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach } from "vitest";
import { spawnSync, execSync } from "node:child_process";
import { mkdtemp, stat, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import {
  probeActivationReady,
  bindingAgrees,
} from "../src/core/doctor-probe.js";

const ROOT = process.cwd();
const SCRIPT = join(ROOT, "scripts/gptqueue-doctor.mjs");
const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

const run = (args: string[]) => {
  const result = spawnSync(process.execPath, [SCRIPT, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 30_000,
  });
  return {
    status: result.status,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
};

describe("gptqueue-doctor CLI", () => {
  let redis: Redis;
  let workdir: string;

  // The script imports from dist/, so build once before the suite.
  beforeAll(() => {
    execSync("npm run build", { cwd: ROOT, stdio: "inherit" });
  });

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    workdir = await mkdtemp(join(tmpdir(), "gptqueue-doctor-"));
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
    await rm(workdir, { recursive: true, force: true });
  });

  const seedAgent = async (name: string, metadata: Record<string, unknown> = {}) => {
    await redis.hset(
      SESSION_KEYS.registry,
      name,
      JSON.stringify({ name, role: "both", registered_at: new Date().toISOString(), metadata })
    );
  };

  // -- usage and option parsing ---------------------------------------------
  it("prints usage and exits 1 for an unknown command, 0 for --help", () => {
    const unknown = run(["bogus"]);
    expect(unknown.status).toBe(1);
    expect(unknown.stdout).toContain("Usage");
    expect(run([]).status).toBe(1);
    const help = run(["--help"]);
    expect(help.status).toBe(0);
    expect(help.stdout).toContain("Usage");
  });

  it("rejects duplicate options, missing values, and non-option arguments", () => {
    const dup = run(["agent", "--redis-url", TEST_REDIS_URL, "--agent", "a", "--agent", "b"]);
    expect(dup.status).not.toBe(0);
    expect(dup.stderr).toContain("unique");

    const missingValue = run(["agent", "--redis-url", TEST_REDIS_URL, "--agent"]);
    expect(missingValue.status).not.toBe(0);

    const positional = run(["agent", "--redis-url", TEST_REDIS_URL, "bare"]);
    expect(positional.status).not.toBe(0);
  });

  it("requires --redis-url and per-command required options, closing Redis on error", () => {
    const noUrl = run(["agent", "--agent", "a"]);
    expect(noUrl.status).not.toBe(0);
    expect(noUrl.stderr).toContain("Required --redis-url");

    // Valid URL but missing the command's own required option: the script
    // must fail fast (finally: redis.quit()) instead of hanging.
    const noAgent = run(["agent", "--redis-url", TEST_REDIS_URL]);
    expect(noAgent.status).not.toBe(0);
    expect(noAgent.stderr).toContain("Required --agent");
  });

  // -- read-only commands ----------------------------------------------------
  it("agent reports diagnostics for a registered agent and unknown_agent otherwise", async () => {
    await seedAgent("doc-worker");
    const known = run(["agent", "--redis-url", TEST_REDIS_URL, "--agent", "doc-worker"]);
    expect(known.status).toBe(0);
    const payload = JSON.parse(known.stdout);
    expect(payload.name).toBe("doc-worker");
    expect(payload).toHaveProperty("readiness");
    expect(payload).toHaveProperty("queue");

    const unknown = run(["agent", "--redis-url", TEST_REDIS_URL, "--agent", "no-such-agent"]);
    expect(unknown.status).toBe(0);
    expect(JSON.parse(unknown.stdout).next_action).toBe("unknown_agent");
  });

  it("find filters by working directory", async () => {
    await seedAgent("doc-a", { working_directory: "/tmp/doc-dir" });
    await seedAgent("doc-b", { working_directory: "/tmp/other" });
    const found = run(["find", "--redis-url", TEST_REDIS_URL, "--cwd", "/tmp/doc-dir"]);
    expect(found.status).toBe(0);
    const payload = JSON.parse(found.stdout);
    expect(payload.total_matches).toBe(1);
    expect(payload.matches[0]?.name).toBe("doc-a");
  });

  it("delivery reports a queued message", async () => {
    await seedAgent("doc-worker");
    await redis.rpush(
      SESSION_KEYS.queue("doc-worker"),
      JSON.stringify({ id: "doc-m1", type: "task" })
    );
    const result = run([
      "delivery", "--redis-url", TEST_REDIS_URL,
      "--agent", "doc-worker", "--message-id", "doc-m1",
    ]);
    expect(result.status).toBe(0);
    expect(JSON.parse(result.stdout).status).toBe("queued");
  });

  it("connection fails fast when --thread-id is missing", () => {
    const result = run(["connection", "--redis-url", TEST_REDIS_URL]);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("Required --thread-id");
  });

  // -- continuity plan / apply ----------------------------------------------
  const continuityFixture = async () => {
    // Realistic adoption shape: the runtime mailbox is currently mapped at a
    // PROVISIONAL source agent; the plan targets the canonical agent.
    const target = `doc-canonical-${randomUUID().slice(0, 8)}`;
    const provisional = `doc-provisional-${randomUUID().slice(0, 8)}`;
    const runtimeId = randomUUID();
    await seedAgent(target);
    await seedAgent(provisional);
    const mappingKey = `gptq:runtime-mailbox:${createHash("sha256")
      .update(JSON.stringify(["codex", runtimeId]))
      .digest("hex")}`;
    await redis.set(
      mappingKey,
      JSON.stringify({ agent: provisional, working_directory: process.cwd() })
    );
    return { target, provisional, runtimeId };
  };

  it("continuity-plan writes a 0600 plan file once (wx) and refuses to overwrite", async () => {
    const { target, runtimeId } = await continuityFixture();
    const out = join(workdir, "plan.json");
    const created = run([
      "continuity-plan", "--redis-url", TEST_REDIS_URL,
      "--client", "codex", "--runtime-id", runtimeId, "--cwd", process.cwd(),
      "--agent", target, "--out", out,
    ]);
    expect(created.status).toBe(0);
    const mode = (await stat(out)).mode & 0o777;
    expect(mode).toBe(0o600);
    const plan = JSON.parse(await readFile(out, "utf8"));
    expect(plan.target).toBe(target);
    expect(plan.binding.client).toBe("codex");

    const again = run([
      "continuity-plan", "--redis-url", TEST_REDIS_URL,
      "--client", "codex", "--runtime-id", runtimeId, "--cwd", process.cwd(),
      "--agent", target, "--out", out,
    ]);
    expect(again.status).not.toBe(0);
  });

  it("continuity-apply requires --apply yes, then applies and is idempotent", async () => {
    const { target, runtimeId } = await continuityFixture();
    const out = join(workdir, "plan.json");
    expect(
      run([
        "continuity-plan", "--redis-url", TEST_REDIS_URL,
        "--client", "codex", "--runtime-id", runtimeId, "--cwd", process.cwd(),
        "--agent", target, "--out", out,
      ]).status
    ).toBe(0);

    const ungated = run([
      "continuity-apply", "--redis-url", TEST_REDIS_URL, "--plan", out,
    ]);
    expect(ungated.status).not.toBe(0);
    expect(ungated.stderr).toContain("Operator approval required");

    const applied = run([
      "continuity-apply", "--redis-url", TEST_REDIS_URL, "--plan", out, "--apply", "yes",
    ]);
    expect(applied.status).toBe(0);
    expect(JSON.parse(applied.stdout).status).toBe("applied");

    const replay = run([
      "continuity-apply", "--redis-url", TEST_REDIS_URL, "--plan", out, "--apply", "yes",
    ]);
    expect(replay.status).toBe(0);
    expect(JSON.parse(replay.stdout).status).toBe("idempotent");
  });

  it("continuity-plan supports explicit legacy adoption", async () => {
    const target = `doc-legacy-${randomUUID().slice(0, 8)}`;
    await seedAgent(target);
    const out = join(workdir, "legacy-plan.json");
    const created = run([
      "continuity-plan", "--redis-url", TEST_REDIS_URL,
      "--client", "codex", "--runtime-id", randomUUID(), "--cwd", process.cwd(),
      "--agent", target, "--legacy", "yes", "--out", out,
    ]);
    expect(created.status).toBe(0);
    const plan = JSON.parse(await readFile(out, "utf8"));
    expect(plan.legacy_adoption).toBe(true);
    expect(plan.source).toBeNull();
  });

  // D7: the doctor's connection probe gates activation_ready on the same
  // identity/binding conjunction as the MCP probe — not on
  // `status.activation_ready === true` alone.
  describe("connection probe readiness conjunction (D7)", () => {
    const ready = {
      agent: "probe-target",
      activation_ready: true,
      runtime: { runtime_id: "r-1", epoch: "e-1" },
    };
    const binding = { runtime_id: "r-1", epoch: "e-1" };

    it("reports ready only on the full conjunction", () => {
      expect(probeActivationReady(false, ready, "probe-target", binding)).toBe(true);
    });

    it.each([
      { name: "error response", isError: true, status: ready, expected: "probe-target", binding },
      { name: "not activation_ready", isError: false, status: { ...ready, activation_ready: false }, expected: "probe-target", binding },
      { name: "wrong agent vs --agent", isError: false, status: { ...ready, agent: "someone-else" }, expected: "probe-target", binding },
      { name: "no agent in status", isError: false, status: { activation_ready: true, runtime: ready.runtime }, expected: undefined, binding },
      { name: "binding runtime_id mismatch", isError: false, status: { ...ready, runtime: { runtime_id: "other", epoch: "e-1" } }, expected: "probe-target", binding },
      { name: "binding epoch mismatch", isError: false, status: { ...ready, runtime: { runtime_id: "r-1", epoch: "other" } }, expected: "probe-target", binding },
    ])("refuses readiness for $name", ({ isError, status, expected, binding: b }) => {
      expect(probeActivationReady(isError, status, expected, b)).toBe(false);
    });

    it("skips binding agreement only when no binding is known", () => {
      expect(bindingAgrees(ready, null)).toBeNull();
      expect(probeActivationReady(false, ready, undefined, null)).toBe(true);
      expect(bindingAgrees(ready, binding)).toBe(true);
      expect(bindingAgrees({ ...ready, runtime: { runtime_id: "x", epoch: "e-1" } }, binding)).toBe(false);
    });

    it("does not gate on --agent when it was not supplied", () => {
      expect(probeActivationReady(false, ready, undefined, binding)).toBe(true);
    });
  });
});

describe("config subcommand (baseline lint, review fixes)", () => {
  const runConfig = (home: string, args: string[]) => {
    const result = spawnSync(process.execPath, ["scripts/gptqueue-doctor.mjs", ...args], {
      cwd: ROOT,
      encoding: "utf8",
      timeout: 30_000,
      env: { ...process.env, CODEX_HOME: home },
    });
    return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
  };

  const seedConfig = async (home: string, body: string) => {
    await writeFile(join(home, "config.toml"), body, "utf8");
  };

  const BASE_CONFIG = [
    'sandbox_mode = "workspace-write"',
    "",
    "[sandbox_workspace_write]",
    "network_access = true",
    "",
    '[mcp_servers."gptqueue-shared"]',
    'command = "node"',
    'args = ["dist/mcp-server/index.js"]',
    'default_tools_approval_mode = "approve"',
    "",
  ].join("\n");

  let home: string;
  beforeEach(async () => {
    home = await mkdtemp(join(tmpdir(), "gptqueue-codex-home-"));
  });
  afterEach(async () => {
    await rm(home, { recursive: true, force: true });
  });

  it("passes a base config that meets the canonical baseline", async () => {
    await seedConfig(home, BASE_CONFIG);
    const result = runConfig(home, ["config"]);
    expect(result.status).toBe(0);
    const parsed = JSON.parse(result.stdout);
    expect(parsed.status).toBe("ok");
    expect(parsed.checks.every((check: { ok: boolean }) => check.ok)).toBe(true);
  });

  it("fails the approval check when the key sits under another section (false-positive regression)", async () => {
    await seedConfig(home, BASE_CONFIG.replace('[mcp_servers."gptqueue-shared"]', '[mcp_servers."other-server"]'));
    const result = runConfig(home, ["config"]);
    expect(result.status).toBe(1);
    const parsed = JSON.parse(result.stdout);
    expect(parsed.status).toBe("baseline_not_met");
    const approval = parsed.checks.find(
      (check: { key: string }) => check.key.includes("default_tools_approval_mode"),
    );
    expect(approval.ok).toBe(false);
  });

  it("fails the network_access check when the key sits under the wrong table", async () => {
    await seedConfig(home, [
      'sandbox_mode = "workspace-write"',
      "",
      "[some_other_table]",
      "network_access = true",
      "",
      "[sandbox_workspace_write]",
      "",
      '[mcp_servers."gptqueue-shared"]',
      'default_tools_approval_mode = "approve"',
      "",
    ].join("\n"));
    const result = runConfig(home, ["config"]);
    expect(result.status).toBe(1);
    const parsed = JSON.parse(result.stdout);
    expect(parsed.status).toBe("baseline_not_met");
    const network = parsed.checks.find(
      (check: { key: string }) => check.key.includes("network_access"),
    );
    expect(network.ok).toBe(false);
  });

  it("reports a clean error (no stack frames) for a missing config file", async () => {
    const result = runConfig(home, ["config"]);
    expect(result.status).toBe(2);
    expect(result.stderr).toContain("No Codex config at");
    expect(result.stderr).not.toContain("\tat ");
  });

  it("rejects an unknown --tier value", async () => {
    await seedConfig(home, BASE_CONFIG);
    const result = runConfig(home, ["config", "--tier", "bogus"]);
    expect(result.status).toBe(2);
    expect(result.stderr).toContain("Unknown tier");
  });

  it("rejects unknown options on the config subcommand", async () => {
    await seedConfig(home, BASE_CONFIG);
    const result = runConfig(home, ["config", "--redis-url", "redis://127.0.0.1:6379/0"]);
    expect(result.status).toBe(2);
    expect(result.stderr).toContain("Unknown option");
  });
});
