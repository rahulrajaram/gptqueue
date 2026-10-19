/**
 * F4 regression suite: subprocess-level coverage of the doctor CLI
 * (scripts/gptqueue-doctor.mjs) — every command, option-parsing failures,
 * missing required options, Redis close-on-error, continuity-plan file
 * creation (wx + 0600), --apply gating, and continuity-apply outcomes.
 */
import { privateRedisEnv } from "./helpers/test-redis.js";
import { testRedisUrl } from "./helpers/test-redis.js";
import { describe, it, expect, beforeAll, beforeEach, afterEach } from "vitest";
import { spawnSync, execSync } from "node:child_process";
import { mkdtemp, stat, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS, SYSTEM_SENDER } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import {
  probeActivationReady,
  bindingAgrees,
} from "../src/core/doctor-probe.js";

const ROOT = process.cwd();
const SCRIPT = join(ROOT, "scripts/gptqueue-doctor.mjs");
const TEST_REDIS_URL = testRedisUrl();

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
  }, 180_000); // tsc under full-suite load exceeds the 10 s default hookTimeout

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

  it("reap-offline defaults to a dry run, reaps only with --apply, and rejects --dry-run with --apply", async () => {
    // Mutations caught: defaulting dryRun to false; ignoring --apply; accepting both flags.
    const name = `gptqueue-shell-pi-x-${randomUUID()}`;
    await redis.hset(SESSION_KEYS.registry, name,
      JSON.stringify({ name, role: "both", registered_at: new Date(Date.now() - 3 * 86_400_000).toISOString() }));
    const dry = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--json"]);
    expect(dry.status).toBe(0);
    const report = JSON.parse(dry.stdout);
    expect(report.dry_run).toBe(true);
    expect(report.candidates.map((c: { name: string }) => c.name)).toEqual([name]);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    const text = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--min-age-hours", "1"]);
    expect(text.stdout).toContain("would_reap: 1");
    const both = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--dry-run", "--apply"]);
    expect(both.status).not.toBe(0);
    const applied = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--apply", "--json"]);
    expect(applied.status).toBe(0);
    expect(JSON.parse(applied.stdout).counts.reaped).toBe(1);
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(0);
  });

  it("reap-offline exit codes: 0 evaluated, 4 refused without writes (breaker or batch-wide type), 3 timeout, 1 usage, 2 error", async () => {
    // Mutations caught: a refused batch exiting 0 (a timer would count it healthy); dropping the timeout exit.
    const name = `gptqueue-shell-pi-x-${randomUUID()}`;
    await redis.hset(SESSION_KEYS.registry, name,
      JSON.stringify({ name, role: "both", registered_at: new Date(Date.now() - 3 * 86_400_000).toISOString() }));
    const ok = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--json"]);
    expect(ok.status).toBe(0);
    expect(JSON.parse(ok.stdout).refused).toBeNull();
    for (const flag of ["--dry-run", "--apply"]) {
      const big = run(["reap-offline", "--redis-url", TEST_REDIS_URL, flag, "--max-keys", "0", "--json"]);
      expect(big.status, flag).toBe(4);
      expect(JSON.parse(big.stdout).refused).toBe("keyspace_too_large");
    }
    await redis.set(SESSION_KEYS.identityAudit, "not-a-stream");
    for (const flag of ["--dry-run", "--apply"]) {
      const typed = run(["reap-offline", "--redis-url", TEST_REDIS_URL, flag, "--json"]);
      expect(typed.status, flag).toBe(4);
      expect(JSON.parse(typed.stdout).refused).toBe("unexpected_key_type");
    }
    expect(await redis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    await redis.del(SESSION_KEYS.identityAudit);
    expect(run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--timeout-seconds", "0"]).status).toBe(3);
    expect(run(["reap-offlin", "--redis-url", TEST_REDIS_URL]).status).toBe(1);
    expect(run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--bogus", "1"]).status).toBe(2);
  });

  it("reap-offline exits 5 when a per-name unexpected_key_type skip occurs; dry run writes nothing, apply reaps only the healthy name", async () => {
    // Mutation caught: per-name type anomalies exiting 0 (a timer would count them healthy).
    const old = new Date(Date.now() - 3 * 86_400_000).toISOString();
    const seed = async () => {
      const [good, bad] = [`gptqueue-shell-pi-x-${randomUUID()}`, `gptqueue-shell-pi-x-${randomUUID()}`];
      for (const n of [good, bad]) await redis.hset(SESSION_KEYS.registry, n, JSON.stringify({ name: n, role: "both", registered_at: old }));
      await redis.hset(SESSION_KEYS.forward(bad), "x", "1");
      return { good, bad };
    };
    const dry = await seed();
    const before = (await redis.keys("gptq:*")).sort();
    const d = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--dry-run", "--json"]);
    expect(d.status).toBe(5);
    expect(JSON.parse(d.stdout)).toMatchObject({ refused: null, anomalies: 1 });
    expect((await redis.keys("gptq:*")).sort()).toEqual(before);
    await redis.hdel(SESSION_KEYS.registry, dry.good, dry.bad); await redis.del(SESSION_KEYS.forward(dry.bad));
    const live = await seed();
    const a = run(["reap-offline", "--redis-url", TEST_REDIS_URL, "--apply", "--json"]);
    expect(a.status).toBe(5);
    expect(JSON.parse(a.stdout)).toMatchObject({ refused: null, anomalies: 1 });
    expect(await redis.hexists(SESSION_KEYS.registry, live.good)).toBe(0);
    expect(await redis.hexists(SESSION_KEYS.registry, live.bad)).toBe(1);
    expect(await redis.hget(SESSION_KEYS.forward(live.bad), "x")).toBe("1");
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
  it("sweep-provisional reports registrations that claim the reserved system sender (SEC-F4)", async () => {
    const clean = run(["sweep-provisional", "--redis-url", TEST_REDIS_URL]);
    expect(clean.status, clean.stderr).toBe(0);
    expect(JSON.parse(clean.stdout)).toEqual({ swept: [], reserved_sender: { registry: false, sessions: 0 } });
    // State an older server could have written before the name was reserved.
    await redis.hset(SESSION_KEYS.registry, SYSTEM_SENDER, JSON.stringify({ name: SYSTEM_SENDER }));
    await redis.sadd(SESSION_KEYS.agentSessions(SYSTEM_SENDER), "legacy-session-1", "legacy-session-2");
    const dirty = run(["sweep-provisional", "--redis-url", TEST_REDIS_URL]);
    expect(dirty.status, dirty.stderr).toBe(0);
    expect(JSON.parse(dirty.stdout).reserved_sender).toEqual({ registry: true, sessions: 2 });
    await redis.hdel(SESSION_KEYS.registry, SYSTEM_SENDER);
    await redis.del(SESSION_KEYS.agentSessions(SYSTEM_SENDER));
  });

  // Operator recovery for a refused runtime mailbox adoption (P1).
  describe("remap-runtime-mailbox", () => {
    // A pid that existed and has exited (spawnSync reaps it before returning).
    const deadPid = spawnSync(process.execPath, ["-e", ""]).pid!;
    const seed = async () => {
      const runtimeId = randomUUID();
      const from = `remap-from-${randomUUID()}`;
      const to = `remap-to-${randomUUID()}`;
      const key = SESSION_KEYS.runtimeMailbox("codex", runtimeId);
      const mapping = JSON.stringify({ agent: from, working_directory: "/workspace/project" });
      await redis.set(key, mapping);
      await redis.hset(SESSION_KEYS.registry, from, JSON.stringify({ name: from, pid: deadPid }),
        to, JSON.stringify({ name: to, pid: process.pid, metadata: { working_directory: "/workspace/project" } }));
      await redis.set(SESSION_KEYS.heartbeat(to), "alive", "EX", 60);
      await redis.sadd(SESSION_KEYS.provisional, to);
      // Retained work on both names, which the preview must disclose.
      await redis.rpush(SESSION_KEYS.queue(from), JSON.stringify({ id: "q1" }), JSON.stringify({ id: "q2" }));
      const expiry = Date.now() + 600_000;
      await redis.zadd(CLAIM_KEYS.index(from), expiry, "claim-from", Date.now() - 60_000, "claim-expired");
      await redis.rpush(SESSION_KEYS.queue(to), JSON.stringify({ id: "q3" }));
      // An outstanding request FROM still awaits a reply to (F2).
      await redis.set(SESSION_KEYS.outstanding(from, "t-await"), "peer", "EX", 600);
      const args = (...extra: string[]) => ["remap-runtime-mailbox", "--redis-url", TEST_REDIS_URL, "--client", "codex",
        "--runtime-id", runtimeId, "--from", from, "--to", to, ...extra];
      return { runtimeId, from, to, key, mapping, expiry, args };
    };
    const unchanged = async (s: Awaited<ReturnType<typeof seed>>) => {
      expect(await redis.get(s.key)).toBe(s.mapping);
      expect(await redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(1);
      expect(await redis.xlen(SESSION_KEYS.continuityAudit)).toBe(0);
    };

    it("dry run previews both mailboxes' retained work and the guards, writing nothing", async () => {
      const s = await seed();
      const result = run(s.args());
      expect(result.status, result.stderr).toBe(0);
      const out = JSON.parse(result.stdout);
      expect(out).toMatchObject({ mode: "dry_run", outcome: { status: "dry_run_ok" }, ok: true,
        mapping: { key: s.key, agent: s.from, working_directory: "/workspace/project" } });
      expect(out).not.toHaveProperty("cas");
      expect(out.mailboxes.from).toMatchObject({ name: s.from, registered: true, pid: deadPid, pid_alive: false, queue_length: 2,
        unexpired_claims: [{ claim_id: "claim-from", expires_at: new Date(s.expiry).toISOString() }], expired_claims: 1,
        runtime_binding: false, heartbeat: false, provisional: false });
      expect(out.mailboxes.to).toMatchObject({ name: s.to, registered: true, pid: process.pid, pid_alive: true, queue_length: 1,
        unexpired_claims: [], heartbeat: true, runtime_binding: false, provisional: true, working_directory: "/workspace/project" });
      expect(out.correlations).toMatchObject({ complete: true, from: { awaiting_reply: 1, owes_reply: 0,
        sample: [{ sender: s.from, task_id: "t-await", recipient: "peer" }] }, to: { awaiting_reply: 0, owes_reply: 0 } });
      expect(out.guards.map((g: { guard: string; ok: boolean }) => [g.guard, g.ok])).toEqual([
        ["distinct_mailboxes", true], ["mapping_names_from", true], ["to_registered", true],
        ["to_pid_alive", true], ["to_live_registration", true], ["from_dormant", true], ["correlation_disclosure_complete", true]]);
      expect(out.notes.join(" ")).toMatch(/moves no work: 2 queued message\(s\), 1 unexpired claim\(s\), and 1 awaited and 0 owed reply correlation\(s\) stay at/);
      expect(out.notes.join(" ")).toMatch(/pid liveness is checked by this process/);
      await unchanged(s);
    });

    it("--apply yes repoints the mapping, clears TO's provisional mark and audits operator_remap", async () => {
      const s = await seed();
      const result = run(s.args("--apply", "yes"));
      expect(result.status, result.stderr).toBe(0);
      const out = JSON.parse(result.stdout);
      expect(out).toMatchObject({ mode: "apply", outcome: { status: "applied" } });
      expect(JSON.parse((await redis.get(s.key))!)).toEqual({ agent: s.to, working_directory: "/workspace/project" });
      expect(await redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(0);
      const entries = await redis.xrange(SESSION_KEYS.continuityAudit, "-", "+");
      expect(entries).toHaveLength(1);
      expect(entries[0]![0]).toBe(out.outcome.audit_id);
      expect(entries[0]![1]).toEqual(["event", "operator_remap", "target", s.to, "source", s.from, "runtime_id", s.runtimeId, "client", "codex"]);
      // No work moves: FROM keeps its queue and claims.
      expect(await redis.llen(SESSION_KEYS.queue(s.from))).toBe(2);
      expect(await redis.zcard(CLAIM_KEYS.index(s.from))).toBe(2);
    });

    const refusals: Array<[string, (s: Awaited<ReturnType<typeof seed>>) => Promise<unknown>]> = [
      ["mapping_names_from", (s) => redis.set(s.key, JSON.stringify({ agent: `other-${randomUUID()}`, working_directory: "/workspace/project" }))],
      ["to_registered", (s) => redis.hdel(SESSION_KEYS.registry, s.to)],
      ["to_pid_alive", (s) => redis.hset(SESSION_KEYS.registry, s.to, JSON.stringify({ name: s.to, pid: deadPid }))],
      ["to_live_registration", (s) => redis.del(SESSION_KEYS.heartbeat(s.to))],
      ["from_dormant", (s) => redis.set(SESSION_KEYS.runtimeBinding(s.from), "bound")],
      ["from_dormant", (s) => redis.set(SESSION_KEYS.heartbeat(s.from), "alive", "EX", 60)],
    ];
    it.each(refusals)("refuses %s with exit 6 and a clear message, writing nothing (dry run and apply)", async (guard, mutate) => {
      const s = await seed();
      await mutate(s);
      const current = await redis.get(s.key);
      for (const extra of [[], ["--apply", "yes"]]) {
        const result = run(s.args(...extra));
        expect(result.status, result.stderr).toBe(6);
        expect(result.stderr).toMatch(new RegExp(`^refused: ${guard}: \\S`, "mu"));
        expect(JSON.parse(result.stdout)).toMatchObject({ outcome: { status: "refused" }, ok: false });
      }
      expect(await redis.get(s.key)).toBe(current);
      expect(await redis.sismember(SESSION_KEYS.provisional, s.to)).toBe(1);
      expect(await redis.xlen(SESSION_KEYS.continuityAudit)).toBe(0);
    });

    it("refuses an incomplete correlation disclosure unless --accept-incomplete-disclosure yes (F2)", async () => {
      const s = await seed();
      for (let i = 0; i < 64; i++) await redis.set(`gptq:test-filler:${i}`, "x");
      const capped = run(s.args("--max-correlation-scan", "1", "--apply", "yes"));
      expect(capped.status, capped.stderr).toBe(6);
      expect(capped.stderr).toMatch(/^refused: correlation_disclosure_complete: correlation SCAN stopped at its cap/mu);
      expect(JSON.parse(capped.stdout).correlations).toMatchObject({ complete: false, scan_limit: 1 });
      await unchanged(s);
      const accepted = run(s.args("--max-correlation-scan", "1", "--accept-incomplete-disclosure", "yes", "--apply", "yes"));
      expect(accepted.status, accepted.stderr).toBe(0);
      expect(JSON.parse(accepted.stdout)).toMatchObject({ outcome: { status: "applied" }, correlations: { complete: false } });
      expect(JSON.parse((await redis.get(s.key))!).agent).toBe(s.to);
    });

    it("refuses FROM equal to TO", async () => {
      const s = await seed();
      const result = run(["remap-runtime-mailbox", "--redis-url", TEST_REDIS_URL, "--client", "codex",
        "--runtime-id", s.runtimeId, "--from", s.from, "--to", s.from, "--apply", "yes"]);
      expect(result.status).toBe(6);
      expect(result.stderr).toMatch(/^refused: distinct_mailboxes: /mu);
      await unchanged(s);
    });

    it("exits 1 (usage) for bad or missing options, before touching state (F5)", async () => {
      const s = await seed();
      const cases: Array<[string[], string]> = [
        [["remap-runtime-mailbox", "--redis-url", TEST_REDIS_URL, "--client", "opencode", "--runtime-id", s.runtimeId, "--from", s.from, "--to", s.to],
          "--client must be codex or pi"],
        [s.args("--apply", "true"), "Pass --apply yes"],
        [s.args("--accept-incomplete-disclosure", "true"), "Pass --accept-incomplete-disclosure yes"],
        [s.args("--max-correlation-scan", "0"), "--max-correlation-scan must be a positive integer"],
        [s.args("--cwd", "/workspace"), "Unknown option(s) for remap-runtime-mailbox: --cwd"],
        [["remap-runtime-mailbox", "--redis-url", TEST_REDIS_URL, "--client", "codex", "--runtime-id", s.runtimeId, "--from", s.from], "Required --to"],
        [["remap-runtime-mailbox", "--client", "codex", "--runtime-id", s.runtimeId, "--from", s.from, "--to", s.to], "Required --redis-url"],
        [s.args("--apply"), "Expected unique option/value pairs"],
      ];
      for (const [args, message] of cases) {
        const result = run(args);
        expect(result.status, message).toBe(1);
        expect(result.stderr).toContain(message);
      }
      await unchanged(s);
    });
  });
});

describe("config subcommand (baseline lint, review fixes)", () => {
  const runConfig = (home: string, args: string[]) => {
    const result = spawnSync(process.execPath, ["scripts/gptqueue-doctor.mjs", ...args], {
      cwd: ROOT,
      encoding: "utf8",
      timeout: 30_000,
      env: privateRedisEnv({ ...process.env, CODEX_HOME: home }),
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
