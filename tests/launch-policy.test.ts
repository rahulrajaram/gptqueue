import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { writeFileSync, rmSync, mkdtempSync, symlinkSync, mkdirSync, renameSync, chmodSync } from "fs";
import { tmpdir } from "os";
import { basename, join } from "path";
import { ActorDirectory } from "../src/core/actor-directory.js";
import {
  commandMatches,
  argsMatchTemplate,
  parseLaunchAllowlist,
  launchMatchesConfig,
  evaluateLaunchPolicy,
  launchCwdIsConfined,
} from "../src/core/launch-policy.js";
import { dispatchLaunch } from "../src/mcp-server/launcher.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";
import { WAKE_SLEEPY_SCRIPT } from "./helpers/wake-launch.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const T0 = "2030-01-01T00:00:00.000Z";

// ---------------------------------------------------------------------------
// Pure matching helpers (exact identity — F1)
// ---------------------------------------------------------------------------
describe("launch allowlist matching (pure)", () => {
  it("commandMatches: bare names match only identically; absolute paths match only resolve-equal absolutes", () => {
    // Bare name: byte-identical only.
    expect(commandMatches("node", "node")).toBe(true);
    expect(commandMatches("node", "python")).toBe(false);
    // Absolute entry: absolute request that resolves to the same path.
    expect(commandMatches("/usr/bin/node", "/usr/bin/node")).toBe(true);
    expect(commandMatches("/usr/./bin/node", "/usr/bin/node")).toBe(true);
    // F1 basename-colliding absolute path does NOT alias a bare allowlist name.
    expect(commandMatches("/attacker/work/node", "node")).toBe(false);
    // A bare request does NOT match an absolute entry, and vice versa.
    expect(commandMatches("node", "/usr/bin/node")).toBe(false);
    expect(commandMatches("/usr/bin/node", "node")).toBe(false);
    // Different absolute paths sharing a basename do not alias.
    expect(commandMatches("/opt/node/bin/node", "/usr/bin/node")).toBe(false);
  });

  it("argsMatchTemplate: full equality only — no suffix freedom, no empty-prefix catch-all", () => {
    expect(argsMatchTemplate([], [])).toBe(true);
    expect(argsMatchTemplate(["a", "b"], ["a", "b"])).toBe(true);
    // Extra trailing args are rejected (v1 allowed them freely).
    expect(argsMatchTemplate(["a", "b", "-e", "x"], ["a", "b"])).toBe(false);
    // Fewer args are rejected.
    expect(argsMatchTemplate(["a"], ["a", "b"])).toBe(false);
    // Order matters.
    expect(argsMatchTemplate(["b", "a"], ["a", "b"])).toBe(false);
    // An empty template accepts ONLY a no-args request.
    expect(argsMatchTemplate(["anything"], [])).toBe(false);
    // Leading-dash args are ordinary template elements: they match only when
    // the operator wrote them explicitly.
    expect(argsMatchTemplate(["--port", "1234"], ["--port", "1234"])).toBe(true);
    expect(argsMatchTemplate(["--port", "9999"], ["--port", "1234"])).toBe(false);
  });

  it("launchMatchesConfig consults every entry", () => {
    const loaded = parseLaunchAllowlist(
      JSON.stringify({
        version: 2,
        commands: [
          { command: "node", allowed_args: [["--port", "1234"]] },
          { command: "git", allowed_args: [[]] },
        ],
      }),
      "test"
    );
    expect(loaded.kind).toBe("loaded");
    if (loaded.kind !== "loaded") throw new Error("expected loaded");
    const config = loaded.config;
    expect(launchMatchesConfig("node", ["--port", "1234"], config)).toBe(true);
    // Absolute path no longer aliases the bare allowlisted name (F1).
    expect(launchMatchesConfig("/usr/bin/node", ["--port", "1234"], config)).toBe(false);
    // Suffix freedom is gone.
    expect(launchMatchesConfig("node", ["--port", "1234", "extra"], config)).toBe(false);
    expect(launchMatchesConfig("node", ["--foo"], config)).toBe(false);
    expect(launchMatchesConfig("git", [], config)).toBe(true);
    expect(launchMatchesConfig("git", ["status"], config)).toBe(false);
    expect(launchMatchesConfig("python", [], config)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Allowlist parsing / rejection of malformed documents
// ---------------------------------------------------------------------------
describe("parseLaunchAllowlist", () => {
  it("loads a valid v2 document", () => {
    const res = parseLaunchAllowlist(
      JSON.stringify({
        version: 2,
        commands: [{ command: "node", allowed_args: [["--port", "1234"], []] }],
      }),
      "test"
    );
    expect(res.kind).toBe("loaded");
    if (res.kind !== "loaded") throw new Error("expected loaded");
    expect(res.config.commands[0]?.command).toBe("node");
  });

  it("hard-rejects version-1 documents with a migration message", () => {
    const res = parseLaunchAllowlist(
      JSON.stringify({
        version: 1,
        commands: [{ command: "node", allowed_args_prefixes: [["-e"]] }],
      }),
      "test"
    );
    expect(res.kind).toBe("unparseable");
    if (res.kind !== "unparseable") throw new Error("expected unparseable");
    expect(res.reason).toContain("version-1");
    expect(res.reason).toContain("allowed_args");
  });

  it("rejects malformed JSON / wrong version / bad shape as unparseable", () => {
    expect(parseLaunchAllowlist("not json", "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist("[]", "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist(JSON.stringify({ version: 3, commands: [] }), "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist(JSON.stringify({ version: 2 }), "x").kind).toBe("unparseable");
    expect(
      parseLaunchAllowlist(JSON.stringify({ version: 2, commands: [{ command: "" }] }), "x").kind
    ).toBe("unparseable");
    expect(
      parseLaunchAllowlist(JSON.stringify({ version: 2, commands: [{ command: "x", allowed_args: "nope" }] }), "x").kind
    ).toBe("unparseable");
    expect(
      parseLaunchAllowlist(JSON.stringify({ version: 2, commands: [{ command: "x", allowed_args: [["ok"], 5] }] }), "x").kind
    ).toBe("unparseable");
  });
});

// ---------------------------------------------------------------------------
// evaluateLaunchPolicy (filesystem + env, no Redis, no spawn)
// ---------------------------------------------------------------------------
describe("evaluateLaunchPolicy", () => {
  it("approves an exact allowlisted command/args and rejects any deviation", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "node", allowed_args: [["--port", "1234"]] },
    ]);
    s.set();
    try {
      const ok = await evaluateLaunchPolicy({
        command: "node",
        args: ["--port", "1234"],
      });
      expect(ok).toEqual({ ok: true });

      // Extra args beyond the template are no longer accepted (F1).
      const extra = await evaluateLaunchPolicy({
        command: "node",
        args: ["--port", "1234", "extra.js"],
      });
      expect(extra).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });

      const rejected = await evaluateLaunchPolicy({
        command: "python",
        args: [],
      });
      expect(rejected).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects a shell delegator even when it is allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "/bin/sh", allowed_args: [[]] },
    ]);
    s.set();
    try {
      const res = await evaluateLaunchPolicy({ command: "/bin/sh", args: ["-c", "echo x"] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects interpreter inline-code flags even when exactly allowlisted (F1)", async () => {
    const s = scaffoldLaunchAllowlist([
      {
        command: "node",
        allowed_args: [["-e", "process.exit(0)"], ["server.js"]],
      },
      { command: "python3", allowed_args: [["-c", "print(1)"], []] },
    ]);
    s.set();
    try {
      const nodeEval = await evaluateLaunchPolicy({
        command: "node",
        args: ["-e", "process.exit(0)"],
      });
      expect(nodeEval).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
      if (!nodeEval.ok) expect(nodeEval.error.message).toMatch(/inline-code/);

      const pythonC = await evaluateLaunchPolicy({
        command: "python3",
        args: ["-c", "print(1)"],
      });
      expect(pythonC).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });

      // Non-inline interpreter invocations still pass policy (template match).
      const okScript = await evaluateLaunchPolicy({
        command: "node",
        args: ["server.js"],
      });
      expect(okScript).toEqual({ ok: true });
    } finally {
      s.cleanup();
    }
  });

  it.each([
    ["node", ["--eval=process.exit(1)"]],
    ["node", ["-e=process.exit(1)"]],
    ["node", ["-p", "process.exit(1)"]],
    ["nodejs", ["-e", "process.exit(1)"]],
    ["python", ["-cprint(1)"]],
    ["python3", ["--command=import os"]],
    ["perl", ["-E'say 1'"]],
    ["php", ["-r", "exit(1);"]],
    ["ruby", ["-e", "exit(1)"]],
    ["awk", ["{ print }", "file.txt"]],
    ["gawk", ["{ print }"]],
  ])("rejects the inline-code spelling %s %s in any form (D1)", async (command, args) => {
    const s = scaffoldLaunchAllowlist([
      { command: command as string, allowed_args: [args as string[]] },
    ]);
    s.set();
    try {
      // Even an exact operator template for the hostile argv is refused:
      // inline-code spellings are rejected regardless of the allowlist.
      const res = await evaluateLaunchPolicy({
        command: command as string,
        args: args as string[],
      });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
      if (!res.ok) expect(res.error.message).toMatch(/inline-code|dangerous delegator/);
    } finally {
      s.cleanup();
    }
  });

  it("still admits non-interpreter commands carrying dash args (D1 negative control)", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "git", allowed_args: [["-c", "foo.bar=1", "status"]] },
    ]);
    s.set();
    try {
      const res = await evaluateLaunchPolicy({
        command: "git",
        args: ["-c", "foo.bar=1", "status"],
      });
      expect(res).toEqual({ ok: true });
    } finally {
      s.cleanup();
    }
  });

  it("rejects basename-colliding absolute paths against bare and absolute entries (F1)", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "node", allowed_args: [[]] },
      { command: "/usr/bin/node", allowed_args: [["server.js"]] },
    ]);
    s.set();
    try {
      // Allowlisted bare name; attacker-controlled absolute path sharing the
      // basename must NOT be admitted.
      const collision = await evaluateLaunchPolicy({
        command: "/attacker/work/node",
        args: [],
      });
      expect(collision).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });

      // Absolute entry does not admit a bare-name request either.
      const bare = await evaluateLaunchPolicy({
        command: "node",
        args: ["server.js"],
      });
      expect(bare).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });

      // The exact absolute request is admitted.
      const exact = await evaluateLaunchPolicy({
        command: "/usr/bin/node",
        args: ["server.js"],
      });
      expect(exact).toEqual({ ok: true });
    } finally {
      s.cleanup();
    }
  });

  it("fail-closes when the allowlist file is absent", async () => {
    process.env.GPTQUEUE_LAUNCH_ALLOWLIST = join(tmpdir(), "does-not-exist-launch-allowlist.json");
    try {
      const res = await evaluateLaunchPolicy({ command: "node", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
      expect(res).toMatchObject({ ok: false });
    } finally {
      delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
    }
  });

  it("defaults to the user config directory, not the working directory", async () => {
    const config = mkdtempSync(join(tmpdir(), "gptqueue-xdg-"));
    const saved = process.env.XDG_CONFIG_HOME;
    delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
    process.env.XDG_CONFIG_HOME = config;
    try {
      const missing = await evaluateLaunchPolicy({ command: "node", args: [] });
      if (missing.ok) throw new Error("expected fail-closed");
      expect(missing.error.message).toContain(join(config, "gptqueue", "launch-allowlist.json"));

      const s = scaffoldLaunchAllowlist([{ command: "node" }], { path: join(config, "launch-allowlist.json") });
      mkdirSync(join(config, "gptqueue"));
      renameSync(s.path, join(config, "gptqueue", "launch-allowlist.json"));
      expect(await evaluateLaunchPolicy({ command: "node", args: [] })).toEqual({ ok: true });
      s.cleanup();
    } finally {
      if (saved === undefined) delete process.env.XDG_CONFIG_HOME;
      else process.env.XDG_CONFIG_HOME = saved;
      rmSync(config, { recursive: true, force: true });
    }
  });

  it("names the new location when only the legacy working-directory allowlist exists", async () => {
    const work = mkdtempSync(join(tmpdir(), "gptqueue-legacy-"));
    const config = mkdtempSync(join(tmpdir(), "gptqueue-xdg-"));
    const [savedCwd, savedXdg] = [process.cwd(), process.env.XDG_CONFIG_HOME];
    delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
    process.env.XDG_CONFIG_HOME = config;
    mkdirSync(join(work, ".gptqueue"));
    writeFileSync(join(work, ".gptqueue", "launch-allowlist.json"), JSON.stringify({ version: 2, commands: [{ command: "node", allowed_args: [[]] }] }));
    process.chdir(work);
    try {
      const res = await evaluateLaunchPolicy({ command: "node", args: [] });
      if (res.ok) throw new Error("legacy allowlist must not be honored");
      expect(res.error.code).toBe("launch_not_allowlisted");
      expect(res.error.message).toContain("no longer read");
      expect(res.error.message).toContain(join(config, "gptqueue", "launch-allowlist.json"));
    } finally {
      process.chdir(savedCwd);
      if (savedXdg === undefined) delete process.env.XDG_CONFIG_HOME;
      else process.env.XDG_CONFIG_HOME = savedXdg;
      rmSync(work, { recursive: true, force: true });
      rmSync(config, { recursive: true, force: true });
    }
  });

  it("refuses a symlinked or group/world-writable allowlist", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    const link = `${s.path}.link`;
    try {
      symlinkSync(s.path, link);
      process.env.GPTQUEUE_LAUNCH_ALLOWLIST = link;
      const viaLink = await evaluateLaunchPolicy({ command: "node", args: [] });
      if (viaLink.ok) throw new Error("symlinked allowlist must be refused");
      expect(viaLink.error.message).toContain("not a regular file");

      process.env.GPTQUEUE_LAUNCH_ALLOWLIST = s.path;
      chmodSync(s.path, 0o664);
      const writable = await evaluateLaunchPolicy({ command: "node", args: [] });
      if (writable.ok) throw new Error("group-writable allowlist must be refused");
      expect(writable.error.message).toContain("group- or world-writable");

      chmodSync(s.path, 0o644);
      expect(await evaluateLaunchPolicy({ command: "node", args: [] })).toEqual({ ok: true });
    } finally {
      delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
      s.cleanup();
    }
  });

  it("fail-closes when the allowlist file is unparseable (including a v1 document)", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      writeFileSync(s.path, "{ not json");
      const res = await evaluateLaunchPolicy({ command: "node", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });

      writeFileSync(
        s.path,
        JSON.stringify({ version: 1, commands: [{ command: "node", allowed_args_prefixes: [[]] }] })
      );
      const v1 = await evaluateLaunchPolicy({ command: "node", args: [] });
      expect(v1).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
      if (!v1.ok) expect(v1.error.message).toMatch(/version-1/);
    } finally {
      s.cleanup();
    }
  });

  it("confines launch_cwd: outside workspace rejected, existing inside accepted", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      const outside = await evaluateLaunchPolicy({
        command: "node",
        args: [],
        cwd: join(tmpdir(), "outside-ws"),
      });
      expect(outside).toMatchObject({ ok: false, error: { code: "launch_cwd_confined" } });

      const inside = await evaluateLaunchPolicy({
        command: "node",
        args: [],
        cwd: process.cwd(),
      });
      expect(inside).toEqual({ ok: true });
    } finally {
      s.cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// Admission through ActorDirectory (Redis db-from-env)
// ---------------------------------------------------------------------------
describe("ActorDirectory admission policy enforcement", () => {
  let redis: Redis;
  let store: ActorDirectory;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new ActorDirectory(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  const register = async (launch?: { command: string; args: string[]; cwd?: string }) =>
    store.register({
      profile_input: {
        actor_id: "pol-actor",
        alias: "pol",
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        runtime: launch?.command ?? "manual",
        activation_policy: { mode: "wake_if_offline" },
        max_concurrency: 1,
      },
      launch: launch ?? null,
      registered_by: "session-a",
      registered_at: T0,
    });

  it("admits an exactly-allowlisted wake_if_offline launch", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "/usr/bin/pi", allowed_args: [["--agent", "pol"]] },
    ]);
    s.set();
    try {
      const res = await register({ command: "/usr/bin/pi", args: ["--agent", "pol"] });
      expect(res.ok).toBe(true);
    } finally {
      s.cleanup();
    }
  });

  it("rejects a non-allowlisted command as launch_not_allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "/usr/bin/pi", allowed_args: [[]] }]);
    s.set();
    try {
      const res = await register({ command: "python", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects a shell delegator even when it is allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "/bin/bash" }]);
    s.set();
    try {
      const res = await register({ command: "/bin/bash", args: ["-lc", "rm -rf /"] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects an interpreter inline-code launch even when exactly allowlisted (F1)", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "node", allowed_args: [["-e", "process.exit(0)"]] },
    ]);
    s.set();
    try {
      const res = await register({ command: "node", args: ["-e", "process.exit(0)"] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects a basename-colliding absolute path against a bare allowlist entry (F1)", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node", allowed_args: [[]] }]);
    s.set();
    try {
      const res = await register({ command: "/attacker/work/node", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects an args-template mismatch (extras, missing, wrong value) as launch_not_allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "node", allowed_args: [["--port", "1234"]] },
    ]);
    s.set();
    try {
      // Extra trailing args.
      const extra = await register({ command: "node", args: ["--port", "1234", "extra.js"] });
      expect(extra).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
      // Missing args.
      const missing = await register({ command: "node", args: ["--port"] });
      expect(missing).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
      // Wrong value.
      const wrong = await register({ command: "node", args: ["--foo"] });
      expect(wrong).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
    } finally {
      s.cleanup();
    }
  });

  it("rejects an unconfined launch_cwd as launch_cwd_confined", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      const res = await register({ command: "node", args: [], cwd: join(tmpdir(), "outside") });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_cwd_confined" } });
    } finally {
      s.cleanup();
    }
  });

  it("fail-closes at admission when the allowlist file is absent", async () => {
    process.env.GPTQUEUE_LAUNCH_ALLOWLIST = join(tmpdir(), "missing-allowlist.json");
    try {
      const res = await register({ command: "node", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
      expect(res).toMatchObject({ ok: false });
    } finally {
      delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
    }
  });

  it("rejects actor_id outside the identity charset as invalid_identity_charset", async () => {
    const res = await store.register({
      profile_input: {
        actor_id: "bad id !",
        alias: "pol",
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        runtime: "manual",
        activation_policy: { mode: "store_only" },
        max_concurrency: 1,
      },
      launch: null,
      registered_by: "session-a",
      registered_at: T0,
    });
    expect(res).toMatchObject({ ok: false, error: { code: "invalid_identity_charset" } });
  });

  it("store_only actors are not policy-gated by the allowlist", async () => {
    const res = await store.register({
      profile_input: {
        actor_id: "store-ok",
        alias: "s",
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        runtime: "manual",
        activation_policy: { mode: "store_only" },
        max_concurrency: 1,
      },
      launch: { command: "/bin/sh", args: ["-c", "echo hi"] },
      registered_by: "session-a",
      registered_at: T0,
    });
    // store_only is never dispatched, so its inert launch metadata is not
    // policy-evaluated here.
    expect(res.ok).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// dispatchLaunch defense-in-depth re-check (real spawns / policy refusal)
// ---------------------------------------------------------------------------
describe("dispatchLaunch re-checks the allowlist (defense in depth)", () => {
  it("dispatches an exactly-allowlisted contract", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: process.execPath, allowed_args: [[WAKE_SLEEPY_SCRIPT]] },
    ]);
    s.set();
    let pid: number | undefined;
    try {
      const res = await dispatchLaunch({
        command: process.execPath,
        args: [WAKE_SLEEPY_SCRIPT],
      });
      expect(res.dispatched).toBe(true);
      pid = res.pid;
      expect(pid).toBeTypeOf("number");
      expect(isAlive(pid!)).toBe(true);
    } finally {
      if (pid !== undefined) killPid(pid);
      s.cleanup();
    }
  });

  it("refuses to spawn after the allowlist is rewritten (between admit and wake)", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: process.execPath, allowed_args: [[WAKE_SLEEPY_SCRIPT]] },
    ]);
    s.set();
    try {
      // Admit while the exact argv is allowlisted.
      const contract = { command: process.execPath, args: [WAKE_SLEEPY_SCRIPT] };
      expect(await evaluateLaunchPolicy(contract)).toEqual({ ok: true });

      // Operator rewrites the allowlist to remove it -> dispatch must refuse.
      writeFileSync(
        s.path,
        JSON.stringify({ version: 2, commands: [{ command: "git", allowed_args: [[]] }] })
      );
      const res = await dispatchLaunch(contract);
      expect(res.dispatched).toBe(false);
      expect(res.error?.code).toBe("launch_failed");
      expect(res.error?.message).toMatch(/allowlist/);
    } finally {
      s.cleanup();
    }
  });

  it("fail-closes at dispatch when the allowlist file is absent", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: process.execPath, allowed_args: [[WAKE_SLEEPY_SCRIPT]] },
    ]);
    s.set();
    try {
      rmSync(s.path, { force: true });
      const res = await dispatchLaunch({
        command: process.execPath,
        args: [WAKE_SLEEPY_SCRIPT],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.code).toBe("launch_failed");
      expect(res.error?.message).toMatch(/fail closed/);
    } finally {
      s.cleanup();
    }
  });

  it("refuses to dispatch a shell despite a stale directory record", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "/bin/sh" }]);
    s.set();
    try {
      const res = await dispatchLaunch({ command: "/bin/sh", args: ["-c", "echo hi"] });
      expect(res.dispatched).toBe(false);
      expect(res.error?.code).toBe("launch_failed");
      expect(res.error?.message).toMatch(/dangerous delegator/);
    } finally {
      s.cleanup();
    }
  });

  it("refuses to dispatch an interpreter inline-code contract despite a stale directory record (F1)", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: process.execPath, allowed_args: [["-e", "process.exit(0)"]] },
    ]);
    s.set();
    try {
      const res = await dispatchLaunch({
        command: process.execPath,
        args: ["-e", "process.exit(0)"],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.code).toBe("launch_failed");
      expect(res.error?.message).toMatch(/inline-code/);
    } finally {
      s.cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// launch_cwd symlink behavior (F10 documented limitation)
// ---------------------------------------------------------------------------
describe("launchCwdIsConfined symlink handling (F10)", () => {
  it("accepts a symlink whose lexical prefix is inside the workspace (documented limitation)", async () => {
    // A symlink inside the workspace root pointing at an outside directory
    // passes the lexical resolve()+prefix check: realpath-based escape
    // protection is an explicitly documented limitation of this layer.
    const insideRoot = mkdtempSync(join(process.cwd(), ".tmp-launch-cwd-"));
    const outsideDir = mkdtempSync(join(tmpdir(), "gptqueue-outside-"));
    try {
      const link = join(insideRoot, "escape");
      symlinkSync(outsideDir, link);
      const confined = await launchCwdIsConfined(link);
      expect(confined).toEqual({ ok: true });
    } finally {
      rmSync(insideRoot, { recursive: true, force: true });
      rmSync(outsideDir, { recursive: true, force: true });
    }
  });

  it("rejects a lexically escaping path (.. traversal) and outside prefixes", async () => {
    // A REAL directory outside the workspace root (a sibling of the repo),
    // addressed both directly and via a lexical .. traversal from the root.
    const parent = join(process.cwd(), "..");
    const outsideDir = mkdtempSync(join(parent, ".gptqueue-outside-"));
    try {
      const traversal = await launchCwdIsConfined(
        join(process.cwd(), "..", basename(outsideDir))
      );
      expect(traversal).toMatchObject({ ok: false });
      if (!traversal.ok) expect(traversal.message).toMatch(/outside the server workspace root/);

      const direct = await launchCwdIsConfined(outsideDir);
      expect(direct).toMatchObject({ ok: false });
      if (!direct.ok) expect(direct.message).toMatch(/outside the server workspace root/);
    } finally {
      rmSync(outsideDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function killPid(pid: number): void {
  try {
    process.kill(pid, 0);
    process.kill(pid);
  } catch {
    /* already gone */
  }
}
