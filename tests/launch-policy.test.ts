import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { writeFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { ActorDirectory } from "../src/core/actor-directory.js";
import {
  commandMatches,
  argsPrefixCompatible,
  parseLaunchAllowlist,
  launchMatchesConfig,
  evaluateLaunchPolicy,
} from "../src/core/launch-policy.js";
import { dispatchLaunch } from "../src/mcp-server/launcher.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const T0 = "2030-01-01T00:00:00.000Z";

// ---------------------------------------------------------------------------
// Pure matching helpers
// ---------------------------------------------------------------------------
describe("launch allowlist matching (pure)", () => {
  it("commandMatches resolves a PATH-style name and an absolute path to the same basename", () => {
    expect(commandMatches("node", "node")).toBe(true);
    expect(commandMatches("/usr/bin/node", "node")).toBe(true);
    expect(commandMatches("node", "/usr/local/bin/node")).toBe(true);
    expect(commandMatches("node", "python")).toBe(false);
  });

  it("argsPrefixCompatible: exact prefix matches, prefix shorter than request is ok, first mismatch rejects", () => {
    expect(argsPrefixCompatible([], [])).toBe(true);
    expect(argsPrefixCompatible(["a"], [])).toBe(true);
    expect(argsPrefixCompatible(["a", "b"], ["a"])).toBe(true);
    expect(argsPrefixCompatible(["a", "b", "c"], ["a", "b"])).toBe(true);
    expect(argsPrefixCompatible(["a", "b"], ["a", "b"])).toBe(true);
    expect(argsPrefixCompatible(["a", "b", "c"], ["a", "b", "c"])).toBe(true);
    // Mismatch at index 1.
    expect(argsPrefixCompatible(["a", "z"], ["a", "b"])).toBe(false);
    // Request shorter than the prefix.
    expect(argsPrefixCompatible(["a"], ["a", "b"])).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Allowlist parsing / rejection of malformed documents
// ---------------------------------------------------------------------------
describe("parseLaunchAllowlist", () => {
  it("loads a valid v1 document", () => {
    const res = parseLaunchAllowlist(
      JSON.stringify({
        version: 1,
        commands: [{ command: "node", allowed_args_prefixes: [["--port", "1234"], []] }],
      }),
      "test"
    );
    expect(res.kind).toBe("loaded");
    if (res.kind !== "loaded") throw new Error("expected loaded");
    expect(res.config.commands[0]?.command).toBe("node");
  });

  it("rejects malformed JSON / wrong version / bad shape as unparseable", () => {
    expect(parseLaunchAllowlist("not json", "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist("[]", "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist(JSON.stringify({ version: 2, commands: [] }), "x").kind).toBe("unparseable");
    expect(parseLaunchAllowlist(JSON.stringify({ version: 1 }), "x").kind).toBe("unparseable");
    expect(
      parseLaunchAllowlist(JSON.stringify({ version: 1, commands: [{ command: "" }] }), "x").kind
    ).toBe("unparseable");
    expect(
      parseLaunchAllowlist(JSON.stringify({ version: 1, commands: [{ command: "x", allowed_args_prefixes: "nope" }] }), "x").kind
    ).toBe("unparseable");
  });

  it("launchMatchesConfig consults every entry", () => {
    const loaded = parseLaunchAllowlist(
      JSON.stringify({
        version: 1,
        commands: [
          { command: "node", allowed_args_prefixes: [["--port", "1234"]] },
          { command: "git", allowed_args_prefixes: [[]] },
        ],
      }),
      "x"
    );
    expect(loaded.kind).toBe("loaded");
    if (loaded.kind !== "loaded") throw new Error("expected loaded");
    const config = loaded.config;
    expect(launchMatchesConfig("node", ["--port", "1234"], config)).toBe(true);
    expect(launchMatchesConfig("/usr/bin/node", ["--port", "1234"], config)).toBe(true);
    expect(launchMatchesConfig("node", ["--foo"], config)).toBe(false); // prefix mismatch
    expect(launchMatchesConfig("git", [], config)).toBe(true);
    expect(launchMatchesConfig("python", [], config)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// evaluateLaunchPolicy (filesystem + env, no Redis, no spawn)
// ---------------------------------------------------------------------------
describe("evaluateLaunchPolicy", () => {
  it("approves an allowlisted command/args and rejects a non-allowlisted one", async () => {
    const s = scaffoldLaunchAllowlist([
      { command: "node", allowed_args_prefixes: [["--port", "1234"]] },
    ]);
    s.set();
    try {
      const ok = await evaluateLaunchPolicy({
        command: "node",
        args: ["--port", "1234", "extra"],
      });
      expect(ok).toEqual({ ok: true });

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
      { command: "/bin/sh", allowed_args_prefixes: [[]] },
    ]);
    s.set();
    try {
      const res = await evaluateLaunchPolicy({ command: "/bin/sh", args: ["-c", "echo x"] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_command_rejected" } });
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

  it("fail-closes when the allowlist file is unparseable", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      writeFileSync(s.path, "{ not json");
      const res = await evaluateLaunchPolicy({ command: "node", args: [] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
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

  it("admits an allowlisted wake_if_offline launch", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      const res = await register({ command: "node", args: ["-e", "process.exit(0)"] });
      expect(res.ok).toBe(true);
    } finally {
      s.cleanup();
    }
  });

  it("rejects a non-allowlisted command as launch_not_allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
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

  it("rejects an args-prefix mismatch as launch_not_allowlisted", async () => {
    const s = scaffoldLaunchAllowlist([{ command: "node", allowed_args_prefixes: [["--port", "1234"]] }]);
    s.set();
    try {
      const res = await register({ command: "node", args: ["--foo"] });
      expect(res).toMatchObject({ ok: false, error: { code: "launch_not_allowlisted" } });
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
  it("dispatches an allowlisted contract", async () => {
    const s = scaffoldLaunchAllowlist([{ command: process.execPath }]);
    s.set();
    let pid: number | undefined;
    try {
      const res = await dispatchLaunch({
        command: process.execPath,
        args: ["-e", "setTimeout(()=>{},5000)"],
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
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      // Admit while node is allowlisted.
      const contract = { command: "node", args: ["-e", "setTimeout(()=>{},1000)"] };
      expect(await evaluateLaunchPolicy(contract)).toEqual({ ok: true });

      // Operator rewrites the allowlist to remove node -> dispatch must refuse.
      writeFileSync(
        s.path,
        JSON.stringify({ version: 1, commands: [{ command: "python", allowed_args_prefixes: [[]] }] })
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
    const s = scaffoldLaunchAllowlist([{ command: "node" }]);
    s.set();
    try {
      rmSync(s.path, { force: true });
      const res = await dispatchLaunch({
        command: "node",
        args: ["-e", "process.exit(0)"],
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