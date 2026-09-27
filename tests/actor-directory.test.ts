import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import {
  ActorDirectory,
  type ActorRegisterInput,
  type RuntimeLaunchContract,
} from "../src/core/actor-directory.js";
import { ACTOR_KEYS } from "../src/core/keys.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";


const T0 = "2030-01-01T00:00:00.000Z";

const profileInput = (overrides: Record<string, unknown> = {}): unknown => ({
  actor_id: "actor-a",
  alias: "alice",
  capabilities: ["build"],
  workspace_root: "/workspace",
  working_directory: "/workspace",
  runtime: "pi",
  activation_policy: { mode: "wake_if_offline" },
  max_concurrency: 1,
  ...overrides,
});

const launch = (
  overrides: Partial<RuntimeLaunchContract> = {}
): RuntimeLaunchContract => ({
  command: "/usr/bin/pi",
  args: ["--agent", "alice"],
  ...overrides,
});

const register = (
  overrides: Partial<ActorRegisterInput> & { profile_input?: unknown } = {}
): ActorRegisterInput => ({
  profile_input: profileInput(),
  launch: launch(),
  registered_by: "session-a",
  registered_at: T0,
  ...overrides,
});

const expectOk = <T extends { ok: boolean }>(
  result: T
): Extract<T, { ok: true }> => {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("expected ok result");
  return result;
};

describe("ActorDirectory", () => {
  let redis: Redis;
  let store: ActorDirectory;

  // wake_if_offline admission now requires an operator allowlist. Scaffold one
  // in a temp dir for the file's launches ("/usr/bin/pi") and point
  // GPTQUEUE_LAUNCH_ALLOWLIST at it so the happy-path + ownership tests pass
  // admission.
  let allowlist: ReturnType<typeof scaffoldLaunchAllowlist>;

  beforeEach(async () => {
    allowlist = scaffoldLaunchAllowlist([
      {
        command: "/usr/bin/pi",
        allowed_args: [
          [],
          ["--agent", "alice"],
          ["--agent", "alice-2"],
          ["--agent", "bob"],
        ],
        comment: "test pi launcher",
      },
    ]);
    allowlist.set();
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new ActorDirectory(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
    allowlist.cleanup();
  });

  it("registers an admitted profile on the happy path, deeply frozen", async () => {
    const res = await store.register(register());
    const ok = expectOk(res);
    expect(ok.record.profile.actor_id).toBe("actor-a");
    expect(ok.record.profile.alias).toBe("alice");
    expect(ok.record.launch).toEqual(launch());
    expect(ok.record.registered_by).toBe("session-a");
    expect(ok.record.registered_at).toBe(T0);

    // The directory publishes frozen records: profile, capabilities, and
    // activation_policy are all immutable.
    expect(Object.isFrozen(ok.record)).toBe(true);
    expect(Object.isFrozen(ok.record.profile)).toBe(true);
    expect(Object.isFrozen(ok.record.profile.capabilities)).toBe(true);
    expect(Object.isFrozen(ok.record.profile.activation_policy)).toBe(true);
  });

  it("propagates profile admission errors instead of persisting", async () => {
    const res = await store.register(
      register({ profile_input: profileInput({ max_concurrency: 0 }) })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected invalid_concurrency");
    expect(res.error.code).toBe("invalid_concurrency");
    // Frozen error object.
    expect(Object.isFrozen(res.error)).toBe(true);

    // Nothing persisted.
    const list = await store.list();
    expect(list.ok && list.records).toHaveLength(0);
  });

  it("rejects a wake_if_offline actor that has no launch contract", async () => {
    const res = await store.register(register({ launch: null }));
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected invalid_launch_contract");
    expect(res.error.code).toBe("invalid_launch_contract");
  });

  it("rejects a wake_if_offline actor with an empty command", async () => {
    const res = await store.register(
      register({ launch: launch({ command: "" }) })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected invalid_launch_contract");
    expect(res.error.code).toBe("invalid_launch_contract");
  });

  it("accepts a store_only actor without any launch contract", async () => {
    const res = await store.register(
      register({
        profile_input: profileInput({
          actor_id: "actor-s",
          activation_policy: { mode: "store_only" },
        }),
        launch: null,
      })
    );
    const ok = expectOk(res);
    expect(ok.record.profile.activation_policy.mode).toBe("store_only");
    expect(ok.record.launch).toBeNull();
  });

  it("rejects a cross-session re-register as actor_owned_elsewhere", async () => {
    await store.register(register()); // owned by session-a
    const res = await store.register(
      register({ registered_by: "session-b" })
    );
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected actor_owned_elsewhere");
    expect(res.error.code).toBe("actor_owned_elsewhere");
    // A session id is a bearer credential: the owner must never be echoed.
    expect(res.error.message).not.toContain("session-a");
  });

  it("allows the owning session to update its own profile", async () => {
    await store.register(register());
    const res = await store.register(
      register({
        profile_input: profileInput({ alias: "alice-2" }),
        launch: launch({ args: ["--agent", "alice-2"] }),
        registered_at: "2030-01-01T00:01:00.000Z",
      })
    );
    const ok = expectOk(res);
    expect(ok.record.profile.alias).toBe("alice-2");
    expect(ok.record.registered_at).toBe("2030-01-01T00:01:00.000Z");
  });

  it("get returns the record, or null when absent", async () => {
    const missing = await store.get("nope");
    expect(missing.ok && missing.record).toBeNull();

    await store.register(register());
    const found = await store.get("actor-a");
    expect(found.ok && found.record?.profile.alias).toBe("alice");
  });

  it("list returns every stored record", async () => {
    await store.register(register());
    await store.register(
      register({
        profile_input: profileInput({ actor_id: "actor-b", alias: "bob" }),
        registered_by: "session-b",
        launch: launch({ args: ["--agent", "bob"] }),
      })
    );

    const res = await store.list();
    expect(res.ok && res.records).toHaveLength(2);
    if (!res.ok) throw new Error("expected ok list");
    expect(res.records.map((r) => r.profile.actor_id).sort()).toEqual([
      "actor-a",
      "actor-b",
    ]);
  });

  it("returns store_corrupt for a malformed stored record", async () => {
    await redis.hset(ACTOR_KEYS.profiles, "actor-a", "not-json{{{");
    const res = await store.get("actor-a");
    expect(res.ok).toBe(false);
    if (res.ok) throw new Error("expected store_corrupt");
    expect(res.error.code).toBe("store_corrupt");
    expect(res.error.message).toContain("actor-a");
  });

  it("returns store_corrupt for a stored profile missing required fields", async () => {
    await redis.hset(ACTOR_KEYS.profiles, "actor-a", JSON.stringify({ profile: { actor_id: "actor-a" }, registered_by: "session-a" }));
    const res = await store.get("actor-a");
    if (res.ok) throw new Error("expected store_corrupt");
    expect(res.error.code).toBe("store_corrupt");
  });

  it("reports contractReadiness both ways", async () => {
    const runnable = await store.register(register());
    const ok = expectOk(runnable);
    expect(store.contractReadiness(ok.record)).toBe("runnable");

    const stored = await store.register(
      register({
        profile_input: profileInput({
          actor_id: "actor-s",
          activation_policy: { mode: "store_only" },
        }),
        launch: null,
      })
    );
    const storedOk = expectOk(stored);
    expect(store.contractReadiness(storedOk.record)).toBe("not_runnable");
  });

  it("enforces the conditional-write ownership precondition in the Lua script", async () => {
    const record = {
      profile: profileInput(),
      launch: launch(),
      registered_by: "session-a",
      registered_at: T0,
    };
    await redis.hset(ACTOR_KEYS.profiles, "actor-a", JSON.stringify(record));

    // A foreign owner cannot overwrite via the raw script.
    const lua = `local k=KEYS[1];local f=ARGV[1];local v=ARGV[2];local o=ARGV[3];
      local cur=redis.call('HGET',k,f);
      if cur==false then redis.call('HSET',k,f,v); return 1 end;
      local r=pcall(function() cur=cjson.decode(cur) end);
      if not r or type(cur)~='table' or cur.registered_by~=o then return 0 end;
      redis.call('HSET',k,f,v); return 1`;
    const foreign = await redis.eval(
      lua,
      1,
      ACTOR_KEYS.profiles,
      "actor-a",
      JSON.stringify({ ...record, registered_by: "session-b" }),
      "session-b"
    );
    expect(foreign).toBe(0);
    expect(
      JSON.parse((await redis.hget(ACTOR_KEYS.profiles, "actor-a"))!).registered_by
    ).toBe("session-a");

    // The owning session can update.
    const owner = await redis.eval(
      lua,
      1,
      ACTOR_KEYS.profiles,
      "actor-a",
      JSON.stringify({ ...record, registered_at: "2030-01-01T00:02:00.000Z" }),
      "session-a"
    );
    expect(owner).toBe(1);
    expect(
      JSON.parse((await redis.hget(ACTOR_KEYS.profiles, "actor-a"))!).registered_at
    ).toBe("2030-01-01T00:02:00.000Z");
  });
});