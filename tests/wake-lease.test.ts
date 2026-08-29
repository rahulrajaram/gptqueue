import { describe, it, expect, beforeEach, afterEach, afterAll } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import {
  WakeLeaseStore,
  type WakeLeaseAcquireInput,
} from "../src/core/wake-lease.js";
import { ACTOR_KEYS } from "../src/core/keys.js";
import type { RuntimeLaunchContract } from "../src/core/actor-directory.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";
import {
  actorStatus,
  actorStatusSchema,
} from "../src/mcp-server/tools/actor-status.js";
import {
  registerAgent,
  registerAgentSchema,
} from "../src/mcp-server/tools/register-agent.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

// wake_if_offline admission now requires an operator allowlist. Scaffold one
// permitting the test launchers ("/usr/bin/pi") and point GPTQUEUE_LAUNCH_ALLOWLIST
// at it so the presence matrix can register durable actors.
const allowlist = scaffoldLaunchAllowlist([
  {
    command: "/usr/bin/pi",
    allowed_args_prefixes: [[], ["--agent", "matrix"]],
    comment: "test pi launcher",
  },
]);
allowlist.set();
afterAll(() => allowlist.cleanup());


const T0 = "2030-01-01T00:00:00.000Z";
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const acquire = (
  overrides: Partial<WakeLeaseAcquireInput> = {}
): WakeLeaseAcquireInput => ({
  actor_id: "actor-lease",
  issued_by_session: "session-waker",
  lease_seconds: 300,
  now: T0,
  ...overrides,
});

describe("WakeLeaseStore", () => {
  let redis: Redis;
  let store: WakeLeaseStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new WakeLeaseStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("acquires a fresh lease with coalesced=false", async () => {
    const res = await store.acquire(acquire());
    expect(res.ok).toBe(true);
    if (!res.ok) throw new Error("expected ok acquire");
    expect(res.coalesced).toBe(false);
    expect(res.lease.lease_id).toBeTruthy();
    expect(res.lease.actor_id).toBe("actor-lease");
    expect(res.lease.issued_by_session).toBe("session-waker");
    expect(res.lease.issued_at).toBe(T0);
    expect(res.lease.expires_at).toBe("2030-01-01T00:05:00.000Z"); // +300s
  });

  it("coalesces a second acquire onto the same lease", async () => {
    const first = await store.acquire(acquire());
    if (!first.ok) throw new Error("expected ok first");
    const second = await store.acquire(acquire());
    expect(second.ok).toBe(true);
    if (!second.ok) throw new Error("expected ok second");
    expect(second.coalesced).toBe(true);
    expect(second.lease.lease_id).toBe(first.lease.lease_id);
  });

  it("get returns the outstanding lease, or null when none", async () => {
    expect(await store.get("actor-lease")).toBeNull();

    const res = await store.acquire(acquire());
    if (!res.ok) throw new Error("expected ok acquire");
    const lease = await store.get("actor-lease");
    expect(lease?.lease_id).toBe(res.lease.lease_id);
  });

  it("rejects an out-of-bounds lease duration", async () => {
    for (const lease_seconds of [0, 3601, 1.5]) {
      const res = await store.acquire(acquire({ lease_seconds }));
      expect(res.ok).toBe(false);
      if (res.ok) throw new Error("expected invalid_lease_duration");
      expect(res.error.code).toBe("invalid_lease_duration");
    }
  });

  it("clears a lease when the lease_id matches", async () => {
    const res = await store.acquire(acquire());
    if (!res.ok) throw new Error("expected ok acquire");

    const cleared = await store.clear({
      actor_id: "actor-lease",
      lease_id: res.lease.lease_id,
    });
    expect(cleared.ok).toBe(true);
    expect(cleared.cleared).toBe(true);
    expect(await store.get("actor-lease")).toBeNull();
  });

  it("does not clear on a lease_id mismatch", async () => {
    const res = await store.acquire(acquire());
    if (!res.ok) throw new Error("expected ok acquire");

    const cleared = await store.clear({
      actor_id: "actor-lease",
      lease_id: "some-other-lease",
    });
    expect(cleared.cleared).toBe(false);
    // Lease is untouched.
    expect((await store.get("actor-lease"))?.lease_id).toBe(res.lease.lease_id);
  });

  it("reports cleared:false after the lease TTL expires", async () => {
    await store.acquire(acquire({ lease_seconds: 1 }));
    await delay(1100); // let the EX 1 TTL lapse

    expect(await store.get("actor-lease")).toBeNull();
    const cleared = await store.clear({
      actor_id: "actor-lease",
      lease_id: "whatever",
    });
    expect(cleared.cleared).toBe(false);
  });

  it("register_agent clears an outstanding wake lease as the runtime_ready side effect", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    try {
      const leaseRes = await client.wakeLease.acquire(
        acquire({ actor_id: "wake-register" })
      );
      if (!leaseRes.ok) throw new Error("expected ok lease acquire");
      expect((await client.wakeLease.get("wake-register"))?.lease_id).toBe(
        leaseRes.lease.lease_id
      );

      // Registering the runtime under the same durable actor id fires the
      // runtime_ready event (in the register_agent TOOL) and clears the
      // outstanding lease.
      await registerAgent(
        client,
        registerAgentSchema.parse({
          name: "wake-register",
          role: "both",
          description: "a woken actor",
        })
      );
      expect(await client.wakeLease.get("wake-register")).toBeNull();
    } finally {
      await client.shutdown();
    }
  });
});

const profileOf = (overrides: Record<string, unknown> = {}) => ({
  actor_id: "matrix-act",
  alias: "matrix",
  capabilities: [],
  workspace_root: "/workspace",
  working_directory: "/workspace",
  state_directory: "/state/matrix-act",
  runtime: "pi",
  activation_policy: { mode: "wake_if_offline" },
  max_concurrency: 1,
  ...overrides,
});

const launchOf = (
  overrides: Partial<RuntimeLaunchContract> = {}
): RuntimeLaunchContract => ({
  command: "/usr/bin/pi",
  args: ["--agent", "matrix"],
  ...overrides,
});

const registerStoreProfile = async (
  client: RedisClient,
  profile: Record<string, unknown>,
  launch: RuntimeLaunchContract | null
) => {
  await client.actorDirectory.register({
    profile_input: profile,
    launch,
    registered_by: "session-matrix",
    registered_at: T0,
  });
};

describe("actor_status presence matrix", () => {
  let redis: Redis;
  let client: RedisClient;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    client = new RedisClient(null, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await client.shutdown();
  });

  it("unknown actor -> unknown_recipient", async () => {
    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "unknown-actor" })
    );
    expect(res.isError).toBe(true);
    expect(res.structuredContent).toMatchObject({
      status: "error",
      error: { code: "unknown_recipient" },
    });
  });

  it("runnable wake_if_offline, offline -> offline_launchable", async () => {
    await registerStoreProfile(client, profileOf(), launchOf());
    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-act" })
    );
    expect(res.isError).toBeUndefined();
    expect(res.structuredContent).toMatchObject({ presence: "offline_launchable" });
    expect(res.structuredContent).toMatchObject({ launch_contract: "runnable" });
    expect((res.structuredContent as { wake_lease: unknown }).wake_lease).toBeNull();
  });

  it("store_only -> offline_store_only", async () => {
    // A store_only actor still needs a runnable launch contract to classify as
    // offline_store_only; without one it is not_runnable and thus unavailable.
    await registerStoreProfile(
      client,
      profileOf({
        actor_id: "matrix-store",
        activation_policy: { mode: "store_only" },
      }),
      launchOf()
    );
    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-store" })
    );
    expect(res.structuredContent).toMatchObject({ presence: "offline_store_only" });
  });

  it("wake_if_offline with no launch contract -> unavailable", async () => {
    // A wake_if_offline record with a null launch is normally rejected at
    // registration, but a legacy/inconsistent stored record resolves to
    // not_runnable and therefore unavailable. Persist it directly.
    await redis.hset(
      ACTOR_KEYS.profiles,
      "matrix-nolaunch",
      JSON.stringify({
        profile: profileOf({ actor_id: "matrix-nolaunch" }),
        launch: null,
        registered_by: "session-matrix",
        registered_at: T0,
      })
    );
    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-nolaunch" })
    );
    expect(res.structuredContent).toMatchObject({ presence: "unavailable" });
    expect(res.structuredContent).toMatchObject({ launch_contract: "not_runnable" });
  });

  it("live session -> runtime-attached idle (model maps leased idle workload)", async () => {
    await registerStoreProfile(client, profileOf(), launchOf());
    await client.sessions.createSession("matrix-act", "both", "live session");

    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-act" })
    );
    expect(res.isError).toBeUndefined();
    // The pure model classifies a leased runtime with workload "idle" as
    // "idle", not "active" ("active" requires workload "processing"). This
    // slice has no workload signal, so a live session reports idle.
    expect(res.structuredContent).toMatchObject({ presence: "idle" });
    expect(
      (res.structuredContent as { runtime: { lease_id: string } }).runtime.lease_id
    ).toBeTruthy();
  });

  it("outstanding wake lease -> starting", async () => {
    await registerStoreProfile(client, profileOf(), launchOf());
    const leaseRes = await client.wakeLease.acquire(
      acquire({ actor_id: "matrix-act" })
    );
    if (!leaseRes.ok) throw new Error("expected ok acquire");

    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-act" })
    );
    expect(res.structuredContent).toMatchObject({ presence: "starting" });
    expect(res.structuredContent).toMatchObject({
      wake_lease: { lease_id: leaseRes.lease.lease_id },
    });
  });

  it("a live session takes precedence over an outstanding wake lease", async () => {
    await registerStoreProfile(client, profileOf(), launchOf());
    await client.sessions.createSession("matrix-act", "both", "live session");
    await client.wakeLease.acquire(acquire({ actor_id: "matrix-act" }));

    const res = await actorStatus(
      client,
      actorStatusSchema.parse({ actor_id: "matrix-act" })
    );
    // Runtime (leased session) wins over the outstanding wake lease -> idle.
    expect(res.structuredContent).toMatchObject({ presence: "idle" });
  });
});