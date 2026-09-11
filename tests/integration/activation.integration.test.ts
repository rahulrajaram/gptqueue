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
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Redis } from "ioredis";
import {
  setupIntegrationServer,
  type IntegrationServer,
} from "../helpers/integration-server.js";
import { WAKE_SLEEPY_SCRIPT, WAKE_EXIT_SCRIPT } from "../helpers/wake-launch.js";
import { connectAgent, type Agent } from "../helpers/mcp-agent.js";
import { WakeLeaseStore } from "../../src/core/wake-lease.js";
import { reconcileWakeLease } from "../../src/mcp-server/tools/reconcile-wake-lease.js";
import { SESSION_KEYS, WAKE_LEASE_KEYS, ACTOR_KEYS } from "../../src/core/keys.js";

// Per-file timeout override (does not touch the global vitest config). Several
// wake/lease tests intentionally wait out short process lifetimes and TTL
// expiry without fighting the default 15s test timeout.
vi.setConfig({ testTimeout: 150_000, hookTimeout: 120_000 });

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Short unique suffix for per-test agent/actor names (no cross-talk). */
const uniq = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`;

/** Launch contract that spawns a real, short-lived node child (20s). */
const sleeperLaunch = () => ({
  command: process.execPath,
  args: [WAKE_SLEEPY_SCRIPT],
});

/** Launch contract pointing at a binary that cannot exist (launch fails). */
const deadLaunch = () => ({
  command: "/nonexistent/definitely-not-a-binary-987654",
  args: [],
});

/** Whether a pid refers to a currently-live process. */
function isAlive(pid: number | undefined): boolean {
  if (pid === undefined || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Kill a pid, tolerating it already being gone. */
function killPid(pid: number | undefined): void {
  if (pid === undefined || pid <= 0) return;
  try {
    process.kill(pid, 0);
    process.kill(pid);
  } catch {
    /* already gone */
  }
}

const T0 = "2030-01-01T00:00:00.000Z";

interface Ctx {
  server: IntegrationServer;
  redis: Redis;
  agents: Agent[];
  /** Every child we dispatched (spawned via wake) that must be killed. */
  spawnedPids: number[];
}

let ctx: Ctx;

beforeAll(async () => {
  const server = await setupIntegrationServer();
  ctx = { server, redis: server.redis, agents: [], spawnedPids: [] };
}, 120_000);

afterAll(async () => {
  for (const a of ctx.agents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  for (const pid of ctx.spawnedPids) killPid(pid);
  ctx.spawnedPids = [];
  await ctx.server.cleanup();
}, 60_000);

beforeEach(async () => {
  ctx.agents = [];
  ctx.spawnedPids = [];
  await ctx.server.flushGptqKeys();
}, 60_000);

afterEach(async () => {
  for (const a of ctx.agents) {
    try {
      await a.close();
    } catch {
      /* best-effort */
    }
  }
  for (const pid of ctx.spawnedPids) killPid(pid);
  ctx.spawnedPids = [];
  await ctx.server.flushGptqKeys();
}, 60_000);

/** Connect a fresh agent on the integration server and track it for cleanup. */
async function connect(name: string, opts?: Parameters<typeof connectAgent>[2]) {
  const agent = await connectAgent(ctx.server.baseUrl, name, opts);
  ctx.agents.push(agent);
  return agent;
}

/**
 * Register a durable actor profile through the owning agent's session.
 *
 * H4 identity discipline: actor_id is DERIVED from the calling session's
 * registered agent name (the durable actor identity IS the registered name),
 * so the `owner` session MUST be registered under the actor identity it wants
 * to register. The tool no longer accepts a caller-supplied actor_id.
 */
async function registerActor(
  owner: Agent,
  mode: "wake_if_offline" | "store_only",
  launch?: { command: string; args: string[] },
  maxConcurrency = 1
) {
  return owner.call("actor_register", {
    session_id: owner.sessionId,
    alias: `${owner.name}-alias`,
    activation_policy_mode: mode,
    max_concurrency: maxConcurrency,
    ...(launch
      ? { launch_command: launch.command, launch_args: launch.args }
      : {}),
  });
}

/**
 * Take a durable actor offline by closing its owning session. The actor
 * directory record, registry entry, and mailbox all persist; only the live
 * session (and its lease) is removed, so presence returns to offline.
 * actor_status is a stateless read, so it remains callable on the closed
 * agent for offline-state assertions.
 */
async function takeOffline(owner: Agent): Promise<void> {
  const res = await owner.call("close_session", { session_id: owner.sessionId });
  expect(res.data.status).toBe("session_closed");
}

/** Track a dispatched pid for cleanup, returning it for convenience. */
const track = (pid: number | undefined) => {
  if (pid !== undefined && pid > 0) ctx.spawnedPids.push(pid);
  return pid;
};

/** Track, dispatch, and return a wake payload from a real wire send. */
async function sendAndTrack(
  sender: Agent,
  to: string,
  content: string
): Promise<{
  status: string;
  message_id: string;
  wake?: {
    status: string;
    lease_id?: string;
    pid?: number;
    error_message?: string;
  };
}> {
  const res = await sender.call("send_message", {
    session_id: sender.sessionId,
    to,
    content,
    type: "task",
  });
  expect(res.data.status).toBe("sent");
  if (res.data.wake?.pid) track(res.data.wake.pid);
  return res.data;
}

// ---------------------------------------------------------------------------
// A. Actor registration & directory
// ---------------------------------------------------------------------------
describe("actor registration & directory (wire)", () => {
  it("happy path: wake_if_offline register exposes a runnable launch contract; actor_status reflects the profile", async () => {
    const actorId = uniq("reg-happy-act");
    // The owning session IS registered under the actor identity (actor_id is
    // derived from the registered name under H4).
    const owner = await connect(actorId);

    const res = await registerActor(owner, "wake_if_offline", sleeperLaunch());
    expect(res.data.status).toBe("ok");
    expect(res.data.record.profile.actor_id).toBe(actorId); // derived, not caller-supplied
    expect(res.data.record.profile.activation_policy.mode).toBe("wake_if_offline");
    expect(res.data.record.launch.command).toBe(process.execPath);

    await takeOffline(owner); // no live session -> offline_launchable
    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.status).toBe("ok");
    expect(status.data.actor_id).toBe(actorId);
    expect(status.data.launch_contract).toBe("runnable");
    // No live session and no wake lease -> offline_launchable.
    expect(status.data.presence).toBe("offline_launchable");
    expect(status.data.wake_lease).toBeNull();
  });

  it("rejects wake_if_offline without launch (invalid_launch_contract); accepts store_only without launch", async () => {
    const noLaunchOwner = await connect(uniq("reg-pol-nolaunch"));
    const noLaunch = await registerActor(noLaunchOwner, "wake_if_offline");
    expect(noLaunch.isError).toBe(true);
    expect(noLaunch.data.status).toBe("error");
    expect(noLaunch.data.error.code).toBe("invalid_launch_contract");

    const storeOwner = await connect(uniq("reg-pol-store"));
    const store = await registerActor(storeOwner, "store_only");
    expect(store.data.status).toBe("ok");
    expect(store.data.record.profile.actor_id).toBe(storeOwner.name); // derived
    expect(store.data.record.profile.activation_policy.mode).toBe("store_only");
    // Accepted and persisted. (Offline with a not_runnable contract classifies
    // as unavailable, which the B-7 test covers; here we only assert admission.)
  });

  it("cross-session re-register of the same actor_id is actor_owned_elsewhere; same-session update is ok", async () => {
    // Both sessions register under the SAME actor identity; the directory
    // ownership is session-scoped, so a second session is foreign to the
    // existing profile regardless of sharing the actor name.
    const actorId = uniq("owned-act");
    const ownerA = await connect(actorId);
    const ownerB = await connect(actorId);

    const first = await registerActor(ownerA, "store_only");
    expect(first.data.status).toBe("ok");
    const firstRecord = first.data.record.registered_by;

    // A foreign (differently-sessioned) registration may not re-register / take
    // over the profile, even when it shares the actor name.
    const foreign = await registerActor(ownerB, "store_only");
    expect(foreign.isError).toBe(true);
    expect(foreign.data.error.code).toBe("actor_owned_elsewhere");

    // The owning session may update it (here: flip policy to wake_if_offline).
    const update = await registerActor(ownerA, "wake_if_offline", sleeperLaunch());
    expect(update.data.status).toBe("ok");
    expect(update.data.record.profile.activation_policy.mode).toBe("wake_if_offline");
    expect(update.data.record.registered_by).toBe(firstRecord);
  });

  it("actor_status of an unregistered actor -> unknown_recipient", async () => {
    const owner = await connect(uniq("unknown-owner"));
    const res = await owner.call("actor_status", { actor_id: uniq("nobody") });
    expect(res.isError).toBe(true);
    expect(res.data.status).toBe("error");
    expect(res.data.error.code).toBe("unknown_recipient");
  });
});

// ---------------------------------------------------------------------------
// B. Presence states
// ---------------------------------------------------------------------------
describe("actor presence states (via actor_status)", () => {
  it("offline_launchable: wake_if_offline + runnable contract + no session + no wake lease", async () => {
    const actorId = uniq("pres-launch-act");
    const owner = await connect(actorId);
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // no live session under the actor identity

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data).toMatchObject({
      status: "ok",
      presence: "offline_launchable",
      launch_contract: "runnable",
      wake_lease: null,
    });
    expect(status.data.runtime).toBeUndefined();
  });

  it("offline_store_only: store_only + runnable contact + no session", async () => {
    const actorId = uniq("pres-store-act");
    const owner = await connect(actorId);
    // store_only with a launch contract that happens to be runnable still
    // classifies as offline_store_only while offline.
    await registerActor(owner, "store_only", sleeperLaunch());
    await takeOffline(owner);

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data).toMatchObject({
      status: "ok",
      presence: "offline_store_only",
      launch_contract: "runnable",
      wake_lease: null,
    });
  });

  it("unavailable: wake_if_offline actor whose launch contract is not runnable (launch:null via direct db15)", async () => {
    const actorId = uniq("pres-unavail-act");
    const owner = await connect(actorId);
    // Register a valid wake_if_offline actor over the wire first (the tool
    // layer rejects a null launch for wake_if_offline), then set launch:null
    // directly on db15 so contractReadiness flips to not_runnable. This is the
    // explicitly-authorized direct-manipulation path for an input the schema
    // will not admit through the tool.
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // actor must be offline to classify unavailable
    // Overwrite the stored record with `launch: null` directly on db15.
    await ctx.redis.hset(
      ACTOR_KEYS.profiles,
      actorId,
      JSON.stringify({ ...actorDirRecord(actorId, owner.sessionId), launch: null })
    );

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.status).toBe("ok");
    expect(status.data.presence).toBe("unavailable");
    expect(status.data.launch_contract).toBe("not_runnable");
  });

  it("starting: outstanding wake lease with no session (acquired direct on db15)", async () => {
    const actorId = uniq("pres-start-act");
    const owner = await connect(actorId);
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // no live session -> only the lease drives presence

    const store = new WakeLeaseStore(ctx.redis);
    const acquired = await store.acquire({
      actor_id: actorId,
      issued_by_session: owner.sessionId,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.presence).toBe("starting");
    // No spawn evidence yet -> retained as starting with pid_liveness unknown.
    expect(status.data.wake_lease).toMatchObject({
      lease_id: acquired.lease.lease_id,
      pid_liveness: "unknown",
    });
  });

  it("idle: live session under the actor name + no outstanding claim", async () => {
    const actorId = uniq("pres-idle-act");
    const owner = await connect(actorId); // living session IS the runtime
    await registerActor(owner, "store_only");

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.presence).toBe("idle");
    expect(status.data.runtime).toMatchObject({
      session_id: owner.sessionId,
      workload: "idle",
    });
  });

  it("active: live session + outstanding unacked claim -> processing", async () => {
    const actorId = uniq("pres-active-act");
    const owner = await connect(actorId); // owner IS the actor's runtime session
    const sender = await connect(uniq("pres-active-src"));
    await registerActor(owner, "store_only");
    const runtime = owner;

    await sender.call("send_message", {
      session_id: sender.sessionId,
      to: actorId,
      content: "work to process",
    });
    const claim = await runtime.call("claim_tasks", {
      session_id: runtime.sessionId,
      max_batch: 1,
      ttl_seconds: 300,
    });
    expect(claim.data.status).toBe("ok");
    expect(claim.data.claimed).toBe(true);
    const claimId = (claim.data.claim as { claim_id: string }).claim_id;

    const during = await owner.call("actor_status", { actor_id: actorId });
    expect(during.data.presence).toBe("active");
    expect(during.data.runtime).toMatchObject({
      session_id: runtime.sessionId,
      workload: "processing",
    });

    await runtime.call("acknowledge_tasks", {
      session_id: runtime.sessionId,
      claim_id: claimId,
    });
    const after = await owner.call("actor_status", { actor_id: actorId });
    expect(after.data.presence).toBe("idle");
    expect(after.data.runtime.workload).toBe("idle");
  });

  it("precedence: live session + outstanding wake lease -> leased runtime wins (idle, not starting)", async () => {
    const actorId = uniq("pres-prec-act");
    const owner = await connect(actorId); // owner IS the live runtime session
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    const runtime = owner;

    // A wake lease exists (in-flight activation), but the live leased runtime
    // outranks it in classification.
    const store = new WakeLeaseStore(ctx.redis);
    const acquired = await store.acquire({
      actor_id: actorId,
      issued_by_session: owner.sessionId,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");

    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.presence).toBe("idle"); // NOT starting
    expect(status.data.wake_lease).not.toBeNull(); // still observable, just not driving
    expect(status.data.runtime.session_id).toBe(runtime.sessionId);
  });
});

// ---------------------------------------------------------------------------
// C. Wake-on-send lifecycle (real spawns)
// ---------------------------------------------------------------------------
describe("wake-on-send lifecycle (real spawns)", () => {
  it("offline wake_if_offline: other agent send dispatches a live pid, leases on db15, actor starting", async () => {
    const actorId = uniq("wake-disp-act");
    const owner = await connect(actorId);
    const sender = await connect(uniq("wake-disp-src"));
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // actor offline so wake dispatches

    const sent = await sendAndTrack(sender, actorId, "wake the actor");
    expect(sent.wake?.status).toBe("wake_dispatched");
    expect(sent.wake?.lease_id).toBeTruthy();
    // A real child process was spawned and is currently alive.
    expect(isAlive(sent.wake?.pid)).toBe(true);

    // The wake lease exists in db15 with a live spawned pid.
    const lease = await ctx.redis.get(WAKE_LEASE_KEYS.lease(actorId));
    const leaseObj = lease ? JSON.parse(lease) : null;
    expect(leaseObj).not.toBeNull();
    expect(leaseObj.lease_id).toBe(sent.wake!.lease_id);
    expect(leaseObj.spawned_pid).toBe(sent.wake!.pid);

    // Presence is now starting (activation in flight, pid alive).
    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.presence).toBe("starting");
    expect(status.data.wake_lease).toMatchObject({
      lease_id: sent.wake!.lease_id,
      pid_liveness: "alive",
    });
  });

  it("runtime_ready clears the wake lease; claimable -> active -> ack -> idle", async () => {
    const actorId = uniq("wake-ready-act");
    const owner = await connect(actorId);
    const sender = await connect(uniq("wake-ready-src"));
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // actor offline so wake dispatches

    const sent = await sendAndTrack(sender, actorId, "wake then claim");
    expect(sent.wake?.status).toBe("wake_dispatched");
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(1);

    // The runtime comes up under the actor name -> runtime_ready clears lease.
    const runtime = await connect(actorId);
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(0);
    const idleNow = await owner.call("actor_status", { actor_id: actorId });
    expect(idleNow.data.presence).toBe("idle");

    // The persisted message is claimable by the runtime -> active -> ack -> idle.
    const claim = await runtime.call("claim_tasks", {
      session_id: runtime.sessionId,
      max_batch: 1,
      ttl_seconds: 300,
    });
    expect(claim.data.claimed).toBe(true);
    expect((claim.data.claim as { tasks: string[] }).tasks).toHaveLength(1);

    const active = await owner.call("actor_status", { actor_id: actorId });
    expect(active.data.presence).toBe("active");

    await runtime.call("acknowledge_tasks", {
      session_id: runtime.sessionId,
      claim_id: (claim.data.claim as { claim_id: string }).claim_id,
    });
    const idle = await owner.call("actor_status", { actor_id: actorId });
    expect(idle.data.presence).toBe("idle");
    expect(await ctx.redis.llen(SESSION_KEYS.queue(actorId))).toBe(0);
  });

  it("coalescing: second send while first lease outstanding -> wake_coalesced same lease, exactly one child", async () => {
    const actorId = uniq("wake-coal-act");
    const owner = await connect(actorId);
    const s1 = await connect(uniq("wake-coal-s1"));
    const s2 = await connect(uniq("wake-coal-s2"));
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner); // actor offline so the first send wakes

    const first = await sendAndTrack(s1, actorId, "first wake");
    expect(first.wake?.status).toBe("wake_dispatched");
    expect(isAlive(first.wake?.pid)).toBe(true);
    const firstLease = first.wake!.lease_id!;

    // Actor has not registered, so the lease is still outstanding -> coalesce.
    const second = await sendAndTrack(s2, actorId, "second wake");
    expect(second.wake?.status).toBe("wake_coalesced");
    expect(second.wake?.lease_id).toBe(firstLease);
    // No second child was spawned, and the first child is still the live one.
    expect(second.wake?.pid).toBeUndefined();
    expect(isAlive(first.wake?.pid)).toBe(true);

    // Only ONE lease ever existed (dedup collapsed to it).
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(1);
  });

  it("launch_failed: send succeeds, TTL expires back to offline, a fresh send re-dispatches a new lease", async () => {
    const actorId = uniq("wake-fail-act");
    const owner = await connect(actorId);
    const sender = await connect(uniq("wake-fail-src"));
    // Register against a binary that cannot exist -> dispatch fails.
    await registerActor(owner, "wake_if_offline", deadLaunch());
    await takeOffline(owner);

    const sent = await sendAndTrack(sender, actorId, "will not launch");
    expect(sent.wake?.status).toBe("launch_failed");
    expect(sent.wake?.lease_id).toBeTruthy();
    expect(sent.wake?.error_message).toBeTruthy();
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(1);

    // Deterministically expire the wire-issued lease (its normal 60s TTL is
    // too long for a test): shrink TTL to ~1s, wait, then presence is offline.
    await ctx.redis.pexpire(WAKE_LEASE_KEYS.lease(actorId), 1000);
    await delay(1500);
    const offline = await owner.call("actor_status", { actor_id: actorId });
    expect(offline.data.presence).toBe("offline_launchable");
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(0);

    // The owning session is offline (closed to permit wake), so it cannot issue
    // a further actor_register repair. Apply the runnable launch directly on
    // db15 (the explicitly-authorized manipulation seam, as in the unavailable
    // test) so the actor returns to runnable/offline, then a fresh send
    // re-dispatches with a NEW lease id.
    await ctx.redis.hset(
      ACTOR_KEYS.profiles,
      actorId,
      JSON.stringify(actorDirRecord(actorId, owner.sessionId))
    );
    const again = await sendAndTrack(sender, actorId, "now it launches");
    expect(again.wake?.status).toBe("wake_dispatched");
    expect(again.wake?.lease_id).not.toBe(sent.wake!.lease_id);
    expect(isAlive(again.wake?.pid)).toBe(true);
  });

  it("dead-pid reconciliation: lease cleared, presence offline (not starting), fresh send re-dispatches", async () => {
    const actorId = uniq("wake-dead-act");
    const owner = await connect(actorId);
    const sender = await connect(uniq("wake-dead-src"));
    await registerActor(owner, "wake_if_offline", sleeperLaunch());
    await takeOffline(owner);

    // Acquire a wake lease pinned to a spawned short-lived process that exits.
    const dead = spawn(process.execPath, ["-e", "process.exit(0)"]);
    const deadPid = dead.pid!;
    await new Promise<void>((resolve) => dead.once("exit", () => resolve()));

    const store = new WakeLeaseStore(ctx.redis);
    const acquired = await store.acquire({
      actor_id: actorId,
      issued_by_session: owner.sessionId,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: acquired.lease.lease_id,
      pid: deadPid,
      spawned_at: T0,
    });

    // Direct observation that reconciliation saw `dead` and cleared the lease
    // (actor_status surfaces pid_liveness only when a lease is retained, so we
    // also probe the bounded reconciliation helper on the live store).
    const observe = await reconcileWakeLease({ wakeLease: store }, actorId);
    expect(observe.pid_liveness).toBe("dead");
    expect(observe.cleared).toBe(true);

    // Over the wire the lease is now gone -> presence offline (NOT starting).
    const status = await owner.call("actor_status", { actor_id: actorId });
    expect(status.data.presence).toBe("offline_launchable");
    expect(status.data.wake_lease).toBeNull();
    expect(await store.get(actorId)).toBeNull();

    // A fresh send re-dispatches a NEW lease.
    const fresh = await sendAndTrack(sender, actorId, "re-dispatch me");
    expect(fresh.wake?.status).toBe("wake_dispatched");
    expect(fresh.wake?.lease_id).not.toBe(acquired.lease.lease_id);
    expect(isAlive(fresh.wake?.pid)).toBe(true);
  });

  it("store_only actor: no wake payload, no wake lease key; message waits in inbox", async () => {
    const actorId = uniq("wake-store-act");
    const owner = await connect(actorId);
    const sender = await connect(uniq("wake-store-src"));
    await registerActor(owner, "store_only");
    await takeOffline(owner);

    const sent = await sendAndTrack(sender, actorId, "just store it");
    expect(sent.wake).toBeUndefined();
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(actorId))).toBe(0);
    expect(await ctx.redis.llen(SESSION_KEYS.queue(actorId))).toBe(1);
  });

  it("plain (non-actor) agent pair exchanges via receive_message with no wake payload anywhere", async () => {
    const a = await connect(uniq("plain-a"));
    const b = await connect(uniq("plain-b"));

    const sent = await a.call("send_message", {
      session_id: a.sessionId,
      to: b.name,
      content: "plain hello",
    });
    expect(sent.data.status).toBe("sent");
    expect(sent.data.wake).toBeUndefined();

    const recv = await b.call("receive_message", { session_id: b.sessionId, timeout: 3 });
    expect(recv.data.payload.content).toBe("plain hello");
    expect(recv.data.wake).toBeUndefined();
    expect(await ctx.redis.exists(WAKE_LEASE_KEYS.lease(b.name))).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// D. Custody x actor interplay
// ---------------------------------------------------------------------------
describe("worktree custody vs actor presence (orthogonal planes)", () => {
  it("a durable actor session can hold custody of a worktree without affecting its presence", async () => {
    const actorId = uniq("custx-act");
    const owner = await connect(actorId); // the actor's session claims + holds the worktree
    const ws = `/worktrees/ws-${uniq("one")}`;
    await registerActor(owner, "store_only");
    const runtime = owner;

    const claim = await runtime.call("custody_claim", {
      session_id: runtime.sessionId,
      worktree_path: ws,
      repo_head: "a".repeat(40),
      tree_fingerprint: "fp-own",
      lease_seconds: 300,
    });
    expect(claim.data.status).toBe("ok");
    expect(claim.data.record.state).toBe("held");

    // Presence is driven by the activation plane (live session, no workload) —
    // NOT by custody. Both planes are visible simultaneously.
    const presence = await owner.call("actor_status", { actor_id: actorId });
    expect(presence.data.presence).toBe("idle");
    const custody = await runtime.call("custody_status", { worktree_path: ws });
    expect(custody.data.status).toBe("ok");
    expect(custody.data.record.state).toBe("held");
    expect(custody.data.record.custodian.session_id).toBe(runtime.sessionId);
  });
});

// ---------------------------------------------------------------------------
// E. PTY / hook prompt regression (static, cheap)
// ---------------------------------------------------------------------------
describe("PTY & shell-hook prompt injection regression", () => {
  it("built PTY wrapper instructs claim_tasks/acknowledge_tasks, never 'Call the receive_message tool'", () => {
    const source = readFileSync(
      new URL("../../src/pty-wrapper/index.ts", import.meta.url),
      "utf-8"
    );
    expect(source).toMatch(/claim_tasks/);
    expect(source).toMatch(/acknowledge_tasks/);
    expect(source).not.toMatch(/Call the receive_message tool/);
  });

  it("scripts/check-queue.sh instructs claim_tasks/acknowledge_tasks, never 'Call the receive_message tool'", () => {
    const source = readFileSync(
      new URL("../../scripts/check-queue.sh", import.meta.url),
      "utf-8"
    );
    expect(source).toMatch(/claim_tasks/);
    expect(source).toMatch(/acknowledge_tasks/);
    expect(source).not.toMatch(/Call the receive_message tool/);
  });
});

// ---------------------------------------------------------------------------
// F. Launch allowlist & identity policy (wire)
// ---------------------------------------------------------------------------
describe("launch allowlist & identity policy (wire)", () => {
  it("rejects a wake_if_offline actor whose command is not allowlisted (launch_not_allowlisted)", async () => {
    const owner = await connect(uniq("pol-notallow-owner"));
    const res = await owner.call("actor_register", {
      session_id: owner.sessionId,
      alias: "notallowed",
      activation_policy_mode: "wake_if_offline",
      max_concurrency: 1,
      launch_command: "/usr/bin/env",
      launch_args: [],
    });
    expect(res.isError).toBe(true);
    expect(res.data.error.code).toBe("launch_not_allowlisted");
    expect(res.data.error.message).toMatch(/allowlist/);
  });

  it("rejects a shell delegator even though it is a delegator (launch_command_rejected)", async () => {
    const owner = await connect(uniq("pol-shell-owner"));
    const res = await owner.call("actor_register", {
      session_id: owner.sessionId,
      alias: "shellac",
      activation_policy_mode: "wake_if_offline",
      max_concurrency: 1,
      launch_command: "/bin/sh",
      launch_args: ["-c", "echo pwned"],
    });
    expect(res.isError).toBe(true);
    expect(res.data.error.code).toBe("launch_command_rejected");
  });

  it("confines launch_cwd to the server workspace (outside rejected, inside ok)", async () => {
    const owner = await connect(uniq("pol-cwd-owner"));
    const outsideRes = await owner.call("actor_register", {
      session_id: owner.sessionId,
      alias: "cwdout",
      activation_policy_mode: "wake_if_offline",
      max_concurrency: 1,
      launch_command: process.execPath,
      launch_args: [WAKE_EXIT_SCRIPT],
      launch_cwd: join(tmpdir(), "gptqueue-outside-ws"),
    });
    expect(outsideRes.isError).toBe(true);
    expect(outsideRes.data.error.code).toBe("launch_cwd_confined");

    // An existing directory inside the workspace is accepted (and, being an
    // allowlisted process.execPath command, registers cleanly).
    const insideRes = await owner.call("actor_register", {
      session_id: owner.sessionId,
      alias: "cwdinside",
      activation_policy_mode: "wake_if_offline",
      max_concurrency: 1,
      launch_command: process.execPath,
      launch_args: [WAKE_EXIT_SCRIPT],
      launch_cwd: process.cwd(),
    });
    expect(insideRes.data.status).toBe("ok");
  });

  it("rejects a derived actor_id outside the identity charset over the wire (invalid_identity_charset)", async () => {
    // actor_id is now DERIVED from the registered name (H4); register a name
    // containing a space so the derived identity violates the charset.
    const owner = await connect(uniq("pol-ident-act") + " ");
    const res = await owner.call("actor_register", {
      session_id: owner.sessionId,
      alias: "ok-alias",
      activation_policy_mode: "store_only",
      max_concurrency: 1,
    });
    expect(res.isError).toBe(true);
    expect(res.data.error.code).toBe("invalid_identity_charset");
  });
});

/**
 * Reconstruct a minimal actor-directory record for a registered actor so a
 * test may craft a legacy/inconsistent variant (e.g. `launch: null` for a
 * wake_if_offline actor) directly on db15. Kept local to this file.
 */
function actorDirRecord(actorId: string, registeredBy: string) {
  return {
    profile: {
      actor_id: actorId,
      alias: `${actorId}-alias`,
      capabilities: [],
      workspace_root: process.cwd(),
      working_directory: process.cwd(),
      runtime: "manual",
      activation_policy: { mode: "wake_if_offline" },
      max_concurrency: 1,
    },
    launch: { command: process.execPath, args: [WAKE_SLEEPY_SCRIPT] },
    registered_by: registeredBy,
    registered_at: T0,
  };
}