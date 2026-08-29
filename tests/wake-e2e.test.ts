import { describe, it, expect, beforeEach, afterEach, afterAll } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { spawn } from "child_process";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import {
  sendMessage,
  sendMessageSchema,
} from "../src/mcp-server/tools/send-message.js";
import {
  registerAgent,
  registerAgentSchema,
} from "../src/mcp-server/tools/register-agent.js";
import type { RuntimeLaunchContract } from "../src/core/actor-directory.js";
import { WAKE_LEASE_KEYS, ACTOR_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import {
  claimTasks,
  claimTasksSchema,
} from "../src/mcp-server/tools/claim-tasks.js";
import {
  acknowledgeTasks,
  acknowledgeTasksSchema,
} from "../src/mcp-server/tools/acknowledge-tasks.js";
import {
  actorStatus,
  actorStatusSchema,
} from "../src/mcp-server/tools/actor-status.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";

// wake_if_offline admission + dispatch now require an operator launch
// allowlist. Scaffold one (temp dir + GPTQUEUE_LAUNCH_ALLOWLIST) permitting
// process.execPath (empty prefix AND the "-e <script>" arg pattern) and the
// test dead-binary basename used to exercise launch failure, so the existing
// suites pass the new policy gate. Top-level hook keeps it scoped to this
// file's worker.
const allowlist = scaffoldLaunchAllowlist([
  {
    command: process.execPath,
    allowed_args_prefixes: [[], ["-e"]],
    comment: "test process.execPath launcher",
  },
  {
    command: "/nonexistent/definitely-not-a-binary-12345",
    allowed_args_prefixes: [[]],
    comment: "test dead binary (admission passes, dispatch ENOENT)",
  },
]);
allowlist.set();
afterAll(() => allowlist.cleanup());

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";


const T0 = "2030-01-01T00:00:00.000Z";
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Harmless, short-lived spawned child (sleeps then exits). */
const sleepyLaunch = (): RuntimeLaunchContract => ({
  command: process.execPath,
  args: ["-e", "setTimeout(() => process.exit(0), 15000)"],
});

/** A launch contract pointing at a binary that cannot exist. */
const deadLaunch = (): RuntimeLaunchContract => ({
  command: "/nonexistent/definitely-not-a-binary-12345",
  args: [],
});

interface SendResult {
  status: string;
  wake?: {
    status: string;
    lease_id?: string;
    pid?: number;
    error_message?: string;
  };
}

function parseText(result: {
  content: readonly { type: "text"; text: string }[];
}): SendResult {
  return JSON.parse(result.content[0]!.text);
}

/** Assert a pid is a live process; returns true when it is. */
function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Kill a spawned pid, tolerating it already being gone. */
function killPid(pid: number | undefined): void {
  if (pid === undefined) return;
  try {
    process.kill(pid, 0);
    process.kill(pid);
  } catch {
    // already gone
  }
}

/** Poll until a launched pid is no longer alive (a failed activation). */
async function waitForDead(pid: number, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (isAlive(pid)) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("expected launched process to exit");
    }
    await delay(25);
  }
}

/** Register a durable actor directory record. */
async function registerActor(
  client: RedisClient,
  actorId: string,
  mode: "wake_if_offline" | "store_only",
  launch: RuntimeLaunchContract | null,
  registeredBy: string
): Promise<void> {
  const result = await client.actorDirectory.register({
    profile_input: {
      actor_id: actorId,
      alias: actorId,
      capabilities: [],
      workspace_root: "/workspace",
      working_directory: "/workspace",
      runtime: "node",
      activation_policy: { mode },
      max_concurrency: 1,
    },
    launch,
    registered_by: registeredBy,
    registered_at: T0,
  });
  expect(result.ok).toBe(true);
}

describe("wake-on-send e2e", () => {
  let redis: Redis;
  let sender: RedisClient;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    sender = new RedisClient(null, TEST_REDIS_URL);
    const reg = await sender.register("publisher", "wake-sender", "sends");
    expect(reg.session_id).toBeTruthy();
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await sender.shutdown();
    await redis.quit();
  });

  it("end-to-end: wake_if_offline durable actor is dispatched and the lease clears on runtime_ready", async () => {
    const reg = await sender.register("publisher", "wake-sender-2", "sends");
    const registeredBy = reg.session_id;
    await registerActor(sender, "wakee", "wake_if_offline", sleepyLaunch(), registeredBy);

    // No session exists for "wakee" -> offline_launchable -> dispatch.
    const res = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "wakee", content: "wake me", type: "task" })
      )
    );
    expect(res.status).toBe("sent");
    expect(res.wake?.status).toBe("wake_dispatched");
    expect(res.wake?.lease_id).toBeTruthy();
    const pid = res.wake?.pid;
    expect(pid).toBeTypeOf("number");
    expect(isAlive(pid!)).toBe(true);

    // The issued wake lease exists in Redis with the matching lease id.
    const lease = await sender.wakeLease.get("wakee");
    expect(lease?.lease_id).toBe(res.wake!.lease_id);
    expect(lease?.actor_id).toBe("wakee");
    expect(lease?.issued_by_session).toBe(registeredBy);

    try {
      // Now the runtime comes up under the actor's name -> runtime_ready
      // clears the outstanding wake lease.
      const woken = new RedisClient(null, TEST_REDIS_URL);
      try {
        await registerAgent(
          woken,
          registerAgentSchema.parse({
            name: "wakee",
            role: "both",
            description: "woken runtime",
          })
        );
        expect(await sender.wakeLease.get("wakee")).toBeNull();
      } finally {
        await woken.shutdown();
      }
    } finally {
      killPid(pid);
    }
  });

  it("coalesces concurrent wakes onto one lease and spawns exactly one child", async () => {
    const reg = await sender.register("publisher", "wake-sender-3", "sends");
    await registerActor(
      sender,
      "coalesce",
      "wake_if_offline",
      sleepyLaunch(),
      reg.session_id
    );

    const first = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "coalesce", content: "wake 1", type: "task" })
      )
    );
    expect(first.wake?.status).toBe("wake_dispatched");
    const firstPid = first.wake?.pid;
    const leaseId = first.wake!.lease_id;

    // Runtime still absent; the second send must coalesce (NOT spawn again).
    const second = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "coalesce", content: "wake 2", type: "task" })
      )
    );
    expect(second.wake?.status).toBe("wake_coalesced");
    expect(second.wake?.lease_id).toBe(leaseId);
    // Exactly one child was spawned: only the first result carries a pid.
    expect(second.wake?.pid).toBeUndefined();
    expect(isAlive(firstPid!)).toBe(true);

    killPid(firstPid);
  });

  it("store_only actor: send succeeds with NO wake field and no wake-lease key", async () => {
    const reg = await sender.register("publisher", "wake-sender-4", "sends");
    await registerActor(
      sender,
      "storeonly",
      "store_only",
      sleepyLaunch(),
      reg.session_id
    );

    const res = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "storeonly", content: "just store", type: "task" })
      )
    );
    expect(res.status).toBe("sent");
    expect(res.wake).toBeUndefined();
    expect(await redis.exists(WAKE_LEASE_KEYS.lease("storeonly"))).toBe(0);
  });

  it("plain agent (no directory record): regression, byte-identical send path", async () => {
    const plain = new RedisClient(null, TEST_REDIS_URL);
    try {
      await registerAgent(
        plain,
        registerAgentSchema.parse({
          name: "plain-rcpt",
          role: "consumer",
          description: "no durable record",
        })
      );
      const res = parseText(
        await sendMessage(
          sender,
          sendMessageSchema.parse({ to: "plain-rcpt", content: "hi", type: "task" })
        )
      );
      expect(res.status).toBe("sent");
      expect(res.wake).toBeUndefined();
      expect(await redis.exists(WAKE_LEASE_KEYS.lease("plain-rcpt"))).toBe(0);
    } finally {
      await plain.shutdown();
    }
  });

  it("launch failure: send still succeeds with wake.status launch_failed", async () => {
    const reg = await sender.register("publisher", "wake-sender-5", "sends");
    await registerActor(
      sender,
      "badlaunch",
      "wake_if_offline",
      deadLaunch(),
      reg.session_id
    );

    const res = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "badlaunch", content: "wake", type: "task" })
      )
    );
    expect(res.status).toBe("sent");
    expect(res.wake?.status).toBe("launch_failed");
    expect(res.wake?.lease_id).toBeTruthy();
    expect(res.wake?.error_message).toBeTruthy();
  });

  it("offline_store_only and unavailable recipients produce no wake field", async () => {
    const reg = await sender.register("publisher", "wake-sender-6", "sends");
    // A store_only actor that is offline is presence-gated: no wake.
    await registerActor(
      sender,
      "storeoffline",
      "store_only",
      sleepyLaunch(),
      reg.session_id
    );
    const store = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "storeoffline", content: "m", type: "task" })
      )
    );
    expect(store.wake).toBeUndefined();

    // Persist a wake_if_offline record with a null launch directly (a legacy /
    // inconsistent record) so it classifies as not_runnable -> unavailable.
    await redis.hset(
      ACTOR_KEYS.profiles,
      "unavailable-act",
      JSON.stringify({
        profile: {
          actor_id: "unavailable-act",
          alias: "unavailable-act",
          capabilities: [],
          workspace_root: "/workspace",
          working_directory: "/workspace",
          state_directory: "/state/unavailable-act",
          runtime: "manual",
          activation_policy: { mode: "wake_if_offline" },
          max_concurrency: 1,
        },
        launch: null,
        registered_by: reg.session_id,
        registered_at: T0,
      })
    );
    const unavail = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "unavailable-act", content: "m", type: "task" })
      )
    );
    expect(unavail.wake).toBeUndefined();
    expect(await redis.exists(WAKE_LEASE_KEYS.lease("unavailable-act"))).toBe(0);
  });
});

describe("task claim e2e (at-least-once delivery)", () => {
  let redis: Redis;
  let sender: RedisClient;
  const actorId = "e2e-actor";

  const deadLaunch = (): RuntimeLaunchContract => ({
    command: "/nonexistent/definitely-not-a-binary-12345",
    args: [],
  });

  const registerActorProfile = async (registeredBy: string) => {
    await registerActor(
      sender,
      actorId,
      "wake_if_offline",
      deadLaunch(),
      registeredBy
    );
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    sender = new RedisClient(null, TEST_REDIS_URL);
    const reg = await sender.register("publisher", "e2e-sender", "sends");
    expect(reg.session_id).toBeTruthy();
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await sender.shutdown();
    await redis.quit();
  });

  it("send 2 messages -> runtime registers -> claim both -> ack -> inbox empty and actor_status idle", async () => {
    const reg = await sender.register("publisher", "e2e-sender-2", "sends");
    await registerActorProfile(reg.session_id);

    // The runtime comes online under the durable actor id (live session). This
    // makes the actor idle at send time, so dispatch is skipped entirely.
    const worker = new RedisClient(null, TEST_REDIS_URL);
    let workerSession: string;
    try {
      const wreg = await registerAgent(
        worker,
        registerAgentSchema.parse({ name: actorId, role: "both", description: "runtime" })
      );
      workerSession = wreg.session_id;

      // Send two messages to the (idle, live) actor.
      const r1 = parseText(
        await sendMessage(
          sender,
          sendMessageSchema.parse({ to: actorId, content: "task one", type: "task" })
        )
      );
      const r2 = parseText(
        await sendMessage(
          sender,
          sendMessageSchema.parse({ to: actorId, content: "task two", type: "task" })
        )
      );
      expect(r1.status).toBe("sent");
      expect(r2.status).toBe("sent");
      expect(await redis.llen(SESSION_KEYS.queue(actorId))).toBe(2);

      // Runtime claims both messages in one batch.
      const claim = await claimTasks(
        worker,
        claimTasksSchema.parse({ session_id: workerSession, max_batch: 5, ttl_seconds: 300 })
      );
      const claimPayload = claim.structuredContent as {
        claimed: boolean;
        claim: { claim_id: string; tasks: string[] };
      };
      expect(claimPayload.claimed).toBe(true);
      expect(claimPayload.claim.tasks).toHaveLength(2);
      expect(claimPayload.claim.tasks.join(",")).toContain("task one");
      expect(claimPayload.claim.tasks.join(",")).toContain("task two");

      // Claimed (popped) messages are out of the inbox.
      expect(await redis.llen(SESSION_KEYS.queue(actorId))).toBe(0);

      // The holding runtime reports active.
      const during = await actorStatus(sender, actorStatusSchema.parse({ actor_id: actorId }));
      expect(during.structuredContent).toMatchObject({ presence: "active" });

      // Runtime acknowledges -> batch removed -> inbox empty and idle.
      const ack = await acknowledgeTasks(
        worker,
        acknowledgeTasksSchema.parse({ session_id: workerSession, claim_id: claimPayload.claim.claim_id })
      );
      expect(ack.structuredContent).toMatchObject({ status: "ok", acknowledged: 2 });
      expect(await redis.llen(SESSION_KEYS.queue(actorId))).toBe(0);

      const after = await actorStatus(sender, actorStatusSchema.parse({ actor_id: actorId }));
      expect(after.structuredContent).toMatchObject({ presence: "idle" });
    } finally {
      await worker.shutdown();
    }
  });

  it("crash path: claim without ack, expire, recover, and re-claim returns the messages", async () => {
    const reg = await sender.register("publisher", "e2e-sender-3", "sends");
    await registerActorProfile(reg.session_id);

    const worker = new RedisClient(null, TEST_REDIS_URL);
    let workerSession: string;
    try {
      const wreg = await registerAgent(
        worker,
        registerAgentSchema.parse({ name: actorId, role: "both", description: "runtime" })
      );
      workerSession = wreg.session_id;

      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: actorId, content: "durable msg", type: "task" })
      );

      // Runtime claims with a short TTL and then "crashes" (never acks).
      const claim = await claimTasks(
        worker,
        claimTasksSchema.parse({ session_id: workerSession, max_batch: 5, ttl_seconds: 1 })
      );
      const firstClaim = (claim.structuredContent as {
        claimed: boolean;
        claim: { claim_id: string; tasks: string[] };
      });
      expect(firstClaim.claimed).toBe(true);
      expect(firstClaim.claim.tasks).toHaveLength(1);

      // Wait past expiry so the unacked claim expires.
      await delay(1100);

      // A fresh claim lazily recovers the expired claim's tasks first, then
      // re-claims them from the inbox.
      const reclaim = await claimTasks(
        worker,
        claimTasksSchema.parse({ session_id: workerSession, max_batch: 5, ttl_seconds: 300 })
      );
      const secondClaim = (reclaim.structuredContent as {
        claimed: boolean;
        claim: { tasks: string[] };
      });
      expect(secondClaim.claimed).toBe(true);
      expect(secondClaim.claim.tasks).toHaveLength(1);
    } finally {
      await worker.shutdown();
    }
  });
});

describe("pid-liveness reconciliation", () => {
  let redis: Redis;
  let sender: RedisClient;

  const immediateExitLaunch = (): RuntimeLaunchContract => ({
    command: process.execPath,
    args: ["-e", "process.exit(0)"],
  });

  const liveSleeperLaunch = (): RuntimeLaunchContract => ({
    command: process.execPath,
    args: ["-e", "setTimeout(() => {}, 15000)"],
  });

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    sender = new RedisClient(null, TEST_REDIS_URL);
    const reg = await sender.register("publisher", "recon-sender", "sends");
    expect(reg.session_id).toBeTruthy();
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await sender.shutdown();
    await redis.quit();
  });

  it("a lease whose spawned process died is cleared so the actor classifies offline", async () => {
    const reg = await sender.register("publisher", "recon-sender-2", "sends");
    await registerActor(
      sender,
      "deadpid-act",
      "wake_if_offline",
      liveSleeperLaunch(),
      reg.session_id
    );

    // A definitely-dead pid: spawn a child that exits immediately, then wait.
    const dead = spawn(process.execPath, ["-e", "process.exit(0)"]);
    const deadPid = dead.pid!;
    await new Promise<void>((resolve) => dead.once("exit", () => resolve()));

    const acquired = await sender.wakeLease.acquire({
      actor_id: "deadpid-act",
      issued_by_session: reg.session_id,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    await sender.wakeLease.attachSpawn({
      actor_id: "deadpid-act",
      lease_id: acquired.lease.lease_id,
      pid: deadPid,
      spawned_at: T0,
    });

    // Reconciliation observes the dead pid -> clears the lease -> offline.
    const res = await actorStatus(
      sender,
      actorStatusSchema.parse({ actor_id: "deadpid-act" })
    );
    expect(res.structuredContent).toMatchObject({ presence: "offline_launchable" });
    expect((res.structuredContent as { wake_lease: unknown }).wake_lease).toBeNull();
    expect(await sender.wakeLease.get("deadpid-act")).toBeNull();
  });

  it("a lease with a live spawned pid is retained while the actor starts", async () => {
    const reg = await sender.register("publisher", "recon-sender-3", "sends");
    await registerActor(
      sender,
      "livepid-act",
      "wake_if_offline",
      liveSleeperLaunch(),
      reg.session_id
    );

    const sleeper = spawn(process.execPath, ["-e", "setTimeout(() => {}, 15000)"]);
    const livePid = sleeper.pid!;
    try {
      const acquired = await sender.wakeLease.acquire({
        actor_id: "livepid-act",
        issued_by_session: reg.session_id,
        lease_seconds: 300,
        now: T0,
      });
      if (!acquired.ok) throw new Error("expected ok acquire");
      await sender.wakeLease.attachSpawn({
        actor_id: "livepid-act",
        lease_id: acquired.lease.lease_id,
        pid: livePid,
        spawned_at: T0,
      });

      const res = await actorStatus(
        sender,
        actorStatusSchema.parse({ actor_id: "livepid-act" })
      );
      expect(res.structuredContent).toMatchObject({ presence: "starting" });
      expect(res.structuredContent).toMatchObject({
        wake_lease: {
          lease_id: acquired.lease.lease_id,
          spawned_pid: livePid,
          pid_liveness: "alive",
        },
      });
      expect(await sender.wakeLease.get("livepid-act")).not.toBeNull();
    } finally {
      killPid(livePid);
    }
  });

  it("a lease without spawn evidence is retained as starting with pid_liveness unknown", async () => {
    const reg = await sender.register("publisher", "recon-sender-4", "sends");
    await registerActor(
      sender,
      "nopid-act",
      "wake_if_offline",
      liveSleeperLaunch(),
      reg.session_id
    );

    const acquired = await sender.wakeLease.acquire({
      actor_id: "nopid-act",
      issued_by_session: reg.session_id,
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");

    const res = await actorStatus(
      sender,
      actorStatusSchema.parse({ actor_id: "nopid-act" })
    );
    expect(res.structuredContent).toMatchObject({ presence: "starting" });
    expect(res.structuredContent).toMatchObject({
      wake_lease: { lease_id: acquired.lease.lease_id, pid_liveness: "unknown" },
    });
    expect(await sender.wakeLease.get("nopid-act")).not.toBeNull();
  });

  it("e2e: a dead-pid actor is re-woken on the next send with a fresh lease", async () => {
    const reg = await sender.register("publisher", "recon-sender-5", "sends");
    await registerActor(
      sender,
      "revenge-act",
      "wake_if_offline",
      immediateExitLaunch(),
      reg.session_id
    );

    // First send dispatches a runtime that exits immediately (dead pid).
    const first = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "revenge-act", content: "wake once", type: "task" })
      )
    );
    expect(first.wake?.status).toBe("wake_dispatched");
    expect(first.wake?.lease_id).toBeTruthy();
    expect(first.wake?.pid).toBeTypeOf("number");
    await waitForDead(first.wake!.pid!); // the launched process is now dead

    // No live runtime; the dead-pid lease is reconciled away on the next
    // presence assembly, so a fresh wake is dispatched with a NEW lease.
    const second = parseText(
      await sendMessage(
        sender,
        sendMessageSchema.parse({ to: "revenge-act", content: "wake again", type: "task" })
      )
    );
    expect(second.wake?.status).toBe("wake_dispatched");
    expect(second.wake?.lease_id).toBeTruthy();
    expect(second.wake!.lease_id).not.toBe(first.wake!.lease_id);
    killPid(second.wake?.pid);
  });
});
