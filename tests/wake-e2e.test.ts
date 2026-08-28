import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
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
import { WAKE_LEASE_KEYS, ACTOR_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

const T0 = "2030-01-01T00:00:00.000Z";

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
      state_directory: `/state/${actorId}`,
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
    await flushTestKeys(redis);
    sender = new RedisClient(null, TEST_REDIS_URL);
    const reg = await sender.register("publisher", "wake-sender", "sends");
    expect(reg.session_id).toBeTruthy();
  });

  afterEach(async () => {
    await flushTestKeys(redis);
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
