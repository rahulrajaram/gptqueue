import { testRedisUrl } from "./helpers/test-redis.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { readFileSync, readlinkSync } from "fs";
import { spawn } from "child_process";
import { WakeLeaseStore } from "../src/core/wake-lease.js";
import { reconcileWakeLease } from "../src/mcp-server/tools/reconcile-wake-lease.js";

const TEST_REDIS_URL = testRedisUrl();
const T0 = "2030-01-01T00:00:00.000Z";

/** This machine's kernel boot id, read the same way the dispatcher records it. */
const localBootId = (): string | null => {
  try {
    return readFileSync("/proc/sys/kernel/random/boot_id", "utf-8").trim() || null;
  } catch {
    return null;
  }
};

/** This process's PID-namespace inode, read the same way the dispatcher records it. */
const localPidns = (): string | null => {
  try {
    return readlinkSync("/proc/self/ns/pid");
  } catch {
    return null;
  }
};

const FOREIGN_BOOT_ID = "00000000-0000-0000-0000-000000000000";
const FOREIGN_PIDNS = "pid:[4026532963]";

/** A pid that is deterministically not alive by the time it is returned. */
const deadPid = async (): Promise<number> => {
  const child = spawn(process.execPath, ["-e", "process.exit(0)"]);
  const pid = child.pid!;
  await new Promise<void>((resolve) => child.once("exit", () => resolve()));
  return pid;
};

describe("reconcileWakeLease host-identity guard", () => {
  let redis: Redis;
  let store: WakeLeaseStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL);
    await flushTestKeys(redis);
    store = new WakeLeaseStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis);
    await redis.quit();
  });

  const acquireLease = async (actorId: string): Promise<string> => {
    const acquired = await store.acquire({
      actor_id: actorId,
      issued_by_session: "session-waker",
      lease_seconds: 300,
      now: T0,
    });
    if (!acquired.ok) throw new Error("expected ok acquire");
    return acquired.lease.lease_id;
  };

  it("does not clear a lease whose spawn identity is from another boot: pid_liveness unknown, lease left to its TTL", async () => {
    const actorId = "wake-guard-cross-boot";
    const leaseId = await acquireLease(actorId);
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid: await deadPid(),
      spawned_at: T0,
      boot_id: FOREIGN_BOOT_ID,
      pidns: FOREIGN_PIDNS,
    });

    const outcome = await reconcileWakeLease({ wakeLease: store }, actorId);
    expect(outcome).toEqual({ cleared: false, pid_liveness: "unknown" });
    // The lease survives: only its TTL may expire the in-flight activation.
    expect(await store.get(actorId)).not.toBeNull();
  });

  it.skipIf(localBootId() === null)("does not clear a lease whose spawn identity is from another PID namespace (same boot)", async () => {
    const actorId = "wake-guard-cross-pidns";
    const leaseId = await acquireLease(actorId);
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid: await deadPid(),
      spawned_at: T0,
      boot_id: localBootId(),
      pidns: FOREIGN_PIDNS,
    });

    const outcome = await reconcileWakeLease({ wakeLease: store }, actorId);
    expect(outcome).toEqual({ cleared: false, pid_liveness: "unknown" });
    expect(await store.get(actorId)).not.toBeNull();
  });

  it.skipIf(localBootId() === null || localPidns() === null)("still clears a same-boot, same-PID-namespace dead-pid lease (guard passes through)", async () => {
    const actorId = "wake-guard-local-dead";
    const leaseId = await acquireLease(actorId);
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid: await deadPid(),
      spawned_at: T0,
      boot_id: localBootId(),
      pidns: localPidns(),
    });

    const outcome = await reconcileWakeLease({ wakeLease: store }, actorId);
    expect(outcome).toEqual({ cleared: true, pid_liveness: "dead" });
    expect(await store.get(actorId)).toBeNull();
  });

  it("reconciles a legacy lease without host identity by local pid liveness as before", async () => {
    const actorId = "wake-guard-legacy";
    const leaseId = await acquireLease(actorId);
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid: await deadPid(),
      spawned_at: T0,
    });

    const outcome = await reconcileWakeLease({ wakeLease: store }, actorId);
    expect(outcome).toEqual({ cleared: true, pid_liveness: "dead" });
    expect(await store.get(actorId)).toBeNull();
  });

  it("attachSpawn records the spawner's boot id and PID namespace on the lease", async () => {
    const actorId = "wake-guard-evidence";
    const leaseId = await acquireLease(actorId);
    const pid = await deadPid();
    await store.attachSpawn({
      actor_id: actorId,
      lease_id: leaseId,
      pid,
      spawned_at: T0,
      boot_id: FOREIGN_BOOT_ID,
      pidns: FOREIGN_PIDNS,
    });

    const lease = await store.get(actorId);
    expect(lease?.spawned_pid).toBe(pid);
    expect(lease?.spawned_boot_id).toBe(FOREIGN_BOOT_ID);
    expect(lease?.spawned_pidns).toBe(FOREIGN_PIDNS);
  });
});
