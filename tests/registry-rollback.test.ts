/**
 * Rename-rollback bookkeeping for the agent registry (R2 redesign).
 *
 * A rename publishes its destination's registry entry before migrating the
 * mailbox, and undoes that write if the migration fails. Registry values are
 * exactly what JavaScript wrote (JSON.stringify, never decoded in Lua); the
 * rollback state lives in two side hashes (SESSION_KEYS.registryPending and
 * registryRestore) that no public reader touches. These tests pin:
 *   - the R2 regression: metadata Lua cjson cannot decode (an unpaired
 *     surrogate) registers exactly as before, and a failing registry step
 *     leaves no orphaned session;
 *   - every interleaving of 2 and 3 overlapping failed renames, over a free
 *     name, a completed offline registration and a completed online owner;
 *   - a successful rename or registration overlapping failed attempts;
 *   - that no public reader returns the side hashes' data.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { AgentDiagnostics } from "../src/core/agent-diagnostics.js";
import { listAgents } from "../src/mcp-server/tools/list-agents.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const N = "shared-new";

type Gate = Readonly<{
  start: () => Promise<{ attempt: Promise<unknown> }>;
  release: () => void;
}>;

/**
 * Register `who` under `oldName`, then make its rename onto N stop at the
 * migration step until released. A failing gate rejects there (driving the
 * rollback); a succeeding one runs the real migration.
 */
const gatedRename = async (who: RedisClient, oldName: string, outcome: "fail" | "succeed" = "fail"): Promise<Gate> => {
  await who.register("both", oldName, oldName);
  const internals = who as unknown as { mailbox: MailboxStore };
  const realMigrate = internals.mailbox.migrateMessages.bind(internals.mailbox);
  let reached!: () => void;
  let release!: () => void;
  const atMigration = new Promise<void>((r) => { reached = r; });
  const released = new Promise<void>((r) => { release = r; });
  internals.mailbox.migrateMessages = (async (...args: Parameters<MailboxStore["migrateMessages"]>) => {
    reached();
    await released;
    if (outcome === "fail") throw new Error("migration failed");
    return realMigrate(...args);
  }) as MailboxStore["migrateMessages"];
  return {
    start: async () => {
      const attempt = who.register("both", N, `${oldName} attempt`);
      attempt.catch(() => {});
      await atMigration;
      return { attempt }; // wrapped: an async function would flatten it
    },
    release,
  };
};

type Step = Readonly<{ kind: "write" | "undo"; attempt: number }>;

/** Every order of k attempts' write and undo events with each write before its undo. */
const interleavings = (k: number): Step[][] => {
  const out: Step[][] = [];
  const walk = (states: number[], path: Step[]) => {
    if (states.every((s) => s === 2)) { out.push(path); return; }
    states.forEach((s, i) => {
      if (s === 2) return;
      const next = [...states];
      next[i] = s + 1;
      walk(next, [...path, { kind: s === 0 ? "write" : "undo", attempt: i }]);
    });
  };
  walk(Array(k).fill(0), []);
  return out;
};

/** Session ids whose record names `agent`, from a scan of this test db. */
const sessionsOf = async (redis: Redis, agent: string): Promise<string[]> => {
  const ids: string[] = [];
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", "gptq:session:*", "COUNT", 500);
    cursor = next;
    for (const key of keys) {
      if ((await redis.hget(key, "agent_name")) === agent) ids.push(key.slice("gptq:session:".length));
    }
  } while (cursor !== "0");
  return ids.sort();
};

const sideState = async (redis: Redis, name: string) => ({
  pending: await redis.hget(SESSION_KEYS.registryPending, name),
  restore: await redis.hget(SESSION_KEYS.registryRestore, name),
});

const initialStates = ["free", "completed offline", "completed online"] as const;
type InitialState = (typeof initialStates)[number];

describe("registry rollback bookkeeping (R2)", () => {
  let redis: Redis;
  const live: RedisClient[] = [];
  const tracked = () => {
    const c = new RedisClient(null, TEST_REDIS_URL);
    live.push(c);
    return c;
  };
  const shutdownAll = async () => {
    for (const c of live.splice(0)) await c.shutdown();
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await shutdownAll();
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  /** Set up N in `state`; returns the owner's live session id, if any. */
  const prepare = async (state: InitialState): Promise<string | null> => {
    if (state === "free") return null;
    const owner = tracked();
    const { session_id } = await owner.register("both", N, "existing owner");
    if (state === "completed online") return session_id;
    await owner.closeCurrentSession();
    return null;
  };

  it("re-registering with a description Lua cjson cannot decode stores the baseline value (R2)", async () => {
    const client = tracked();
    const first = await client.register("both", N, "ok");
    const surrogate = "\uD800";

    const second = await client.register("both", N, surrogate);

    const raw = await redis.hget(SESSION_KEYS.registry, N);
    const stored = JSON.parse(raw!);
    expect(stored.description).toBe(surrogate);
    // Exactly what register() has always written: JSON.stringify of its registration.
    expect(raw).toBe(JSON.stringify({ name: N, role: "both", description: surrogate, registered_at: stored.registered_at, pid: process.pid }));
    // Re-registering a name has always kept its earlier session; nothing else exists.
    const both = [first.session_id, second.session_id].sort();
    expect(await sessionsOf(redis, N)).toEqual(both);
    expect((await redis.smembers(SESSION_KEYS.agentSessions(N))).sort()).toEqual(both);
  });

  it("a failing registry step leaves no session, lease or membership behind (R2)", async () => {
    const client = tracked();
    await client.register("both", "old-name", "first");
    // A wrong-typed registry makes the registry step fail before any write.
    await redis.set(SESSION_KEYS.registry, "not-a-hash");

    await expect(client.register("both", N, "second")).rejects.toThrow(/WRONGTYPE/);

    expect(await sessionsOf(redis, N)).toEqual([]);
    expect(await redis.exists(SESSION_KEYS.agentSessions(N))).toBe(0);
    let leases = 0;
    let cursor = "0";
    do {
      const [next, keys] = await redis.scan(cursor, "MATCH", "gptq:lease:*", "COUNT", 500);
      cursor = next;
      leases += keys.length;
    } while (cursor !== "0");
    expect(leases).toBe(1); // only old-name's own session
    expect(client.agentName).toBe("old-name");
  });

  // Exhaustive: every interleaving of k overlapping failed renames' writes
  // and undos (each write before its own undo), over each initial state.
  for (const state of initialStates) {
    for (const k of [2, 3]) {
      const schedules = interleavings(k);
      it(`every one of the ${schedules.length} interleavings of ${k} failed renames onto a ${state} name restores it exactly (R2)`, async () => {
        let writesFirst = 0;
        const unwindOrders = new Set<string>();
        for (const schedule of schedules) {
          await flushTestKeys(redis, TEST_REDIS_URL);
          const ownerSession = await prepare(state);
          const before = await redis.hget(SESSION_KEYS.registry, N);
          const gates = await Promise.all(Array.from({ length: k }, (_, i) => gatedRename(tracked(), `old-${i}`)));
          const attempts: Promise<unknown>[] = [];
          for (const step of schedule) {
            if (step.kind === "write") {
              attempts[step.attempt] = (await gates[step.attempt]!.start()).attempt;
            } else {
              gates[step.attempt]!.release();
              await expect(attempts[step.attempt]).rejects.toThrow(/migration failed/);
            }
          }
          if (schedule.slice(0, k).every((s) => s.kind === "write")) {
            writesFirst += 1;
            unwindOrders.add(schedule.slice(k).map((s) => s.attempt).join(","));
          }

          const label = `${state}: ${schedule.map((s) => `${s.kind[0]}${s.attempt}`).join(" ")}`;
          expect(await redis.hget(SESSION_KEYS.registry, N), label).toBe(before);
          expect(await sideState(redis, N), label).toEqual({ pending: null, restore: null });
          expect(await sessionsOf(redis, N), label).toEqual(ownerSession ? [ownerSession] : []);
          expect(await redis.smembers(SESSION_KEYS.agentSessions(N)), label).toEqual(ownerSession ? [ownerSession] : []);
          await shutdownAll();
        }
        // Fully overlapping schedules (every write before any undo) cover
        // every write order times every one of the k! unwind orders.
        const factorial = k === 2 ? 2 : 6;
        expect(writesFirst).toBe(factorial * factorial);
        expect(unwindOrders.size).toBe(factorial);
      }, 120_000);
    }
  }

  for (const state of initialStates) {
    for (const order of ["winner publishes first", "losers unwind first"] as const) {
      it(`a successful rename overlapping failed ones onto a ${state} name wins: ${order} (R2)`, async () => {
        await prepare(state);
        const loserA = await gatedRename(tracked(), "old-a");
        const winner = await gatedRename(tracked(), "old-w", "succeed");
        const loserB = await gatedRename(tracked(), "old-b");
        const a = (await loserA.start()).attempt;
        const w = (await winner.start()).attempt;
        const b = (await loserB.start()).attempt;
        const finish = async (gate: Gate, attempt: Promise<unknown>, wins: boolean) => {
          gate.release();
          if (wins) await expect(attempt).resolves.toMatchObject({ name: N });
          else await expect(attempt).rejects.toThrow(/migration failed/);
        };
        if (order === "winner publishes first") {
          await finish(winner, w, true);
          await finish(loserA, a, false);
          await finish(loserB, b, false);
        } else {
          await finish(loserB, b, false);
          await finish(loserA, a, false);
          await finish(winner, w, true);
        }

        const raw = await redis.hget(SESSION_KEYS.registry, N);
        expect(JSON.parse(raw!)).toMatchObject({ name: N, description: "old-w attempt", pid: process.pid });
        expect(await sideState(redis, N)).toEqual({ pending: null, restore: null });
      });
    }
  }

  for (const order of ["registration completes first", "rollback lands between the registration's write and its publication", "rollback completes first"] as const) {
    it(`a plain registration overlapping a failed rename wins: ${order} (R2)`, async () => {
      const loser = await gatedRename(tracked(), "old-a");
      const a = (await loser.start()).attempt;
      const registrant = tracked();
      const sessions = registrant.sessions as unknown as { publishRegistration: (name: string, value: string) => Promise<void> };
      const realPublish = sessions.publishRegistration.bind(sessions);
      let reachedPublish!: () => void;
      let releasePublish!: () => void;
      const atPublish = new Promise<void>((r) => { reachedPublish = r; });
      const publishReleased = new Promise<void>((r) => { releasePublish = r; });
      sessions.publishRegistration = async (name, value) => {
        reachedPublish();
        await publishReleased;
        return realPublish(name, value);
      };
      const rollback = async () => {
        loser.release();
        await expect(a).rejects.toThrow(/migration failed/);
      };

      if (order === "rollback completes first") await rollback();
      const registering = registrant.register("both", N, "plain registration");
      await atPublish; // its provisional write has landed
      if (order.startsWith("rollback lands")) await rollback();
      releasePublish();
      await registering;
      if (order === "registration completes first") await rollback();

      const raw = await redis.hget(SESSION_KEYS.registry, N);
      expect(JSON.parse(raw!)).toMatchObject({ name: N, description: "plain registration", pid: process.pid });
      expect(await sideState(redis, N)).toEqual({ pending: null, restore: null });
    });
  }

  it("a completed registration publishes the plain JSON value and clears the name's rollback state (R2)", async () => {
    await redis.hset(SESSION_KEYS.registryPending, N, "stale-pending");
    await redis.hset(SESSION_KEYS.registryRestore, N, "stale-restore");
    const client = tracked();

    await client.register("both", N, "fresh");

    const raw = await redis.hget(SESSION_KEYS.registry, N);
    expect(raw).toBe(JSON.stringify({ name: N, role: "both", description: "fresh", registered_at: JSON.parse(raw!).registered_at, pid: process.pid }));
    expect(await sideState(redis, N)).toEqual({ pending: null, restore: null });
  });

  it("no public reader returns the rollback side hashes' data (R2)", async () => {
    // Grep: only the registry writers may name the side hashes.
    const allowed = new Set(["src/core/keys.ts", "src/core/session-store.ts", "src/mcp-server/redis-client.ts"]);
    const root = fileURLToPath(new URL("..", import.meta.url)).replace(/\/$/, "");
    const walk = (dir: string): string[] =>
      readdirSync(dir).flatMap((entry) => {
        const path = join(dir, entry);
        return statSync(path).isDirectory() ? walk(path) : [path];
      });
    const offenders = walk(join(root, "src"))
      .map((path) => path.slice(root.length + 1))
      .filter((rel) => /registryPending|registryRestore|gptq:registry:/.test(readFileSync(join(root, rel), "utf-8")))
      .filter((rel) => !allowed.has(rel));
    expect(offenders).toEqual([]);

    // Behaviour: with an in-flight attempt over a completed owner, the
    // restore hash holds the owner's value; no public read may show it.
    const owner = tracked();
    await owner.register("both", N, "completed-owner-only-in-restore");
    await tracked().sessions.createSession(N, "both", "in-flight attempt");
    expect(await redis.hget(SESSION_KEYS.registryRestore, N)).toContain("completed-owner-only-in-restore");
    const diagnostics = new AgentDiagnostics(redis);
    const reads = {
      list_agents: await listAgents(owner),
      get_agent_details: await diagnostics.details(N),
      find_agents: await diagnostics.find({ query: N }),
    };
    expect(JSON.stringify(reads.find_agents)).toContain("in-flight attempt");
    for (const [tool, result] of Object.entries(reads)) {
      expect(JSON.stringify(result), tool).not.toMatch(/completed-owner-only-in-restore|registry:pending|registry:restore/);
    }
  });
});
