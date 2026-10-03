import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { createHash, randomUUID } from "node:crypto";
import { WakeLeaseStore } from "../src/core/wake-lease.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { ActivationStore } from "../src/registered-shell/activation-store.js";
import { ACTOR_KEYS, CLAIM_KEYS, CUSTODY_KEYS, DLQ_KEYS, SESSION_KEYS, WAKE_LEASE_KEYS } from "../src/core/keys.js";
import { TaskClaimStore } from "../src/core/task-claim-store.js";
import { ActorDirectory } from "../src/core/actor-directory.js";
import { CustodyStore } from "../src/core/custody-store.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

/** Stored values that are valid JSON but not a complete record read as absent/corrupt, never throw. */
describe("stored record admission", () => {
  let redis: Redis;
  const keys: string[] = [];
  beforeAll(() => { redis = new Redis(TEST_REDIS_URL); });
  afterAll(async () => { if (keys.length) await redis.del(...keys); await redis.quit(); });

  it("treats an incomplete wake lease as absent", async () => {
    const actor = `lease-${randomUUID()}`;
    keys.push(WAKE_LEASE_KEYS.lease(actor));
    await redis.set(WAKE_LEASE_KEYS.lease(actor), JSON.stringify({ lease_id: "only-an-id" }));
    expect(await new WakeLeaseStore(redis).get(actor)).toBeNull();
  });

  it("treats a corrupt activation record as absent instead of wedging the dispatcher", async () => {
    const agent = `activation-${randomUUID()}`;
    keys.push(SESSION_KEYS.activation(agent));
    const store = new ActivationStore(redis, agent);
    for (const bad of ["{not json", JSON.stringify({ operation_id: "op", state: "exploded", message_ids: [], attempt: 1, created_at: "t" })]) {
      await redis.set(SESSION_KEYS.activation(agent), bad);
      await expect(store.current()).resolves.toBeNull();
    }
  });

  it("an unbounded receive does not stall other receives on the same store", async () => {
    const [idle, busy] = [`idle-${randomUUID()}`, `busy-${randomUUID()}`];
    keys.push(SESSION_KEYS.queue(idle), SESSION_KEYS.queue(busy), SESSION_KEYS.mailboxMeta(idle), SESSION_KEYS.mailboxMeta(busy));
    const mailbox = new MailboxStore(redis, redis.duplicate());
    const waiting = new AbortController();
    const unbounded = mailbox.receive(idle, 0, waiting.signal).catch(() => null); // blocks until a message or abort
    await new Promise((r) => setTimeout(r, 100));
    await redis.rpush(SESSION_KEYS.queue(busy), JSON.stringify({ id: "b1" }));
    const started = Date.now();
    await expect(mailbox.receive(busy, 5)).resolves.toMatchObject({ id: "b1" });
    expect(Date.now() - started).toBeLessThan(2_000);
    await redis.rpush(SESSION_KEYS.queue(idle), JSON.stringify({ id: "i1" }));
    await expect(unbounded).resolves.toMatchObject({ id: "i1" });
  });

  it("receive records the post-pop queue depth in mailbox metadata", async () => {
    const agent = `depth-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent));
    await redis.rpush(SESSION_KEYS.queue(agent), JSON.stringify({ id: "a" }), JSON.stringify({ id: "b" }));
    await new MailboxStore(redis, redis).receive(agent, 1);
    expect(await redis.hget(SESSION_KEYS.mailboxMeta(agent), "current_size")).toBe("1");
  });

  it("recovers tasks from an expired claim record that lacks actor_id", async () => {
    const agent = `recover-${randomUUID()}`;
    const claimId = `claim-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), CLAIM_KEYS.index(agent), DLQ_KEYS.list(agent));
    const task = JSON.stringify({ id: "t1", type: "task" });
    await redis.hset(CLAIM_KEYS.claims, claimId, JSON.stringify({ claim_id: claimId, tasks: [task] }));
    await redis.zadd(CLAIM_KEYS.index(agent), 1, claimId);
    const result = await new TaskClaimStore(redis).recoverExpired({ actor_id: agent, now: new Date().toISOString() });
    expect(result).toMatchObject({ ok: true, recovered: 1, deadlettered: 0 });
    expect(await redis.lrange(SESSION_KEYS.queue(agent), 0, -1)).toEqual([task]);
  });

  it("store_corrupt diagnostics never echo stored bytes, even a session id at the front", async () => {
    const sid = randomUUID();
    const actor = `diag-actor-${randomUUID()}`;
    const path = `/w/diag-${randomUUID()}`;
    const claimId = `claim-${randomUUID()}`;
    const leaseActor = `diag-lease-${randomUUID()}`;
    keys.push(WAKE_LEASE_KEYS.lease(leaseActor));
    const profile = {
      actor_id: actor, alias: "diag", capabilities: [], workspace_root: "/workspace",
      working_directory: "/workspace", runtime: "manual",
      activation_policy: { mode: "store_only" }, max_concurrency: 1,
    };
    // Each value is corrupt and carries a session id in its first 80 characters.
    const stored = {
      actor: JSON.stringify({ registered_by: sid, profile }), // no launch
      custody: JSON.stringify({ custodian: { session_id: sid, actor_name: "a" }, state: "held" }), // no worktree
      claim: JSON.stringify({ session_id: sid, claim_id: claimId }), // no actor_id
      lease: JSON.stringify({ issued_by_session: sid, lease_id: "l" }), // no actor_id
    };
    const expected = (raw: string, field: string) =>
      `${Buffer.byteLength(raw, "utf8")} bytes, sha256:${createHash("sha256").update(raw).digest("hex").slice(0, 12)}, failing: ${field}`;
    for (const raw of Object.values(stored)) expect(raw.slice(0, 80)).toContain(sid);

    await redis.hset(ACTOR_KEYS.profiles, actor, stored.actor);
    await redis.hset(CUSTODY_KEYS.records, path, stored.custody);
    await redis.hset(CLAIM_KEYS.claims, claimId, stored.claim);
    await redis.set(WAKE_LEASE_KEYS.lease(leaseActor), stored.lease);
    try {
      const messageOf = (result: { ok: boolean; error?: { code: string; message: string } }) => {
        if (result.ok || result.error?.code !== "store_corrupt") throw new Error(`expected store_corrupt: ${JSON.stringify(result)}`);
        return result.error.message;
      };
      const messages = {
        actor: messageOf(await new ActorDirectory(redis).get(actor)),
        custody: messageOf(await new CustodyStore(redis).status({ worktree_path: path, now: new Date().toISOString() })),
        claim: messageOf(await new TaskClaimStore(redis).get(claimId)),
        lease: messageOf(await new WakeLeaseStore(redis).acquire({
          actor_id: leaseActor, issued_by_session: "other", lease_seconds: 60, now: new Date().toISOString(),
        })),
      };
      expect(Object.entries(messages).filter(([, message]) => message.includes(sid)).map(([store]) => store)).toEqual([]);
      expect(messages.actor).toContain(`${actor}: ${expected(stored.actor, "launch")}`);
      expect(messages.custody).toContain(`${path}: ${expected(stored.custody, "worktree")}`);
      expect(messages.claim).toContain(expected(stored.claim, "actor_id"));
      expect(messages.lease).toContain(expected(stored.lease, "actor_id"));
    } finally {
      await redis.hdel(ACTOR_KEYS.profiles, actor);
      await redis.hdel(CUSTODY_KEYS.records, path);
      await redis.hdel(CLAIM_KEYS.claims, claimId);
    }
  });

  it("parks an unparseable received payload in the DLQ instead of throwing", async () => {
    const agent = `recv-${randomUUID()}`;
    keys.push(SESSION_KEYS.queue(agent), DLQ_KEYS.list(agent), SESSION_KEYS.mailboxMeta(agent));
    await redis.rpush(SESSION_KEYS.queue(agent), "{not json");
    const mailbox = new MailboxStore(redis, redis);
    await expect(mailbox.receive(agent, 1)).resolves.toBeNull();
    expect(await redis.lrange(DLQ_KEYS.list(agent), 0, -1)).toEqual(["{not json"]);
  });
});
