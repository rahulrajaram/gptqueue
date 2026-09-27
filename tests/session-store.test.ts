import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { SessionStore } from "../src/core/session-store.js";
import { SESSION_KEYS, SESSION_DEFAULTS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";


describe("SessionStore", () => {
  let redis: Redis;
  let store: SessionStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new SessionStore(redis);
  });

  afterEach(async () => {
    store.stopLeaseRefresh();
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("creates a session with a unique ID and stores it in Redis", async () => {
    const session = await store.createSession("agent-a", "both", "test agent");

    expect(session.session_id).toBeTruthy();
    expect(session.agent_name).toBe("agent-a");
    expect(session.role).toBe("both");

    // Verify Redis state
    const stored = await store.getSession(session.session_id);
    expect(stored).not.toBeNull();
    expect(stored!.agent_name).toBe("agent-a");

    // Verify agent-sessions set
    const sessions = await redis.smembers(
      SESSION_KEYS.agentSessions("agent-a")
    );
    expect(sessions).toContain(session.session_id);
  });

  it("refreshes lease and extends TTL", async () => {
    const session = await store.createSession("agent-b", "consumer");

    // Age the lease to a known-short TTL so the refresh has to restore it.
    await redis.set(SESSION_KEYS.lease(session.session_id), "alive", "EX", 2);
    const lease1 = await store.getLeaseState(session.session_id);
    expect(lease1.alive).toBe(true);
    expect(lease1.ttl_seconds).toBeLessThanOrEqual(2);

    await store.refreshLease(session.session_id);

    const lease2 = await store.getLeaseState(session.session_id);
    expect(lease2.alive).toBe(true);
    expect(lease2.ttl_seconds).toBeGreaterThan(SESSION_DEFAULTS.LEASE_TTL_SECONDS - 5);
  });

  it("closes a session without deleting the mailbox", async () => {
    const session = await store.createSession("agent-c", "both");

    // Simulate a mailbox with data
    await redis.rpush(SESSION_KEYS.queue("agent-c"), "test-message");

    const agentName = await store.closeSession(session.session_id);
    expect(agentName).toBe("agent-c");

    // Session is gone
    const closed = await store.getSession(session.session_id);
    expect(closed).toBeNull();

    // Mailbox data is preserved
    const queueLen = await redis.llen(SESSION_KEYS.queue("agent-c"));
    expect(queueLen).toBe(1);
  });

  it("reports agent as offline when all sessions are closed", async () => {
    const session = await store.createSession("agent-d", "both");

    const before = await store.getPresence("agent-d");
    expect(before.online).toBe(true);
    expect(before.active_sessions).toHaveLength(1);

    await store.closeSession(session.session_id);

    const after = await store.getPresence("agent-d");
    expect(after.online).toBe(false);
    expect(after.active_sessions).toHaveLength(0);
  });

  it("supports multiple sessions for one agent", async () => {
    const s1 = await store.createSession("multi-agent", "both", "session 1");
    const s2 = await store.createSession("multi-agent", "both", "session 2");

    const presence = await store.getPresence("multi-agent");
    expect(presence.online).toBe(true);
    expect(presence.active_sessions).toHaveLength(2);

    // Close one session -- agent remains online
    await store.closeSession(s1.session_id);

    const afterOne = await store.getPresence("multi-agent");
    expect(afterOne.online).toBe(true);
    expect(afterOne.active_sessions).toHaveLength(1);

    // Close second -- agent goes offline
    await store.closeSession(s2.session_id);

    const afterBoth = await store.getPresence("multi-agent");
    expect(afterBoth.online).toBe(false);
  });

  it("computes presence for several agents in one call without pruning on read", async () => {
    const live = await store.createSession("pm-live", "both");
    const stale = await store.createSession("pm-mixed", "both");
    const fresh = await store.createSession("pm-mixed", "both");
    const gone = await store.createSession("pm-gone", "both");
    await redis.del(SESSION_KEYS.lease(stale.session_id), SESSION_KEYS.lease(gone.session_id));

    const presences = await store.getPresenceMany(["pm-gone", "pm-live", "pm-mixed", "pm-none"]);
    expect(presences).toEqual([
      { agent_name: "pm-gone", online: false, active_sessions: [] },
      { agent_name: "pm-live", online: true, active_sessions: [live.session_id] },
      { agent_name: "pm-mixed", online: true, active_sessions: [fresh.session_id] },
      { agent_name: "pm-none", online: false, active_sessions: [] },
    ]);
    expect(await redis.scard(SESSION_KEYS.agentSessions("pm-mixed"))).toBe(2);
    expect(await redis.exists(SESSION_KEYS.session(stale.session_id), SESSION_KEYS.session(gone.session_id))).toBe(2);
    expect(await store.getPresenceMany([])).toEqual([]);
  });

  it("brings a stalled session back online when its lease is refreshed after a presence read", async () => {
    const stalled = await store.createSession("stall-agent", "both");
    await redis.del(SESSION_KEYS.lease(stalled.session_id));
    expect((await store.getPresence("stall-agent")).online).toBe(false);

    expect(await store.refreshLease(stalled.session_id)).toBe(true);
    expect(await store.getPresence("stall-agent")).toEqual({
      agent_name: "stall-agent", online: true, active_sessions: [stalled.session_id],
    });
    expect((await store.getSession(stalled.session_id))?.agent_name).toBe("stall-agent");
  });

  it("does not resurrect a closed session on a late lease refresh", async () => {
    const closed = await store.createSession("late-refresh-agent", "both");
    await store.closeSession(closed.session_id);

    expect(await store.refreshLease(closed.session_id)).toBe(false);
    expect(await redis.exists(SESSION_KEYS.session(closed.session_id), SESSION_KEYS.lease(closed.session_id))).toBe(0);
    expect(await redis.scard(SESSION_KEYS.agentSessions("late-refresh-agent"))).toBe(0);
  });

  it("keeps every session when one agent registers concurrently", async () => {
    const sessions = await Promise.all(
      // Staggered starts make later prunes overlap earlier, still-initializing registrations.
      Array.from({ length: 20 }, (_, i) =>
        new Promise((r) => setTimeout(r, i)).then(() => store.createSession("burst-agent", "both"))
      )
    );
    expect((await store.getPresence("burst-agent")).active_sessions.sort()).toEqual(
      sessions.map((s) => s.session_id).sort()
    );
  });

  it("prunes an agent's expired sessions when the agent registers again", async () => {
    const live = await store.createSession("prune-agent", "both");
    const expired = await store.createSession("prune-agent", "both");
    await redis.del(SESSION_KEYS.lease(expired.session_id));

    const next = await store.createSession("prune-agent", "both");
    expect((await redis.smembers(SESSION_KEYS.agentSessions("prune-agent"))).sort()).toEqual(
      [live.session_id, next.session_id].sort()
    );
    expect(await redis.exists(SESSION_KEYS.session(expired.session_id))).toBe(0);
    expect(await store.refreshLease(expired.session_id)).toBe(false);
  });

  it("resolves agent name from session ID", async () => {
    const session = await store.createSession("lookup-agent", "publisher");

    const resolved = await store.resolveAgent(session.session_id);
    expect(resolved).toBe("lookup-agent");

    // Non-existent session returns null
    const missing = await store.resolveAgent("no-such-session");
    expect(missing).toBeNull();
  });

  it("lists all sessions for an agent", async () => {
    await store.createSession("list-agent", "both", "first");
    await store.createSession("list-agent", "both", "second");

    const sessions = await store.listSessions("list-agent");
    expect(sessions).toHaveLength(2);
  });
});
