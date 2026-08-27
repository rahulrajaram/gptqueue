import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { SessionStore } from "../src/core/session-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("SessionStore", () => {
  let redis: Redis;
  let store: SessionStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis);
    store = new SessionStore(redis);
  });

  afterEach(async () => {
    store.stopLeaseRefresh();
    await flushTestKeys(redis);
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

    const lease1 = await store.getLeaseState(session.session_id);
    expect(lease1.alive).toBe(true);
    expect(lease1.ttl_seconds).toBeGreaterThan(0);

    await store.refreshLease(session.session_id);

    const lease2 = await store.getLeaseState(session.session_id);
    expect(lease2.alive).toBe(true);
    expect(lease2.ttl_seconds).toBeGreaterThan(0);
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
