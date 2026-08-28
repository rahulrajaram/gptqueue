import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";
import {
  custodyClaim,
  custodyClaimSchema,
} from "../src/mcp-server/tools/custody-claim.js";
import {
  custodyRelease,
  custodyReleaseSchema,
} from "../src/mcp-server/tools/custody-release.js";
import {
  custodyStatus,
  custodyStatusSchema,
} from "../src/mcp-server/tools/custody-status.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("Tool contract (NXT-015)", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("register -> send -> crash -> reconnect -> receive (session survives crash)", async () => {
    // Agent A sends a message to agent B
    const clientA = new RedisClient(null, TEST_REDIS_URL);
    const clientB1 = new RedisClient(null, TEST_REDIS_URL);

    await clientA.register("publisher", "tool-sender", "sends stuff");
    const regB = await clientB1.register("consumer", "tool-receiver", "receives stuff");

    await clientA.sendMessage({
      id: "tc-1",
      from: "tool-sender",
      to: "tool-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "important work" },
    });

    // B "crashes" -- shutdown without close_session (session remains in Redis)
    await clientB1.shutdown();

    // New process for B reconnects with original session_id
    const clientB2 = new RedisClient(null, TEST_REDIS_URL);
    const resumed = await clientB2.reconnectSession(regB.session_id);
    expect(resumed).toBe("tool-receiver");

    // B2 can receive the queued message
    const msg = await clientB2.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("important work");

    await clientA.shutdown();
    await clientB2.shutdown();
  });

  it("close_session preserves mailbox for later re-registration", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "close-test", "will close session");

    // Send a message to self
    await client.sendMessage({
      id: "cs-1",
      from: "close-test",
      to: "close-test",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "preserved message" },
    });

    // Close session -- mailbox preserved
    const closedName = await client.closeCurrentSession();
    expect(closedName).toBe("close-test");
    expect(client.registered).toBe(false);

    // Verify mailbox still has the message
    const depth = await cleanup.llen("gptq:q:close-test");
    expect(depth).toBe(1);

    // Re-register as the same agent (new session) and receive the message
    const client2 = new RedisClient(null, TEST_REDIS_URL);
    await client2.register("both", "close-test", "re-registered");

    const msg = await client2.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("preserved message");

    await client.shutdown();
    await client2.shutdown();
  });

  it("unregister deletes mailbox while close_session preserves it", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "delete-test", "will be deleted");

    // Send a message to self
    await client.sendMessage({
      id: "dt-1",
      from: "delete-test",
      to: "delete-test",
      timestamp: new Date().toISOString(),
      type: "ping",
      payload: { content: "keep me?" },
    });

    // Unregister (destructive) -- mailbox should be deleted
    await client.unregister();

    const depth = await cleanup.llen("gptq:q:delete-test");
    expect(depth).toBe(0);

    // Agent should not appear in list
    const client2 = new RedisClient(null, TEST_REDIS_URL);
    const agents = await client2.listAgents();
    expect(agents.find((a) => a.name === "delete-test")).toBeUndefined();

    await client.shutdown();
    await client2.shutdown();
  });

  it("register returns session_id that can be used to reconnect", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    const result = await client.register("both", "session-test", "test");

    expect(result.session_id).toBeTruthy();
    expect(result.name).toBe("session-test");
    expect(client.sessionId).toBe(result.session_id);

    await client.shutdown();
  });

  it("claims, releases, and queries custody through the MCP tools", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    const reg = await client.register("both", "custody-sender", "tests custody");

    const claimed = await custodyClaim(
      client,
      custodyClaimSchema.parse({
        session_id: reg.session_id,
        worktree_path: "/work/custody-a",
        repo_head: "abc123",
        tree_fingerprint: "fp-a",
        lease_seconds: 60,
      })
    );
    expect(claimed.isError).toBeUndefined();
    expect(claimed.structuredContent).toMatchObject({ status: "ok" });
    expect(
      (claimed.structuredContent as { record: { state: string } }).record.state
    ).toBe("held");

    // A double claim surfaces a structured domain error, not a throw.
    const double = await custodyClaim(
      client,
      custodyClaimSchema.parse({
        session_id: reg.session_id,
        worktree_path: "/work/custody-a",
        repo_head: "abc123",
        tree_fingerprint: "fp-a",
        lease_seconds: 60,
      })
    );
    expect(double.isError).toBe(true);
    expect(double.structuredContent).toMatchObject({
      status: "error",
      error: { code: "already_held" },
    });

    // Single-record status reflects the held lease.
    const st = await custodyStatus(
      client,
      custodyStatusSchema.parse({ worktree_path: "/work/custody-a" })
    );
    expect(st.structuredContent).toMatchObject({ status: "ok" });
    const rec = (st.structuredContent as { record: { lease_expires_at?: string } }).record;
    expect(rec.lease_expires_at).toBeTruthy();

    // Release records a handoff and clears the custodian.
    const released = await custodyRelease(
      client,
      custodyReleaseSchema.parse({
        session_id: reg.session_id,
        worktree_path: "/work/custody-a",
        repo_head: "abc123",
        tracked_tree_state: "clean",
        untracked_inventory: [],
        unfinished_work: "wrapped up the demo",
        hazards: [],
        next_step: "commit the demo",
      })
    );
    expect(released.isError).toBeUndefined();
    expect(
      (released.structuredContent as { record: { state: string } }).record.state
    ).toBe("released");

    // Listing returns the released record.
    const listed = await custodyStatus(client, custodyStatusSchema.parse({}));
    expect(listed.structuredContent).toMatchObject({ status: "ok" });
    const records = (listed.structuredContent as { records: { state: string }[] }).records;
    expect(records).toHaveLength(1);
    expect(records[0]!.state).toBe("released");

    await client.shutdown();
  });

  it("requires a bound session for custody_claim like send_message", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await expect(
      custodyClaim(
        client,
        custodyClaimSchema.parse({
          worktree_path: "/work/custody-unregistered",
          repo_head: "abc123",
          tree_fingerprint: "fp-u",
          lease_seconds: 60,
        })
      )
    ).rejects.toThrow(/not registered/i);
    await client.shutdown();
  });
});
