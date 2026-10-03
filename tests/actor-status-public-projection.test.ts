import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { createHash, randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { actorStatus, actorStatusSchema } from "../src/mcp-server/tools/actor-status.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const T0 = "2030-01-01T00:00:00.000Z";

// Mirrors the shared session tag (http.ts sessionLogTag): public reads carry
// a tag, never the bearer session id.
const tag = (sid: string): string =>
  `sid-sha256:${createHash("sha256").update(sid).digest("hex").slice(0, 12)}`;

const textOf = (result: Awaited<ReturnType<typeof actorStatus>>): string =>
  (result.content as Array<{ text: string }>).map((part) => part.text).join("\n");

/**
 * actor_status is readable by any participant, even before registration. A
 * session id is a bearer credential for its agent, so the public projection
 * must never carry another participant's raw id (review PR6 F1).
 */
describe("actor_status public projection", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  const client = () => {
    const created = new RedisClient(null, TEST_REDIS_URL);
    clients.push(created);
    return created;
  };

  const registerDurable = async (owner: RedisClient, actorId: string, sessionId: string) => {
    const registered = await owner.actorDirectory.register({
      profile_input: {
        actor_id: actorId,
        alias: actorId,
        capabilities: [],
        workspace_root: "/workspace",
        working_directory: "/workspace",
        runtime: "manual",
        activation_policy: { mode: "store_only" },
        max_concurrency: 1,
      },
      launch: null,
      registered_by: sessionId,
      registered_at: T0,
    });
    expect(registered.ok).toBe(true);
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await Promise.all(clients.splice(0).map((created) => created.shutdown()));
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("tags a live actor's runtime ids for a different participant, keeping the shape", async () => {
    const actorId = `pr6-live-${randomUUID()}`;
    const victim = client();
    const { session_id: victimSession } = await victim.register("both", actorId);
    await registerDurable(victim, actorId, victimSession);

    const observer = client();
    await observer.register("both", `pr6-observer-${randomUUID()}`);
    const result = await actorStatus(observer, actorStatusSchema.parse({ actor_id: actorId }));

    expect(result.isError).toBeUndefined();
    expect(JSON.stringify(result.structuredContent)).not.toContain(victimSession);
    expect(textOf(result)).not.toContain(victimSession);
    expect(result.structuredContent).toMatchObject({
      status: "ok",
      presence: "idle",
      runtime: {
        incarnation_id: tag(victimSession),
        session_id: tag(victimSession),
        lease_id: tag(victimSession),
        workload: "idle",
      },
    });
  });

  it("tags the waking sender's session on an outstanding wake lease, even before registration", async () => {
    const actorId = `pr6-wake-${randomUUID()}`;
    const owner = client();
    const { session_id: ownerSession } = await owner.register("both", `pr6-owner-${randomUUID()}`);
    await registerDurable(owner, actorId, ownerSession);

    const sender = client();
    const { session_id: senderSession } = await sender.register("both", `pr6-sender-${randomUUID()}`);
    const acquired = await sender.wakeLease.acquire({
      actor_id: actorId,
      issued_by_session: senderSession,
      lease_seconds: 300,
      now: new Date().toISOString(),
    });
    if (!acquired.ok) throw new Error("expected a wake lease");

    const anonymous = client();
    const result = await actorStatus(anonymous, actorStatusSchema.parse({ actor_id: actorId }));

    for (const secret of [ownerSession, senderSession]) {
      expect(JSON.stringify(result.structuredContent)).not.toContain(secret);
      expect(textOf(result)).not.toContain(secret);
    }
    expect(result.structuredContent).toMatchObject({
      status: "ok",
      presence: "starting",
      wake_lease: {
        lease_id: acquired.lease.lease_id,
        actor_id: actorId,
        issued_by_session: tag(senderSession),
      },
    });
  });
});
