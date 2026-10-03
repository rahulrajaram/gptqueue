import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { createHash, randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { custodyClaim, custodyClaimSchema } from "../src/mcp-server/tools/custody-claim.js";
import { custodyStatus, custodyStatusSchema } from "../src/mcp-server/tools/custody-status.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";

// Mirrors the shared session tag (src/core/session-tag.ts).
const tag = (sid: string): string =>
  `sid-sha256:${createHash("sha256").update(sid).digest("hex").slice(0, 12)}`;

const textOf = (result: { content: unknown }): string =>
  (result.content as Array<{ text: string }>).map((part) => part.text).join("\n");

/**
 * custody_status is readable by any participant, even before registration,
 * and a held record names its custodian's session, a bearer credential. The
 * public result carries only the tag; the holder's own claim result is raw.
 */
describe("custody_status public projection", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  const client = () => {
    const created = new RedisClient(null, TEST_REDIS_URL);
    clients.push(created);
    return created;
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

  it("tags the custodian's session for a different participant, in one record and in the list", async () => {
    const holder = client();
    const holderName = `custody-holder-${randomUUID()}`;
    const { session_id: holderSession } = await holder.register("both", holderName);
    const worktree_path = `/worktrees/custody-${randomUUID()}`;
    const claimed = await custodyClaim(
      holder,
      custodyClaimSchema.parse({ worktree_path, repo_head: "a".repeat(40), tree_fingerprint: "fp", lease_seconds: 300 })
    );
    // The holder's own claim result stays raw.
    expect(claimed.structuredContent).toMatchObject({
      status: "ok",
      record: { custodian: { actor_name: holderName, session_id: holderSession } },
    });

    const observer = client();
    for (const result of [
      await custodyStatus(observer, custodyStatusSchema.parse({ worktree_path })),
      await custodyStatus(observer, custodyStatusSchema.parse({})),
    ]) {
      expect(result.isError).toBeUndefined();
      expect(JSON.stringify(result.structuredContent)).not.toContain(holderSession);
      expect(textOf(result)).not.toContain(holderSession);
    }

    const one = await custodyStatus(observer, custodyStatusSchema.parse({ worktree_path }));
    expect(one.structuredContent).toMatchObject({
      status: "ok",
      record: {
        state: "held",
        worktree: { worktree_path },
        custodian: { actor_name: holderName, session_id: tag(holderSession) },
      },
    });
    const all = await custodyStatus(observer, custodyStatusSchema.parse({}));
    expect(all.structuredContent).toMatchObject({
      status: "ok",
      records: [{ custodian: { actor_name: holderName, session_id: tag(holderSession) } }],
    });
  });
});
