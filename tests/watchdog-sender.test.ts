/**
 * W4: the watchdog's sender is unforgeable by agents.
 * - Registration refuses the reserved name (src/core/session-store.ts, AGENT_NAME_RESERVED).
 * - The send tool has no caller-supplied `from`: the sender comes from the bound session
 *   (src/mcp-server/tools/send-message.ts, `from: sender` after bindSession).
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { afterAll, describe, expect, it } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SYSTEM_SENDER } from "../src/core/keys.js";
import { sendMessageSchema } from "../src/mcp-server/tools/send-message.js";
import { assertPrivateTestRedis } from "./helpers/redis-test-utils.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "watchdog-sender");

describe("W4: sender enforcement", () => {
  const client = new RedisClient(null, REDIS_URL);
  afterAll(async () => { await client.shutdown(); });

  it("refuses to register the reserved system sender name", async () => {
    await expect(client.register("both", SYSTEM_SENDER)).rejects.toMatchObject({ code: "AGENT_NAME_RESERVED" });
  });

  it("send_message accepts no caller-supplied from", () => {
    expect(Object.keys(sendMessageSchema.shape)).not.toContain("from");
    const parsed = sendMessageSchema.safeParse({ to: "x", content: "y", type: "task", from: SYSTEM_SENDER });
    if (parsed.success) expect("from" in parsed.data).toBe(false);
  });
});
