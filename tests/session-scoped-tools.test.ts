import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import {
  registerAgent,
  registerAgentSchema,
} from "../src/mcp-server/tools/register-agent.js";
import {
  sendMessage,
  sendMessageSchema,
} from "../src/mcp-server/tools/send-message.js";
import {
  receiveMessage,
  receiveMessageSchema,
} from "../src/mcp-server/tools/receive-message.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

function parseTextPayload(result: {
  content: readonly { type: "text"; text: string }[];
}): any {
  return JSON.parse(result.content[0]!.text);
}

describe("Session-scoped tools across fresh clients", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("sends and receives with session_id after transport-local state is lost", async () => {
    const registerSenderClient = new RedisClient(null, TEST_REDIS_URL);
    const registerReceiverClient = new RedisClient(null, TEST_REDIS_URL);

    const senderRegistration = parseTextPayload(
      await registerAgent(
        registerSenderClient,
        registerAgentSchema.parse({
          name: "stateless-sender",
          role: "publisher",
          description: "sender registered in one process",
        })
      )
    );
    const receiverRegistration = parseTextPayload(
      await registerAgent(
        registerReceiverClient,
        registerAgentSchema.parse({
          name: "stateless-receiver",
          role: "consumer",
          description: "receiver registered in one process",
        })
      )
    );

    await registerSenderClient.shutdown();
    await registerReceiverClient.shutdown();

    const sendClient = new RedisClient(null, TEST_REDIS_URL);
    const sendResult = parseTextPayload(
      await sendMessage(
        sendClient,
        sendMessageSchema.parse({
          session_id: senderRegistration.session_id,
          to: "stateless-receiver",
          content: "hello from a fresh client",
          type: "task",
        })
      )
    );
    expect(sendResult.status).toBe("sent");

    const receiveClient = new RedisClient(null, TEST_REDIS_URL);
    const received = parseTextPayload(
      await receiveMessage(
        receiveClient,
        receiveMessageSchema.parse({
          session_id: receiverRegistration.session_id,
          timeout: 2,
        })
      )
    );

    expect(received.payload.content).toBe("hello from a fresh client");
    expect(received.from).toBe("stateless-sender");
    expect(received.to).toBe("stateless-receiver");

    await sendClient.shutdown();
    await receiveClient.shutdown();
  });
});
