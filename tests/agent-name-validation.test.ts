/**
 * SEC-1 regression suite: agent names are validated once at the core
 * boundary (SessionStore.createSession and the send-time recipient
 * resolution, resolveRegistered) and mirrored in the tool schemas
 * (register_agent `name`, send_message `to`).
 *
 * Before the fix, any string was accepted — only the exact reserved name
 * was refused — so an empty, whitespace or ':'-containing name flowed into
 * every `gptq:*:<name>` Redis key and the Lua key expressions built as
 * `'gptq:q:' .. name`, while the identical string was refused as a durable
 * actor_id. The rule here is deliberately NOT the 64-char IDENTITY_CHARSET
 * bound of durable actors: 427 of 725 live registered names are longer
 * than 64 (max 87) and all fit [A-Za-z0-9._-], so every live name must
 * still be accepted.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { SessionStore } from "../src/core/session-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { registerAgentSchema } from "../src/mcp-server/tools/register-agent.js";
import { sendMessageSchema } from "../src/mcp-server/tools/send-message.js";

const TEST_REDIS_URL = testRedisUrl();

/** 87 characters with dashes: the longest live registered name shape. */
const dashed87 = "ab-".repeat(29);

describe("SessionStore agent-name validation (SEC-1)", () => {
  let redis: Redis;
  let store: SessionStore;

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    store = new SessionStore(redis);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("rejects an empty name with AGENT_NAME_INVALID", async () => {
    await expect(store.createSession("", "both")).rejects.toMatchObject({ code: "AGENT_NAME_INVALID" });
    expect(await redis.exists(SESSION_KEYS.queue(""), SESSION_KEYS.agentSessions(""))).toBe(0);
  });

  it("rejects a whitespace name with AGENT_NAME_INVALID", async () => {
    await expect(store.createSession("agent one", "both")).rejects.toMatchObject({ code: "AGENT_NAME_INVALID" });
    await expect(store.createSession(" ", "both")).rejects.toMatchObject({ code: "AGENT_NAME_INVALID" });
  });

  it("rejects a ':'-containing name (key-namespace separator) with AGENT_NAME_INVALID", async () => {
    await expect(store.createSession("agent:one", "both")).rejects.toMatchObject({ code: "AGENT_NAME_INVALID" });
  });

  it("rejects a 129-character name with AGENT_NAME_INVALID", async () => {
    await expect(store.createSession("a".repeat(129), "both")).rejects.toMatchObject({ code: "AGENT_NAME_INVALID" });
  });

  it("still accepts an 87-character dashed name: the longest live registered shape must keep registering", async () => {
    const session = await store.createSession(dashed87, "both");
    expect(session.agent_name).toBe(dashed87);
    expect(await redis.exists(SESSION_KEYS.agentSessions(dashed87))).toBe(1);
    expect(await store.getPresence(dashed87)).toMatchObject({ agent_name: dashed87, online: true });
  });

  it("still reserves gptqueue-system with AGENT_NAME_RESERVED", async () => {
    await expect(store.createSession("gptqueue-system", "both")).rejects.toMatchObject({ code: "AGENT_NAME_RESERVED" });
  });

  it("resolveRegistered refuses an invalid name even when a legacy registry field holds it", async () => {
    // A pre-validation (or hand-written) registry field must not make an
    // invalid name deliverable at send time.
    await redis.hset(SESSION_KEYS.registry, "legacy:invalid", "{}");
    await redis.hset(SESSION_KEYS.registry, "", "{}");
    await expect(store.resolveRegistered("legacy:invalid")).resolves.toBe(false);
    await expect(store.resolveRegistered("")).resolves.toBe(false);
  });

  it("resolveRegistered still resolves a valid registered name", async () => {
    await redis.hset(SESSION_KEYS.registry, "valid-agent", "{}");
    await expect(store.resolveRegistered("valid-agent")).resolves.toBe(true);
  });
});

describe("agent-name tool schemas (SEC-1)", () => {
  it("register_agent rejects empty, whitespace, ':' and 129-character names at the schema", () => {
    for (const name of ["", " ", "agent one", "agent:one", "a".repeat(129)]) {
      expect(registerAgentSchema.safeParse({ name, role: "both", description: "d" }).success).toBe(false);
    }
  });

  it("register_agent accepts an 87-character dashed name", () => {
    expect(registerAgentSchema.safeParse({ name: dashed87, role: "both", description: "d" }).success).toBe(true);
  });

  it("send_message rejects an invalid `to` at the schema and accepts a valid one", () => {
    for (const to of ["", " ", "to:name", "a".repeat(129)]) {
      expect(sendMessageSchema.safeParse({ to, content: "hi" }).success).toBe(false);
    }
    expect(sendMessageSchema.safeParse({ to: dashed87, content: "hi" }).success).toBe(true);
  });
});
