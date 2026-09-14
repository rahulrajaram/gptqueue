import { Redis } from "ioredis";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import {
  createOpenCodeBackend,
  opencodeAgentName,
} from "../src/registered-shell/opencode-backend.js";
import type { OpenCodeRuntimePort } from "../src/registered-shell/opencode-runtime.js";
import type { OpenCodeSessionBackend } from "../src/registered-shell/opencode-sessions.js";

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const directory = "/tmp/gptqueue-opencode-backend-test";
const liveBackends: OpenCodeSessionBackend[] = [];
const redis = new Redis(REDIS_URL);

const portFor = (sessionID: string): OpenCodeRuntimePort => ({
  readIdentity: vi.fn(async () => ({ runtime_id: sessionID, working_directory: directory })),
  status: vi.fn(async () => "idle" as const),
  history: vi.fn(async () => []),
  promptAsync: vi.fn(async () => undefined),
  close: vi.fn(async () => undefined),
});

const payload = (value: unknown): Record<string, unknown> => {
  const structured = (value as { structuredContent?: unknown }).structuredContent;
  if (!structured || typeof structured !== "object" || Array.isArray(structured)) {
    throw new Error(`Expected structured tool payload: ${JSON.stringify(value)}`);
  }
  return structured as Record<string, unknown>;
};

const cleanupAgent = async (name: string): Promise<void> => {
  await redis.hdel(SESSION_KEYS.registry, name);
  await redis.del(
    SESSION_KEYS.queue(name),
    SESSION_KEYS.agent(name),
    SESSION_KEYS.mailboxMeta(name),
    SESSION_KEYS.heartbeat(name),
    SESSION_KEYS.agentSessions(name),
    `gptq:idempotency:${name}`,
  );
};

afterEach(async () => {
  await Promise.all(liveBackends.splice(0).map((backend) => backend.close()));
});

afterAll(async () => {
  await redis.quit();
});

describe("OpenCode backend", () => {
  it("keeps parent and child registrations independent across bound MCP peers", async () => {
    const parentID = `backend-parent-${Date.now()}`;
    const childID = `backend-child-${Date.now()}`;
    const parentName = opencodeAgentName(parentID);
    const childName = opencodeAgentName(childID);
    let parent: OpenCodeSessionBackend | undefined;
    let child: OpenCodeSessionBackend | undefined;
    try {
      parent = await createOpenCodeBackend(
        { sessionID: parentID, directory },
        { redisUrl: REDIS_URL, runtimePort: portFor(parentID), dispatcherOptions: { intervalMs: 20 } },
      );
      child = await createOpenCodeBackend(
        { sessionID: childID, directory },
        { redisUrl: REDIS_URL, runtimePort: portFor(childID), dispatcherOptions: { intervalMs: 20 } },
      );
      liveBackends.push(parent, child);

      const listed = payload(await parent.callTool("list_agents", {}));
      expect(listed.agents).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: parentName }),
          expect.objectContaining({ name: childName }),
        ]),
      );

      const sent = payload(await parent.callTool("send_message", {
        to: childName,
        content: "parent-to-child",
        type: "task",
      }));
      expect(sent.status).toBe("sent");
      const received = payload(await child.callTool("receive_message", { timeout: 1 }));
      expect((received.message as { payload: { content: string } }).payload.content).toBe("parent-to-child");

      const reply = payload(await child.callTool("send_message", {
        to: parentName,
        content: "child-to-parent",
        type: "result",
        in_reply_to: sent.message_id,
      }));
      expect(reply.status).toBe("sent");
      const parentReceived = payload(await parent.callTool("receive_message", { timeout: 1 }));
      expect((parentReceived.message as { payload: { content: string } }).payload.content).toBe("child-to-parent");

      const task = payload(await parent.callTool("send_message", {
        to: childName,
        content: "claim-me",
        type: "task",
      }));
      expect(task.status).toBe("sent");
      const claimed = payload(await child.callTool("claim_tasks", { max_batch: 1, ttl_seconds: 30 }));
      const claim = claimed.claim as { claim_id: string; tasks: readonly unknown[] };
      expect(claim.claim_id).toBeTruthy();
      expect(claim.tasks).toHaveLength(1);
      const acknowledged = payload(await child.callTool("acknowledge_tasks", { claim_id: claim.claim_id }));
      expect(acknowledged.status).toBe("ok");

      expect(await redis.hexists(SESSION_KEYS.registry, parentName)).toBe(1);
      expect(await redis.hexists(SESSION_KEYS.registry, childName)).toBe(1);
      expect(await redis.scard(SESSION_KEYS.agentSessions(parentName))).toBe(1);
      expect(await redis.scard(SESSION_KEYS.agentSessions(childName))).toBe(1);
    } finally {
      await Promise.all([parent?.close(), child?.close()]);
      await cleanupAgent(parentName);
      await cleanupAgent(childName);
    }
  });

  it("closes the native runtime when dispatcher startup loses the lease", async () => {
    const sessionID = `backend-startup-${Date.now()}`;
    const name = opencodeAgentName(sessionID);
    const firstPort = portFor(sessionID);
    const secondPort = portFor(sessionID);
    let first: OpenCodeSessionBackend | undefined;
    try {
      first = await createOpenCodeBackend(
        { sessionID, directory },
        { redisUrl: REDIS_URL, runtimePort: firstPort, dispatcherOptions: { intervalMs: 20 } },
      );
      const ready = payload(await first.callTool("get_runtime_status", {}));
      expect(ready.activation_ready).toBe(true);
      await expect(createOpenCodeBackend(
        { sessionID, directory },
        { redisUrl: REDIS_URL, runtimePort: secondPort, dispatcherOptions: { intervalMs: 20 } },
      )).rejects.toThrow(/live dispatcher/);
      expect(secondPort.close).toHaveBeenCalledTimes(1);
    } finally {
      await first?.close();
      await cleanupAgent(name);
    }
  });
});
