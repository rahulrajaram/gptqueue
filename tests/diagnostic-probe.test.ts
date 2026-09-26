/**
 * F3 regression suite: the exact native probe rejection paths of
 * get_agent_details (src/registered-shell/diagnostic-tools.ts). Every
 * mismatch class — wrong agent, wrong runtime id, wrong epoch, malformed
 * status, RPC error, timeout — must classify as NOT activation-ready with
 * explicit evidence, and only an exact identity match may report ready.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Redis } from "ioredis";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerDiagnosticTools } from "../src/registered-shell/diagnostic-tools.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

// Mock the probe transport; capture instances for call assertions and stage
// the next response globally (a fresh client is constructed per probe).
const probe = vi.hoisted(() => ({
  instances: [] as Array<{
    request: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
  }>,
  nextResponse: undefined as unknown,
}));
vi.mock("../src/registered-shell/codex-socket.js", () => ({
  CodexSocketClient: class {
    request = vi.fn(async () => {
      if (probe.nextResponse instanceof Error) throw probe.nextResponse;
      return probe.nextResponse;
    });
    close = vi.fn(async () => {});
    constructor() {
      probe.instances.push(this as never);
    }
  },
}));

type Handler = (params: Record<string, unknown>) => Promise<unknown>;
interface CapturedServer {
  handlers: Map<string, Handler>;
}

const captureTools = (client: RedisClient): CapturedServer => {
  const handlers = new Map<string, Handler>();
  const fake = {
    // Mirrors the SDK's tool() overloads: the callback is the trailing
    // function argument, with an optional annotations object before it.
    tool: (name: string, ...rest: unknown[]) => {
      const handler = rest.find((candidate) => typeof candidate === "function");
      handlers.set(name, handler as Handler);
    },
  };
  registerDiagnosticTools(
    fake as unknown as McpServer,
    client,
    { status: () => ({ activation_ready: false }) } as never
  );
  return { handlers };
};

describe("get_agent_details exact native probe rejection (F3)", () => {
  let redis: Redis;
  let client: RedisClient;
  let server: CapturedServer;
  const target = "probe-target";

  const seedTarget = async (
    binding: Record<string, unknown> = {
      client: "codex",
      runtime_id: "r-1",
      epoch: "e-1",
      working_directory: "/workspace",
    }
  ): Promise<void> => {
    await redis.hset(
      SESSION_KEYS.registry,
      target,
      JSON.stringify({ name: target, role: "both", registered_at: new Date().toISOString() })
    );
    if (binding !== null) {
      await redis.set(`gptq:runtime-binding:${target}`, JSON.stringify(binding));
    }
  };

  const probeDetails = async () => {
    const result = (await server.handlers.get("get_agent_details")!({
      agent: target,
      probe: true,
    })) as { structuredContent: Record<string, unknown> };
    return result.structuredContent;
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "probe-caller");
    server = captureTools(client);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await client.shutdown();
    await redis.quit();
  });

  it("reports ready only on an exact identity match (agent + runtime_id + epoch)", async () => {
    await seedTarget();
    probe.nextResponse = {
      isError: false,
      structuredContent: {
        agent: target,
        activation_ready: true,
        runtime: { runtime_id: "r-1", epoch: "e-1" },
      },
    };
    const result = await probeDetails();
    expect(result.activation_ready).toBe(true);
    expect(result.readiness).toBe("ready");
    expect(result.readiness_evidence).toBe("exact_native_probe");
    // The probe used the exact RPC contract against the bound runtime.
    const rpc = probe.instances.at(-1)!;
    expect(rpc.request).toHaveBeenCalledWith(
      "mcpServer/tool/call",
      {
        threadId: "r-1",
        server: "gptqueue-shared",
        tool: "get_runtime_status",
        arguments: {},
      },
      expect.any(AbortSignal)
    );
  });

  it.each([
    {
      name: "wrong agent",
      response: { isError: false, structuredContent: { agent: "someone-else", activation_ready: true, runtime: { runtime_id: "r-1", epoch: "e-1" } } },
    },
    {
      name: "wrong runtime id",
      response: { isError: false, structuredContent: { agent: target, activation_ready: true, runtime: { runtime_id: "other-runtime", epoch: "e-1" } } },
    },
    {
      name: "wrong epoch",
      response: { isError: false, structuredContent: { agent: target, activation_ready: true, runtime: { runtime_id: "r-1", epoch: "other-epoch" } } },
    },
    {
      name: "not activation_ready",
      response: { isError: false, structuredContent: { agent: target, activation_ready: false, runtime: { runtime_id: "r-1", epoch: "e-1" } } },
    },
    {
      name: "isError response",
      response: { isError: true, structuredContent: { agent: target, activation_ready: true, runtime: { runtime_id: "r-1", epoch: "e-1" } } },
    },
    {
      name: "malformed status (no structuredContent)",
      response: { isError: false },
    },
    {
      name: "malformed status (no runtime object)",
      response: { isError: false, structuredContent: { agent: target, activation_ready: true } },
    },
  ])("refuses readiness for $name", async ({ response }) => {
    await seedTarget();
    probe.nextResponse = response;
    const result = await probeDetails();
    expect(result.activation_ready).toBe(false);
    expect(result.readiness_evidence).toBe("exact_native_probe");
  });

  it("classifies an RPC error as probe_unavailable with activation_ready null", async () => {
    await seedTarget();
    probe.nextResponse = new Error("socket failed");
    const result = await probeDetails();
    expect(result.activation_ready).toBeNull();
    expect(result.readiness_evidence).toBe("probe_unavailable");
  });

  it("classifies a probe timeout as probe_unavailable with activation_ready null", async () => {
    await seedTarget();
    probe.nextResponse = new Error("timeout");
    const result = await probeDetails();
    expect(result.activation_ready).toBeNull();
    expect(result.readiness_evidence).toBe("probe_unavailable");
  });

  it("does not probe a non-codex binding and reports lease observation only", async () => {
    await seedTarget({ client: "pi", runtime_id: "r-2", epoch: "e-2", working_directory: "/workspace" });
    const before = probe.instances.length;
    const result = await probeDetails();
    expect(probe.instances.length).toBe(before);
    expect(result.activation_ready).toBe(false);
    expect(result.readiness_evidence).toBe("lease_observation_only");
  });

  it("does not probe when probe is not requested", async () => {
    await seedTarget();
    const before = probe.instances.length;
    const result = (await server.handlers.get("get_agent_details")!({
      agent: target,
    })) as { structuredContent: Record<string, unknown> };
    expect(probe.instances.length).toBe(before);
    expect(result.structuredContent.readiness_evidence).toBe("lease_observation_only");
  });
});
