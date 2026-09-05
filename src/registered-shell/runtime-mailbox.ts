import { createHash } from "node:crypto";
import type { RedisClient } from "../mcp-server/redis-client.js";
import { SESSION_KEYS } from "../core/keys.js";
import { discoveryMetadata } from "../core/agent-discovery.js";
import type { RuntimeBinding } from "./runtime.js";

/** Preserve the public queue UUID and backlog when the same native session reconnects. */
export const restoreRuntimeMailbox = async (client: RedisClient, binding: RuntimeBinding): Promise<string> => {
  const redis = client.adapterConnection;
  const key = `gptq:runtime-mailbox:${createHash("sha256").update(JSON.stringify([binding.client, binding.runtime_id])).digest("hex")}`;
  const proposed = JSON.stringify({ agent: client.requireRegistered(), working_directory: binding.working_directory });
  await redis.set(key, proposed, "NX");
  const raw = await redis.get(key);
  if (!raw) throw new Error("Runtime mailbox mapping unavailable");
  const mapping = JSON.parse(raw) as { agent?: unknown; working_directory?: unknown };
  if (typeof mapping.agent !== "string" || mapping.working_directory !== binding.working_directory) {
    throw new Error("Runtime mailbox mapping does not match binding");
  }
  if (mapping.agent === client.agentName) return mapping.agent;
  const stored = await redis.hget(SESSION_KEYS.registry, mapping.agent);
  if (!stored) throw new Error("Runtime mailbox registration unavailable");
  const registry = JSON.parse(stored) as Record<string, unknown>;
  const metadata = discoveryMetadata(mapping.agent, registry.metadata);
  await client.register("both", mapping.agent,
    typeof registry.description === "string" ? registry.description : undefined, metadata);
  return mapping.agent;
};
