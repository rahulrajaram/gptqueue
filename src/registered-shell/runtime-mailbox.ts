import type { RedisClient } from "../mcp-server/redis-client.js";
import { runtimeBindingSchema, type RuntimeBinding } from "./runtime.js";
import { runtimeMailboxKey } from "../core/mailbox-continuity.js";
import { SESSION_KEYS } from "../core/keys.js";
import { z } from "zod";
import { resolve } from "node:path";

/** Adoption of the bound mailbox was refused; `target` is that mailbox. */
export class RuntimeMailboxAdoptionRefused extends Error {
  constructor(readonly cause: unknown, readonly target: string) {
    super(cause instanceof Error ? cause.message : String(cause));
    this.name = "RuntimeMailboxAdoptionRefused";
  }
}

/**
 * Exact native continuity. The only envelopes ever moved are those a peer
 * sent to this sidecar's per-startup provisional name; they belong to the
 * bound mailbox (DL-2).
 */
export const restoreRuntimeMailbox = async (client: RedisClient, binding: RuntimeBinding): Promise<string> => {
  const validated = runtimeBindingSchema.parse(binding);
  const source = client.requireRegistered(), key = runtimeMailboxKey(validated);
  await client.adapterConnection.set(key, JSON.stringify({ agent: source, working_directory: validated.working_directory }), "NX");
  const raw = await client.adapterConnection.get(key);
  if (!raw) throw new Error("Runtime mailbox mapping unavailable");
  const parsed = z.object({ agent: z.string().min(1), working_directory: z.string() }).strict().safeParse(JSON.parse(raw));
  // F8: accept `.`/`..` lexical aliases, matching binding validation and the
  // continuity plan checks in core/mailbox-continuity.ts.
  if (!parsed.success || resolve(parsed.data.working_directory) !== resolve(validated.working_directory)) throw new Error("Runtime mailbox mapping does not match binding");
  // This name IS the bound mailbox (first start): it is no longer provisional.
  if (parsed.data.agent === source) await client.adapterConnection.srem(SESSION_KEYS.provisional, source);
  if (parsed.data.agent !== source) {
    try {
      await client.adoptIdentity(source, parsed.data.agent, key, raw, validated.runtime_id);
    } catch (error) {
      throw new RuntimeMailboxAdoptionRefused(error, parsed.data.agent);
    }
  }
  return client.requireRegistered();
};
