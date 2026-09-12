import type { RedisClient } from "../mcp-server/redis-client.js";
import { runtimeBindingSchema, type RuntimeBinding } from "./runtime.js";
import { runtimeMailboxKey } from "../core/mailbox-continuity.js";
import { z } from "zod";
import { resolve } from "node:path";

/** Exact native continuity; never move queued envelopes into another address. */
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
  if (parsed.data.agent !== source) await client.adoptIdentity(source, parsed.data.agent, key, raw, validated.runtime_id);
  return client.requireRegistered();
};
