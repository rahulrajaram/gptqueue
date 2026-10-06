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

const BIND_FIRST_START_SCRIPT = `
redis.call('SET', KEYS[1], ARGV[1], 'NX')
local raw = redis.call('GET', KEYS[1])
if not raw then return false end
local ok, mapping = pcall(cjson.decode, raw)
if ok and type(mapping) == 'table' and mapping.agent == ARGV[2] then redis.call('SREM', KEYS[2], ARGV[2]) end
return raw
`;

/**
 * Exact native continuity. The only envelopes ever moved are those a peer
 * sent to this sidecar's per-startup provisional name; they belong to the
 * bound mailbox (DL-2).
 */
export const restoreRuntimeMailbox = async (client: RedisClient, binding: RuntimeBinding): Promise<string> => {
  const validated = runtimeBindingSchema.parse(binding);
  const source = client.requireRegistered(), key = runtimeMailboxKey(validated);
  // COR-F5: write the mapping (NX) and, when it names this very name (first
  // start), clear the provisional mark in ONE step, so no crash can leave a
  // bound mailbox still marked for the provisional sweeper to retire.
  const raw = await client.adapterConnection.eval(BIND_FIRST_START_SCRIPT, 2, key, SESSION_KEYS.provisional,
    JSON.stringify({ agent: source, working_directory: validated.working_directory }), source) as string | null;
  if (!raw) throw new Error("Runtime mailbox mapping unavailable");
  const parsed = z.object({ agent: z.string().min(1), working_directory: z.string() }).strict().safeParse(JSON.parse(raw));
  // F8: accept `.`/`..` lexical aliases, matching binding validation and the
  // continuity plan checks in core/mailbox-continuity.ts.
  if (!parsed.success || resolve(parsed.data.working_directory) !== resolve(validated.working_directory)) throw new Error("Runtime mailbox mapping does not match binding");
  if (parsed.data.agent !== source) {
    try {
      await client.adoptIdentity(source, parsed.data.agent, key, raw, validated.runtime_id);
    } catch (error) {
      throw new RuntimeMailboxAdoptionRefused(error, parsed.data.agent);
    }
  }
  return client.requireRegistered();
};
