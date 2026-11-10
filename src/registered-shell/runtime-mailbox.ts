import type { RedisClient } from "../mcp-server/redis-client.js";
import { runtimeBindingSchema, type RuntimeBinding } from "./runtime.js";
import { runtimeMailboxKey } from "../core/mailbox-continuity.js";
import { SESSION_KEYS } from "../core/keys.js";
import { AdoptionRefusedError, type AdoptionRefusalReason, type AdoptionRefusalSignal } from "../core/adoption-refusal.js";
import { z } from "zod";
import { resolve } from "node:path";

/**
 * Adoption of the bound mailbox `target` did not happen, and this client still
 * holds its provisional name. This base class alone means an infrastructure or
 * protocol failure (Redis, an unexpected reply), never a classified refusal.
 */
export class RuntimeMailboxAdoptionError extends Error {
  constructor(readonly cause: unknown, readonly target: string) {
    super(cause instanceof Error ? cause.message : String(cause));
    this.name = "RuntimeMailboxAdoptionError";
  }
}

/** The adoption guard refused: its classified reason, first blocking signal and code. */
export class RuntimeMailboxAdoptionRefused extends RuntimeMailboxAdoptionError {
  readonly reason: AdoptionRefusalReason;
  readonly signal: AdoptionRefusalSignal | null;
  readonly code: number;
  /** Redis ms when a recheck may be worthwhile: a hint, never a promise. */
  readonly recheckAfterMs: number | null;
  constructor(cause: AdoptionRefusedError, target: string) {
    super(cause, target);
    this.name = "RuntimeMailboxAdoptionRefused";
    this.reason = cause.refusal.reason;
    this.signal = cause.refusal.signal;
    this.code = cause.refusal.code;
    this.recheckAfterMs = cause.refusal.recheckAfterMs;
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
export const restoreRuntimeMailbox = async (client: RedisClient, binding: RuntimeBinding, opts: Readonly<{ fixedName?: boolean }> = {}): Promise<string> => {
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
    // A fixed role name (GPTQ_AGENT_NAME) IS the address senders use: adopting
    // another mailbox would move the alias off its own name and, on the Lua
    // success path, delete its registry entry. Keep the fixed name instead.
    if (opts.fixedName) return client.requireRegistered();
    try {
      await client.adoptIdentity(source, parsed.data.agent, key, raw, validated.runtime_id);
    } catch (error) {
      throw error instanceof AdoptionRefusedError
        ? new RuntimeMailboxAdoptionRefused(error, parsed.data.agent)
        : new RuntimeMailboxAdoptionError(error, parsed.data.agent);
    }
  }
  return client.requireRegistered();
};
