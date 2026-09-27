import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { z } from "zod";
import type { Redis } from "ioredis";
import { runtimeBindingSchema, type RuntimeBinding } from "../registered-shell/runtime.js";
import { occupancyGuardLua } from "./occupancy-guard.js";
import { SESSION_KEYS } from "./keys.js";

/**
 * F8: lexical path equivalence. Runtime bindings accept `.`/`..` aliases
 * (registered-shell/runtime.ts); every working-directory comparison here must
 * accept the same aliases instead of demanding byte-identical strings, or a
 * valid binding fails continuity restore.
 */
const sameDirectory = (a: string, b: string): boolean => resolve(a) === resolve(b);

export const runtimeMailboxKey = (binding: RuntimeBinding): string =>
  SESSION_KEYS.runtimeMailbox(binding.client, binding.runtime_id);
const fingerprint = (raw: string | null): string => createHash("sha256").update(raw ?? "<missing>").digest("hex");
const namespace = (redis: Redis): string => fingerprint(JSON.stringify([redis.options.host, redis.options.port, redis.options.path, redis.options.db]));
const continuityPlanSchema = z.object({
  binding: runtimeBindingSchema, source: z.string().min(1).nullable(), target: z.string().min(1).max(500),
  mappingKey: z.string().min(1), expectedMapping: z.string().nullable(), namespace: z.string(),
  sourceFingerprint: z.string(), targetFingerprint: z.string(), legacy_adoption: z.boolean(),
}).strict();
export type ContinuityPlan = Readonly<z.infer<typeof continuityPlanSchema>>;

/** Read-only plan. Explicit legacy adoption is an operator action, never cwd inference. */
export const prepareContinuity = async (redis: Redis, binding: RuntimeBinding, target: string,
  options: Readonly<{ allowLegacy?: boolean }> = {}): Promise<ContinuityPlan> => {
  const validated = runtimeBindingSchema.parse(binding);
  const key = runtimeMailboxKey(validated), raw = await redis.get(key);
  if (!raw && !options.allowLegacy) throw new Error("Continuity provenance missing; explicit legacy adoption required");
  const mapping = raw ? z.object({ agent: z.string().min(1), working_directory: z.string() }).strict().parse(JSON.parse(raw)) : null;
  if (mapping && !sameDirectory(mapping.working_directory, validated.working_directory)) throw new Error("Runtime mailbox mapping does not match binding");
  const sourceRaw = mapping ? await redis.hget(SESSION_KEYS.registry, mapping.agent) : null;
  const targetRaw = await redis.hget(SESSION_KEYS.registry, target);
  if (!targetRaw || (mapping && !sourceRaw)) throw new Error("Continuity registry metadata unavailable");
  return Object.freeze(continuityPlanSchema.parse({ binding: validated, source: mapping?.agent ?? null, target,
    mappingKey: key, expectedMapping: raw, namespace: namespace(redis), sourceFingerprint: fingerprint(sourceRaw),
    targetFingerprint: fingerprint(targetRaw), legacy_adoption: !mapping }));
};

/** Local operator-only CAS: preserves queues, addresses, envelopes and correlations. */
export const applyContinuity = async (redis: Redis, plan: ContinuityPlan): Promise<"applied" | "idempotent"> => {
  const p = continuityPlanSchema.parse(plan);
  if (p.mappingKey !== runtimeMailboxKey(p.binding) || p.namespace !== namespace(redis)) throw new Error("Continuity namespace or mapping mismatch");
  const plannedMapping = p.expectedMapping ? z.object({ agent: z.string(), working_directory: z.string() }).strict().parse(JSON.parse(p.expectedMapping)) : null;
  if ((plannedMapping?.agent ?? null) !== p.source || (plannedMapping && !sameDirectory(plannedMapping.working_directory, p.binding.working_directory)) ||
      (!plannedMapping) !== p.legacy_adoption) throw new Error("Inconsistent continuity plan");
  const desired = JSON.stringify({ agent: p.target, working_directory: p.binding.working_directory });
  const [current, sourceRaw, targetRaw] = await Promise.all([redis.get(p.mappingKey),
    p.source ? redis.hget(SESSION_KEYS.registry, p.source) : Promise.resolve(null), redis.hget(SESSION_KEYS.registry, p.target)]);
  // F6: idempotent replay must revalidate the recorded target fingerprint,
  // not just mapping equality — a target registry entry deleted/recreated or
  // reclaimed between applies must not yield a false success. A changed
  // fingerprint falls through to the mismatch throw below.
  if (current === desired && targetRaw && fingerprint(targetRaw) === p.targetFingerprint) return "idempotent";
  if (current !== p.expectedMapping || fingerprint(sourceRaw) !== p.sourceFingerprint || !targetRaw || fingerprint(targetRaw) !== p.targetFingerprint)
    throw new Error("Continuity fingerprints or mapping changed");
  if ((!p.source) !== p.legacy_adoption) throw new Error("Invalid legacy adoption plan");
  const wrapperKey = SESSION_KEYS.wrapperClaim;
  // D5: the occupied() guard is generated from the shared signal table
  // (core/occupancy-guard.ts) — identical to adoptIdentity's target guard.
  const result = await redis.eval(`
    ${occupancyGuardLua("occupied")}
    if (redis.call('GET', KEYS[1]) or '') ~= ARGV[1] then return -1 end
    if (redis.call('HGET', KEYS[2], ARGV[2]) or '') ~= ARGV[3] then return -2 end
    if ARGV[4] ~= '' and (redis.call('HGET', KEYS[2], ARGV[4]) or '') ~= ARGV[5] then return -3 end
    if occupied(ARGV[2], KEYS[3]) then return -4 end
    if ARGV[4] ~= '' and ARGV[4] ~= ARGV[2] then
      if occupied(ARGV[4], KEYS[4]) or redis.call('LLEN','gptq:q:'..ARGV[4]) > 0 or redis.call('EXISTS','gptq:outbound-activity:'..ARGV[4]) == 1 then return -5 end
    end
    redis.call('SET',KEYS[1],ARGV[6])
    redis.call('XADD',KEYS[5],'MAXLEN','~',1000,'*','event','operator_continuity','target',ARGV[2],'source',ARGV[4],'runtime_id',ARGV[7])
    return 1
  `, 5, p.mappingKey, SESSION_KEYS.registry, wrapperKey(p.target), wrapperKey(p.source ?? p.target), SESSION_KEYS.continuityAudit,
  p.expectedMapping ?? "", p.target, targetRaw, p.source ?? "", sourceRaw ?? "", desired, p.binding.runtime_id);
  if (result !== 1) throw new Error(`Continuity apply refused (${result}): mapping changed, live owner, claims or source activity`);
  return "applied";
};
