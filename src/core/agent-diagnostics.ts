import type { Redis } from "ioredis";
import { resolve } from "path";
import { SESSION_KEYS, CLAIM_KEYS, DLQ_KEYS } from "./keys.js";
import { SessionStore } from "./session-store.js";
import { discoveryRecord, type AgentDiscoveryRecord } from "./agent-discovery.js";
import { z } from "zod";
import { runtimeBindingSchema } from "./runtime-binding.js";

export type AgentKind = "controller" | "worker" | "interactive" | "unknown";
export type ActivationReadiness = "unknown_legacy" | "offline" | "unbound" | "bound_unverified" | "ready";
export type DeliveryStatus = "queued" | "claimed" | "claim_expired" | "dead_lettered" | "acknowledged" | "unknown_history";

export type DiagnosticsDetails = Readonly<{
  name: string; discovery: AgentDiscoveryRecord | null; profile: Readonly<Record<string, unknown>> | null;
  capabilities: Readonly<{ protocol_version: string | null; tool_names: readonly string[]; published: boolean }>;
  online: boolean; runtime_binding: Readonly<{ client: "codex" | "pi" | "opencode"; runtime_id: string; epoch: string; working_directory: string }> | null;
  activation: Readonly<{ state: string | null; attempt: number | null }>;
  readiness: ActivationReadiness; activation_ready: boolean | null;
  queue: Readonly<{ queued: number; claimed: number; dead_lettered: number }>;
  evidence_at: string; snapshot: "bounded_non_atomic"; next_action: string;
}>;

export type DeliveryDiagnostics = Readonly<{ agent: string; message_id: string; status: DeliveryStatus; claim_id: string | null; evidence_at: string; snapshot: "bounded_non_atomic" }>;

const json = (raw: string | null): unknown => { try { return raw === null ? null : JSON.parse(raw); } catch { return null; } };
const record = (v: unknown): Record<string, unknown> => v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {};
const safeProfile = async (redis: Redis, agent: string): Promise<Readonly<Record<string, unknown>> | null> => {
  const raw = await redis.get(SESSION_KEYS.agentProfile(agent));
  const parsed = record(json(raw));
  if (Object.keys(parsed).length) return Object.freeze({ label: typeof parsed.label === "string" ? parsed.label : undefined, purpose: typeof parsed.purpose === "string" ? parsed.purpose : undefined, kind: kind(parsed.kind), declaration_source: "self", authoritative: false });
  return null;
};
const kind = (v: unknown): AgentKind => v === "controller" || v === "worker" || v === "interactive" ? v : "unknown";
// Stored bindings may come from any host, including OpenCode, whose bindings
// the codex/pi bind_runtime input schema does not admit.
const storedBindingSchema = runtimeBindingSchema.extend({ client: z.enum(["codex", "pi", "opencode"]) });
const safeBinding = (v: unknown) => { const p = storedBindingSchema.safeParse(Object.fromEntries(Object.entries(record(v)).filter(([key]) => ["client", "runtime_id", "epoch", "working_directory"].includes(key)))); return p.success ? Object.freeze({ client: p.data.client, runtime_id: p.data.runtime_id, epoch: p.data.epoch, working_directory: p.data.working_directory }) : null; };
const now = () => new Date().toISOString();

/**
 * D2: the ONE derivation of published/readiness/activation_ready, shared by
 * details() and find()'s cheap filter path. The two copies drifted once
 * already (find() omitted the display-only slice and inherited a different
 * `published` basis); `published` is now computed from the UNSLICED tool
 * list in both paths — the 100-cap is display-only (capabilities.tool_names).
 */
const deriveReadiness = (
  online: boolean,
  binding: Readonly<{ client: string; runtime_id: string; epoch: string; working_directory: string }> | null,
  metadata: Readonly<Record<string, unknown>>
): Readonly<{ published: boolean; readiness: ActivationReadiness; activation_ready: boolean | null }> => {
  const tools = Array.isArray(metadata.tool_names) ? metadata.tool_names.filter((x): x is string => typeof x === "string") : [];
  const published =
    (typeof metadata.protocol_version === "string" || typeof metadata.protocol_version === "number") &&
    tools.includes("get_runtime_status") &&
    tools.includes("bind_runtime");
  const readiness: ActivationReadiness = !online
    ? "offline"
    : binding !== null
      ? "bound_unverified"
      : !published
        ? "unknown_legacy"
        : "unbound";
  return Object.freeze({
    published,
    readiness,
    activation_ready: readiness === "bound_unverified" ? null : false,
  });
};

/**
 * F8: lexical path-equivalence for working-directory comparison. A binding
 * validated with `/workspace/./` must satisfy a `/workspace` filter (and vice
 * versa) everywhere discovery/continuity compare directories; resolve()
 * equivalence matches the binding validation in registered-shell/runtime.ts.
 */
const sameDirectory = (a: string | null, b: string): boolean =>
  a !== null && resolve(a) === resolve(b);

/**
 * F7: run one async task per item with a small bounded worker pool instead
 * of an unbounded Promise.all, so a large registry cannot saturate Redis or
 * the event loop. Results keep the input order.
 */
const withBoundedConcurrency = async <T>(
  items: readonly string[],
  concurrency: number,
  task: (item: string) => Promise<T>
): Promise<T[]> => {
  const results = Array.from({ length: items.length }) as T[];
  let next = 0;
  const workers = Array.from(
    { length: Math.max(1, Math.min(concurrency, items.length)) },
    async () => {
      for (let i = next++; i < items.length; i = next++) {
        results[i] = await task(items[i]!);
      }
    }
  );
  await Promise.all(workers);
  return results;
};

export class AgentDiagnostics {
  private readonly sessions: SessionStore;
  constructor(private readonly redis: Redis) { this.sessions = new SessionStore(redis); }

  /** One online signal, shared by details() and find()'s cheap filter path. */
  private async isOnline(agent: string): Promise<boolean> {
    const presence = await this.sessions.getPresence(agent);
    return presence.online || (await this.redis.exists(SESSION_KEYS.heartbeat(agent))) === 1;
  }

  async details(agent: string): Promise<DiagnosticsDetails> {
    const at = now();
    const raw = await this.redis.hget(SESSION_KEYS.registry, agent);
    const reg = record(json(raw));
    if (!raw || !Object.keys(reg).length) return Object.freeze({ name: agent, discovery: null, profile: null, capabilities: { protocol_version: null, tool_names: [], published: false }, online: false, runtime_binding: null, activation: { state: null, attempt: null }, readiness: "offline", activation_ready: false, queue: { queued: 0, claimed: 0, dead_lettered: 0 }, evidence_at: at, snapshot: "bounded_non_atomic", next_action: "unknown_agent" });
    const online = await this.isOnline(agent);
    const discovery = discoveryRecord({ name: agent, role: typeof reg.role === "string" ? reg.role : "unknown", description: typeof reg.description === "string" ? reg.description : undefined, online, registered_at: reg.registered_at, pid: reg.pid, metadata: reg.metadata });
    const metadata = record(reg.metadata);
    const binding = safeBinding(json(await this.redis.get(SESSION_KEYS.runtimeBinding(agent))));
    const operation = record(json(await this.redis.get(SESSION_KEYS.activation(agent))));
    const activation = { state: typeof operation.state === "string" ? operation.state : null, attempt: Number.isSafeInteger(operation.attempt) ? operation.attempt as number : null };
    const profile = await safeProfile(this.redis, agent);
    const derived = deriveReadiness(online, binding, metadata);
    // Display-only cap: published/readiness derive from the full tool list.
    const tools = Array.isArray(metadata.tool_names) ? metadata.tool_names.filter((x): x is string => typeof x === "string").slice(0, 100) : [];
    const [queued, claimed, dead_lettered] = await Promise.all([this.redis.llen(SESSION_KEYS.queue(agent)), this.redis.zcard(CLAIM_KEYS.index(agent)), this.redis.llen(DLQ_KEYS.list(agent))]);
    return Object.freeze({ name: agent, discovery, profile, capabilities: Object.freeze({ protocol_version: typeof metadata.protocol_version === "string" || typeof metadata.protocol_version === "number" ? String(metadata.protocol_version) : null, tool_names: Object.freeze(tools), published: derived.published }), online, runtime_binding: binding, activation: Object.freeze(activation), readiness: derived.readiness, activation_ready: derived.activation_ready, queue: Object.freeze({ queued, claimed, dead_lettered }), evidence_at: at, snapshot: "bounded_non_atomic", next_action: derived.readiness === "bound_unverified" ? "probe_exact_runtime_before_relying_on_activation" : derived.readiness });
  }

  async find(filters: Readonly<{ query?: string; client?: "codex" | "pi" | "opencode"; cwd?: string; working_directory?: string; kind?: AgentKind; activation_ready?: boolean; online?: boolean; limit?: number }> = {}) {
    const limit = Math.min(100, Math.max(1, Math.floor(filters.limit ?? 50)));
    const directory = filters.working_directory ?? filters.cwd;
    const query = filters.query?.toLowerCase();

    // F7: tiered evaluation. Filters run against the CHEAP data they
    // actually need — one registry read for every name, then per-name
    // profile/binding/presence reads only when an active filter requires
    // them — with bounded concurrency. The expensive full diagnostics run
    // only for the returned page, while total_matches is still computed
    // over every name (exact-match semantics preserved).
    const needProfile = query !== undefined || filters.kind !== undefined;
    const needBinding =
      filters.client !== undefined ||
      directory !== undefined ||
      filters.activation_ready !== undefined;
    const needOnline =
      filters.online !== undefined || filters.activation_ready !== undefined;

    const registry = await this.redis.hgetall(SESSION_KEYS.registry);
    const names = Object.keys(registry);

    const matched = await withBoundedConcurrency(names, 8, async (name) => {
      const reg = record(json(registry[name] ?? null));
      const metadata = record(reg.metadata);
      const discovery = discoveryRecord({
        name, role: typeof reg.role === "string" ? reg.role : "unknown",
        description: typeof reg.description === "string" ? reg.description : undefined,
        online: false, registered_at: reg.registered_at, pid: reg.pid, metadata,
      });
      const profileRaw = needProfile
        ? record(json(await this.redis.get(SESSION_KEYS.agentProfile(name))))
        : {};
      const binding = needBinding
        ? safeBinding(json(await this.redis.get(SESSION_KEYS.runtimeBinding(name))))
        : null;
      const online = needOnline ? await this.isOnline(name) : false;

      // D2: the SAME derivation details() uses — no second copy to drift.
      const derived = deriveReadiness(online, binding, metadata);
      const activationReady = derived.activation_ready;

      const text = [
        name,
        discovery.label,
        typeof profileRaw.label === "string" ? profileRaw.label : null,
        typeof profileRaw.purpose === "string" ? profileRaw.purpose : null,
      ]
        .filter((value): value is string => value !== null)
        .join(" ")
        .toLowerCase();

      // F9: a validated runtime binding is AUTHORITATIVE over (possibly
      // stale) discovery metadata for client and working_directory filters,
      // so an adopted target is discoverable through its live binding.
      const bindingClient = binding?.client ?? discovery.client;
      const bindingDirectory = binding?.working_directory ?? discovery.working_directory;

      return (
        (!query || text.includes(query)) &&
        (filters.client === undefined || bindingClient === filters.client) &&
        (directory === undefined || sameDirectory(bindingDirectory, directory)) &&
        (filters.kind === undefined || kind(profileRaw.kind) === filters.kind) &&
        (filters.activation_ready === undefined || activationReady === filters.activation_ready) &&
        (filters.online === undefined || online === filters.online)
      );
    });

    const matchedNames = names.filter((_, i) => matched[i] === true);
    const total = matchedNames.length;
    const page = await withBoundedConcurrency(
      matchedNames.slice(0, limit),
      8,
      (name) => this.details(name)
    );
    return Object.freeze({ matches: Object.freeze(page), total_matches: total,
      truncated: total > limit, resolution: total === 1 ? "unique" : total > 1 ? "ambiguous" : "none" });
  }

  async delivery(agent: string, messageId: string): Promise<DeliveryDiagnostics> {
    const at = now(); let status: DeliveryStatus = "unknown_history"; let claim_id: string | null = null;
    const queued = await this.redis.lrange(SESSION_KEYS.queue(agent), 0, 1023); if (queued.some((x) => record(json(x)).id === messageId)) status = "queued";
    const claimIds = await this.redis.zrange(CLAIM_KEYS.index(agent), 0, -1);
    const claimRaws = claimIds.length ? await this.redis.hmget(CLAIM_KEYS.claims, ...claimIds) : [];
    for (const [i, id] of claimIds.entries()) { const c = record(json(claimRaws[i] ?? null)); if (Array.isArray(c.tasks) && c.tasks.some((x) => record(json(typeof x === "string" ? x : null)).id === messageId)) { status = Date.parse(String(c.expires_at)) < Date.now() ? "claim_expired" : "claimed"; claim_id = id; } }
    const dlq = await this.redis.lrange(DLQ_KEYS.list(agent), 0, 1023); if (dlq.some((x) => record(json(x)).id === messageId)) status = "dead_lettered";
    const traces = (await this.redis.xrevrange(SESSION_KEYS.inboxTrace(agent), "+", "-", "COUNT", 1024))
      .map(([, fields]) => Object.fromEntries(Array.from({ length: Math.floor(fields.length / 2) }, (_, i) => [fields[i * 2], fields[i * 2 + 1]])));
    if (status === "unknown_history") {
      const claim = traces.find(t => t.message_id === messageId && t.stage === "task_claimed");
      const ack = claim && traces.find(t => t.stage === "task_acknowledged" && t.claim_id === claim.claim_id && t.timestamp >= claim.timestamp);
      if (ack) { status = "acknowledged"; claim_id = claim.claim_id; }
    }
    return Object.freeze({ agent, message_id: messageId, status, claim_id, evidence_at: at, snapshot: "bounded_non_atomic" });
  }
}
