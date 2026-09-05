import type { Redis } from "ioredis";
import { SESSION_KEYS, CLAIM_KEYS, DLQ_KEYS } from "./keys.js";
import { SessionStore } from "./session-store.js";
import { discoveryRecord, type AgentDiscoveryRecord } from "./agent-discovery.js";
import { runtimeBindingSchema } from "../registered-shell/runtime.js";

export type AgentKind = "controller" | "worker" | "interactive" | "unknown";
export type ActivationReadiness = "unknown_legacy" | "offline" | "unbound" | "bound_unverified" | "ready";
export type DeliveryStatus = "queued" | "claimed" | "claim_expired" | "dead_lettered" | "acknowledged" | "unknown_history";

export type DiagnosticsDetails = Readonly<{
  name: string; discovery: AgentDiscoveryRecord | null; profile: Readonly<Record<string, unknown>> | null;
  capabilities: Readonly<{ protocol_version: string | null; tool_names: readonly string[]; published: boolean }>;
  online: boolean; runtime_binding: Readonly<{ client: "codex" | "pi"; runtime_id: string; epoch: string; working_directory: string }> | null;
  activation: Readonly<{ state: string | null; attempt: number | null }>;
  readiness: ActivationReadiness; activation_ready: boolean | null;
  queue: Readonly<{ queued: number; claimed: number; dead_lettered: number }>;
  evidence_at: string; snapshot: "bounded_non_atomic"; next_action: string;
}>;

export type DeliveryDiagnostics = Readonly<{ agent: string; message_id: string; status: DeliveryStatus; claim_id: string | null; evidence_at: string; snapshot: "bounded_non_atomic" }>;

const json = (raw: string | null): unknown => { try { return raw === null ? null : JSON.parse(raw); } catch { return null; } };
const record = (v: unknown): Record<string, unknown> => v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {};
const safeProfile = async (redis: Redis, agent: string): Promise<Readonly<Record<string, unknown>> | null> => {
  const raw = await redis.get(`gptq:agent-profile:${agent}`);
  const parsed = record(json(raw));
  if (Object.keys(parsed).length) return Object.freeze({ label: typeof parsed.label === "string" ? parsed.label : undefined, purpose: typeof parsed.purpose === "string" ? parsed.purpose : undefined, kind: kind(parsed.kind), declaration_source: "self", authoritative: false });
  return null;
};
const kind = (v: unknown): AgentKind => v === "controller" || v === "worker" || v === "interactive" ? v : "unknown";
const safeBinding = (v: unknown) => { const p = runtimeBindingSchema.safeParse(Object.fromEntries(Object.entries(record(v)).filter(([key]) => ["client", "runtime_id", "epoch", "working_directory"].includes(key)))); return p.success ? Object.freeze({ client: p.data.client, runtime_id: p.data.runtime_id, epoch: p.data.epoch, working_directory: p.data.working_directory }) : null; };
const now = () => new Date().toISOString();

export class AgentDiagnostics {
  private readonly sessions: SessionStore;
  constructor(private readonly redis: Redis) { this.sessions = new SessionStore(redis); }

  async details(agent: string): Promise<DiagnosticsDetails> {
    const at = now();
    const raw = await this.redis.hget(SESSION_KEYS.registry, agent);
    const reg = record(json(raw));
    if (!raw || !Object.keys(reg).length) return Object.freeze({ name: agent, discovery: null, profile: null, capabilities: { protocol_version: null, tool_names: [], published: false }, online: false, runtime_binding: null, activation: { state: null, attempt: null }, readiness: "offline", activation_ready: false, queue: { queued: 0, claimed: 0, dead_lettered: 0 }, evidence_at: at, snapshot: "bounded_non_atomic", next_action: "unknown_agent" });
    const presence = await this.sessions.getPresence(agent);
    const heartbeat = await this.redis.exists(SESSION_KEYS.heartbeat(agent));
    const online = presence.online || heartbeat === 1;
    const discovery = discoveryRecord({ name: agent, role: typeof reg.role === "string" ? reg.role : "unknown", description: typeof reg.description === "string" ? reg.description : undefined, online, registered_at: reg.registered_at, pid: reg.pid, metadata: reg.metadata });
    const metadata = record(reg.metadata); const tools = Array.isArray(metadata.tool_names) ? metadata.tool_names.filter((x): x is string => typeof x === "string").slice(0, 100) : [];
    const binding = safeBinding(json(await this.redis.get(`gptq:runtime-binding:${agent}`)));
    const operation = record(json(await this.redis.get(`gptq:activation:${agent}`)));
    const activation = { state: typeof operation.state === "string" ? operation.state : null, attempt: Number.isSafeInteger(operation.attempt) ? operation.attempt as number : null };
    const profile = await safeProfile(this.redis, agent);
    const published = (typeof metadata.protocol_version === "string" || typeof metadata.protocol_version === "number") && tools.includes("get_runtime_status") && tools.includes("bind_runtime");
    const readiness: ActivationReadiness = !online ? "offline" : binding !== null ? "bound_unverified" : !published ? "unknown_legacy" : "unbound";
    const [queued, claimed, dead_lettered] = await Promise.all([this.redis.llen(SESSION_KEYS.queue(agent)), this.redis.zcard(CLAIM_KEYS.index(agent)), this.redis.llen(DLQ_KEYS.list(agent))]);
    return Object.freeze({ name: agent, discovery, profile, capabilities: Object.freeze({ protocol_version: typeof metadata.protocol_version === "string" || typeof metadata.protocol_version === "number" ? String(metadata.protocol_version) : null, tool_names: Object.freeze(tools), published }), online, runtime_binding: binding, activation: Object.freeze(activation), readiness, activation_ready: readiness === "bound_unverified" ? null : false, queue: Object.freeze({ queued, claimed, dead_lettered }), evidence_at: at, snapshot: "bounded_non_atomic", next_action: readiness === "bound_unverified" ? "probe_exact_runtime_before_relying_on_activation" : readiness });
  }

  async find(filters: Readonly<{ query?: string; client?: "codex" | "pi"; cwd?: string; working_directory?: string; kind?: AgentKind; activation_ready?: boolean; online?: boolean; limit?: number }> = {}) {
    const names = await this.redis.hkeys(SESSION_KEYS.registry);
    const limit = Math.min(100, Math.max(1, Math.floor(filters.limit ?? 50)));
    const all = await Promise.all(names.map(n => this.details(n)));
    const directory = filters.working_directory ?? filters.cwd;
    const found = all.filter(d => {
      const query = filters.query?.toLowerCase();
      const text = [d.name, d.discovery?.label, d.profile?.label, d.profile?.purpose]
        .filter((value): value is string => typeof value === "string").join(" ").toLowerCase();
      return (!query || text.includes(query)) &&
        (filters.client === undefined || (d.runtime_binding?.client ?? d.discovery?.client) === filters.client) &&
        (directory === undefined || (d.discovery?.working_directory ?? d.runtime_binding?.working_directory) === directory) &&
        (filters.kind === undefined || d.profile?.kind === filters.kind) &&
        (filters.activation_ready === undefined || d.activation_ready === filters.activation_ready) &&
        (filters.online === undefined || d.online === filters.online);
    });
    return Object.freeze({ matches: Object.freeze(found.slice(0, limit)), total_matches: found.length,
      truncated: found.length > limit, resolution: found.length === 1 ? "unique" : found.length > 1 ? "ambiguous" : "none" });
  }

  async delivery(agent: string, messageId: string): Promise<DeliveryDiagnostics> {
    const at = now(); let status: DeliveryStatus = "unknown_history"; let claim_id: string | null = null;
    const queued = await this.redis.lrange(SESSION_KEYS.queue(agent), 0, 1023); if (queued.some((x) => record(json(x)).id === messageId)) status = "queued";
    for (const id of await this.redis.zrange(CLAIM_KEYS.index(agent), 0, -1)) { const c = record(json(await this.redis.hget(CLAIM_KEYS.claims, id))); if (Array.isArray(c.tasks) && c.tasks.some((x) => record(json(typeof x === "string" ? x : null)).id === messageId)) { status = Date.parse(String(c.expires_at)) < Date.now() ? "claim_expired" : "claimed"; claim_id = id; } }
    const dlq = await this.redis.lrange(DLQ_KEYS.list(agent), 0, 1023); if (dlq.some((x) => record(json(x)).id === messageId)) status = "dead_lettered";
    const traces = (await this.redis.xrevrange(`gptq:inbox-trace:${agent}`, "+", "-", "COUNT", 1024))
      .map(([, fields]) => Object.fromEntries(Array.from({ length: Math.floor(fields.length / 2) }, (_, i) => [fields[i * 2], fields[i * 2 + 1]])));
    if (status === "unknown_history") {
      const claim = traces.find(t => t.message_id === messageId && t.stage === "task_claimed");
      const ack = claim && traces.find(t => t.stage === "task_acknowledged" && t.claim_id === claim.claim_id && t.timestamp >= claim.timestamp);
      if (ack) { status = "acknowledged"; claim_id = claim.claim_id; }
    }
    return Object.freeze({ agent, message_id: messageId, status, claim_id, evidence_at: at, snapshot: "bounded_non_atomic" });
  }
}
