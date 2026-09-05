import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Redis } from "ioredis";
import { AgentDiagnostics } from "../src/core/agent-diagnostics.js";
import { SESSION_KEYS, CLAIM_KEYS, DLQ_KEYS } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const registry = (name: string, metadata: Record<string, unknown> = {}) => JSON.stringify({ name, role: "both", registered_at: new Date().toISOString(), metadata });
const task = (id: string) => JSON.stringify({ id, type: "task", from: "sender", to: "worker", timestamp: new Date().toISOString(), payload: { text: "safe" } });

describe("AgentDiagnostics", () => {
  let redis: Redis; let diagnostics: AgentDiagnostics;
  beforeEach(async () => { redis = new Redis(URL); await flushTestKeys(redis, URL); diagnostics = new AgentDiagnostics(redis); });
  afterEach(async () => { await flushTestKeys(redis, URL); await redis.quit(); });

  it("reports an explicit unknown agent", async () => { const d = await diagnostics.details("missing"); expect(d.discovery).toBeNull(); expect(d.readiness).toBe("offline"); expect(d.next_action).toBe("unknown_agent"); });
  it("classifies legacy online agents as unknown_legacy", async () => { await redis.hset(SESSION_KEYS.registry, "legacy", registry("legacy")); await redis.set(SESSION_KEYS.heartbeat("legacy"), "alive", "EX", 30); const d = await diagnostics.details("legacy"); expect(d.online).toBe(true); expect(d.readiness).toBe("unknown_legacy"); });
  it("classifies published online agents without binding as unbound", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker", { protocol_version: "1", tool_names: ["claim_tasks", "get_runtime_status", "bind_runtime"] })); await redis.set(SESSION_KEYS.heartbeat("worker"), "alive", "EX", 30); expect((await diagnostics.details("worker")).readiness).toBe("unbound"); });
  it("reports a bound lease as unverified until the actual runtime is probed", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker", { protocol_version: "1", tool_names: ["claim_tasks", "get_runtime_status", "bind_runtime"] })); await redis.set(SESSION_KEYS.heartbeat("worker"), "alive", "EX", 30); await redis.set("gptq:runtime-binding:worker", JSON.stringify({ client: "codex", runtime_id: "r1", epoch: "e1", working_directory: "/tmp/work", token: "secret" })); const d = await diagnostics.details("worker"); expect(d.readiness).toBe("bound_unverified"); expect(d.activation_ready).toBeNull(); expect(d.runtime_binding).not.toHaveProperty("token"); });
  it("rejects malformed binding conservatively", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker", { protocol_version: "1", tool_names: ["claim_tasks", "get_runtime_status", "bind_runtime"] })); await redis.set(SESSION_KEYS.heartbeat("worker"), "alive", "EX", 30); await redis.set("gptq:runtime-binding:worker", "{bad"); const d = await diagnostics.details("worker"); expect(d.runtime_binding).toBeNull(); expect(d.readiness).toBe("unbound"); });
  it("ignores forged profile authority fields and sanitizes profile", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.set("gptq:agent-profile:worker", JSON.stringify({ label: "Friendly", purpose: "work", kind: "controller", token: "secret", authority: "admin" })); const d = await diagnostics.details("worker"); expect(d.profile).toEqual({ label: "Friendly", purpose: "work", kind: "controller", declaration_source: "self", authoritative: false }); expect(JSON.stringify(d)).not.toContain("secret"); });
  it("supports exact identity filters and reports ambiguity", async () => { await redis.hset(SESSION_KEYS.registry, "a", registry("a"), "b", registry("b")); const result = await diagnostics.find({ query: "nope" }); expect(result.resolution).toBe("none"); const exact = await diagnostics.find({ query: "a" }); expect(exact.resolution).toBe("unique"); });
  it("filters by cwd and never routes by cwd alone", async () => { await redis.hset(SESSION_KEYS.registry, "a", registry("a", { working_directory: "/same" }), "b", registry("b", { working_directory: "/same" })); const result = await diagnostics.find({ cwd: "/same" }); expect(result.matches).toHaveLength(2); expect(result.resolution).toBe("ambiguous"); });
  it("reports queue counts and queued delivery", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(SESSION_KEYS.queue("worker"), task("m1")); const d = await diagnostics.details("worker"); expect(d.queue.queued).toBe(1); expect((await diagnostics.delivery("worker", "m1")).status).toBe("queued"); });
  it("reports active and expired claim locations", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.hset(CLAIM_KEYS.claims, "c1", JSON.stringify({ claim_id: "c1", actor_id: "worker", session_id: "s", claimed_at: new Date().toISOString(), expires_at: new Date(Date.now() + 60000).toISOString(), tasks: [task("m2")] })); await redis.zadd(CLAIM_KEYS.index("worker"), Date.now() + 60000, "c1"); expect((await diagnostics.delivery("worker", "m2")).status).toBe("claimed"); });
  it("reports DLQ entries", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(DLQ_KEYS.list("worker"), task("m3")); const d = await diagnostics.details("worker"); expect(d.queue.dead_lettered).toBe(1); expect((await diagnostics.delivery("worker", "m3")).status).toBe("dead_lettered"); });
  it("preserves current queued location over historical acknowledgement", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(SESSION_KEYS.queue("worker"), task("m4")); await redis.xadd("gptq:inbox-trace:worker", "*", "stage", "task_acknowledged", "timestamp", new Date().toISOString(), "message_id", "m4", "claim_id", "old"); expect((await diagnostics.delivery("worker", "m4")).status).toBe("queued"); });
  it("returns unknown_history after bounded history has no message", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); const d = await diagnostics.delivery("worker", "never"); expect(d.status).toBe("unknown_history"); expect(d.snapshot).toBe("bounded_non_atomic"); });
});

describe("delivery evidence correlation", () => {
  it("joins message claims to acknowledgements and retains unknown for unrelated claims", async () => {
    const redis = new Redis(URL); const diagnostics = new AgentDiagnostics(redis);
    try {
      await flushTestKeys(redis, URL);
      await redis.xadd("gptq:inbox-trace:worker", "*", "stage", "task_claimed", "timestamp", "2026-01-01T00:00:00Z", "message_id", "m", "claim_id", "c");
      await redis.xadd("gptq:inbox-trace:worker", "*", "stage", "task_acknowledged", "timestamp", "2026-01-01T00:00:01Z", "claim_id", "other");
      expect((await diagnostics.delivery("worker", "m")).status).toBe("unknown_history");
      await redis.xadd("gptq:inbox-trace:worker", "*", "stage", "task_acknowledged", "timestamp", "2026-01-01T00:00:02Z", "claim_id", "c");
      expect((await diagnostics.delivery("worker", "m")).status).toBe("acknowledged");
      await redis.del("gptq:inbox-trace:worker");
      expect((await diagnostics.delivery("worker", "m")).status).toBe("unknown_history");
    } finally { await flushTestKeys(redis, URL); await redis.quit(); }
  });
});
