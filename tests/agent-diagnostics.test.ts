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
  it("keeps OpenCode clients and bindings visible instead of coercing them away", async () => {
    await redis.hset(SESSION_KEYS.registry, "oc", registry("oc", { client: "opencode", protocol_version: "1", tool_names: ["claim_tasks", "get_runtime_status", "bind_runtime"] }));
    await redis.set(SESSION_KEYS.heartbeat("oc"), "alive", "EX", 30);
    await redis.set(SESSION_KEYS.runtimeBinding("oc"), JSON.stringify({ client: "opencode", runtime_id: "ses_1", epoch: "e1", working_directory: "/work" }), "EX", 30);
    const d = await diagnostics.details("oc");
    expect(d.discovery?.client).toBe("opencode");
    expect(d.runtime_binding).toMatchObject({ client: "opencode", runtime_id: "ses_1" });
    const found = await diagnostics.find({ client: "opencode" });
    expect(found.resolution).toBe("unique");
    expect(found.matches.map((m) => m.name)).toEqual(["oc"]);
  });
  it("filters by cwd and never routes by cwd alone", async () => { await redis.hset(SESSION_KEYS.registry, "a", registry("a", { working_directory: "/same" }), "b", registry("b", { working_directory: "/same" })); const result = await diagnostics.find({ cwd: "/same" }); expect(result.matches).toHaveLength(2); expect(result.resolution).toBe("ambiguous"); });
  it("reports queue counts and queued delivery", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(SESSION_KEYS.queue("worker"), task("m1")); const d = await diagnostics.details("worker"); expect(d.queue.queued).toBe(1); expect((await diagnostics.delivery("worker", "m1")).status).toBe("queued"); });
  it("reports active and expired claim locations", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.hset(CLAIM_KEYS.claims, "c1", JSON.stringify({ claim_id: "c1", actor_id: "worker", session_id: "s", claimed_at: new Date().toISOString(), expires_at: new Date(Date.now() + 60000).toISOString(), tasks: [task("m2")] })); await redis.zadd(CLAIM_KEYS.index("worker"), Date.now() + 60000, "c1"); expect((await diagnostics.delivery("worker", "m2")).status).toBe("claimed"); });
  it("reports DLQ entries", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(DLQ_KEYS.list("worker"), task("m3")); const d = await diagnostics.details("worker"); expect(d.queue.dead_lettered).toBe(1); expect((await diagnostics.delivery("worker", "m3")).status).toBe("dead_lettered"); });
  it("preserves current queued location over historical acknowledgement", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); await redis.rpush(SESSION_KEYS.queue("worker"), task("m4")); await redis.xadd("gptq:inbox-trace:worker", "*", "stage", "task_acknowledged", "timestamp", new Date().toISOString(), "message_id", "m4", "claim_id", "old"); expect((await diagnostics.delivery("worker", "m4")).status).toBe("queued"); });
  it("returns unknown_history after bounded history has no message", async () => { await redis.hset(SESSION_KEYS.registry, "worker", registry("worker")); const d = await diagnostics.delivery("worker", "never"); expect(d.status).toBe("unknown_history"); expect(d.snapshot).toBe("bounded_non_atomic"); });

  // D2: find()'s cheap filter path must classify online/activation_ready
  // EXACTLY as details() does — one shared derivation, verified across the
  // four lifecycle states plus a >100-tool agent (the drift case).
  it("find's online/activation_ready filters agree with details() for every state (D2)", async () => {
    const publishedTools = ["claim_tasks", "get_runtime_status", "bind_runtime"];
    await redis.hset(SESSION_KEYS.registry,
      "offline-legacy", registry("offline-legacy"),
      "online-legacy", registry("online-legacy"),
      "online-published", registry("online-published", { protocol_version: "1", tool_names: publishedTools }),
      "online-bound", registry("online-bound", { protocol_version: "1", tool_names: publishedTools }));
    await redis.set(SESSION_KEYS.heartbeat("online-legacy"), "alive", "EX", 30);
    await redis.set(SESSION_KEYS.heartbeat("online-published"), "alive", "EX", 30);
    await redis.set(SESSION_KEYS.heartbeat("online-bound"), "alive", "EX", 30);
    await redis.set("gptq:runtime-binding:online-bound", JSON.stringify({ client: "codex", runtime_id: "r1", epoch: "e1", working_directory: "/tmp/work" }));

    for (const name of ["offline-legacy", "online-legacy", "online-published", "online-bound"]) {
      const d = await diagnostics.details(name);
      const readyMatches = await diagnostics.find({ activation_ready: true });
      const notReadyMatches = await diagnostics.find({ activation_ready: false });
      const onlineMatches = await diagnostics.find({ online: true });
      expect(readyMatches.matches.map((m) => m.name).includes(name)).toBe(d.activation_ready === true);
      expect(notReadyMatches.matches.map((m) => m.name).includes(name)).toBe(d.activation_ready === false);
      expect(onlineMatches.matches.map((m) => m.name).includes(name)).toBe(d.online);
    }
    // The exact state ladder the agreement walk relies on.
    expect((await diagnostics.details("offline-legacy")).readiness).toBe("offline");
    expect((await diagnostics.details("online-legacy")).readiness).toBe("unknown_legacy");
    expect((await diagnostics.details("online-published")).readiness).toBe("unbound");
    expect((await diagnostics.details("online-bound")).readiness).toBe("bound_unverified");
  });

  it("published derives from the full tool list; the 100-cap is display-only (D2)", async () => {
    const many = Array.from({ length: 150 }, (_, i) => `tool-${i}`);
    many[140] = "get_runtime_status";
    many[141] = "bind_runtime";
    await redis.hset(SESSION_KEYS.registry, "wide", registry("wide", { protocol_version: "1", tool_names: many }));
    await redis.set(SESSION_KEYS.heartbeat("wide"), "alive", "EX", 30);
    const d = await diagnostics.details("wide");
    // Both paths agree the agent is published (required tools past index 99),
    // while the displayed tool_names stay capped.
    expect(d.readiness).toBe("unbound");
    expect(d.capabilities.published).toBe(true);
    expect(d.capabilities.tool_names).toHaveLength(100);
    expect((await diagnostics.find({ online: true })).total_matches).toBe(1);
  });
  // F7: the expensive per-agent diagnostics run only for the returned page
  // while total_matches still counts every registry name, so a large
  // registry cannot trigger an unbounded full-diagnostics fan-out.
  it("find computes total_matches over every name but runs details() only for the page", async () => {
    const names = Array.from({ length: 60 }, (_, i) => `scale-${i}`);
    await redis.hset(SESSION_KEYS.registry, ...names.flatMap((n) => [n, registry(n)]));
    let detailsCalls = 0;
    const original = diagnostics.details.bind(diagnostics);
    (diagnostics as unknown as { details: typeof original }).details = async (agent: string) => {
      detailsCalls += 1;
      return original(agent);
    };
    const result = await diagnostics.find({ limit: 5 });
    expect(detailsCalls).toBe(5);
    expect(result.matches).toHaveLength(5);
    expect(result.matches.every((m) => /^scale-\d+$/.test(m.name))).toBe(true);
    expect(result.total_matches).toBe(60);
    expect(result.truncated).toBe(true);
    expect(result.resolution).toBe("ambiguous");
  });

  // F8: working-directory comparison is lexical path equivalence, not exact
  // string equality, so `.`/`..` aliases of a validated directory match.
  it("matches working_directory filters through . and .. aliases (F8)", async () => {
    await redis.hset(SESSION_KEYS.registry,
      "bound", registry("bound"),
      "discovered", registry("discovered", { working_directory: "/tmp/dir-b/./" }));
    await redis.set("gptq:runtime-binding:bound", JSON.stringify({ client: "codex", runtime_id: "r1", epoch: "e1", working_directory: "/tmp/dir-a" }));
    const viaBinding = await diagnostics.find({ working_directory: "/tmp/dir-a/../dir-a" });
    expect(viaBinding.matches.map((m) => m.name)).toEqual(["bound"]);
    const viaDiscovery = await diagnostics.find({ cwd: "/tmp/dir-b/../dir-b" });
    expect(viaDiscovery.matches.map((m) => m.name)).toEqual(["discovered"]);
  });

  // F9: a validated runtime binding is authoritative over stale discovery
  // metadata for client and working_directory filters.
  it("prefers the runtime binding over discovery metadata for client and working_directory (F9)", async () => {
    await redis.hset(SESSION_KEYS.registry, "adopted", registry("adopted", { client: "pi", working_directory: "/stale-dir" }));
    await redis.set("gptq:runtime-binding:adopted", JSON.stringify({ client: "codex", runtime_id: "r2", epoch: "e1", working_directory: "/live-dir" }));
    const byLive = await diagnostics.find({ client: "codex", working_directory: "/live-dir" });
    expect(byLive.matches.map((m) => m.name)).toEqual(["adopted"]);
    const byStaleClient = await diagnostics.find({ client: "pi" });
    expect(byStaleClient.total_matches).toBe(0);
    const byStaleDir = await diagnostics.find({ working_directory: "/stale-dir" });
    expect(byStaleDir.total_matches).toBe(0);
  });
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
