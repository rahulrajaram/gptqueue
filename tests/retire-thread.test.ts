/**
 * Focused guard tests for core/retire-thread.ts (P004). The core runs with
 * injected fake RPC / Redis / clock / process-pin dependencies, so every
 * refusal and environment error is asserted to send NO thread/archive request.
 * A second section runs the same guards over this run's private socket-only
 * test Redis (real SCAN pagination, real zset claim scoring) through the same
 * storeOf adapter the CLI uses.
 */
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { execFileSync } from "node:child_process";
import { createServer, type Socket } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import {
  DEFAULT_VERIFY_TIMEOUT_MS,
  parseRetireThreadArgs,
  runRetireThread,
  storeOf,
  type DaemonRef,
  type DaemonSnapshotRef,
  type ChildRef,
  type ProcPinRef,
  type RetireEvent,
  type RetireDeps,
  type RetireOptions,
  type RetireOutcome,
  type RetireRpc,
  type RetireStore,
} from "../src/core/retire-thread.js";
import { testRedisUrl } from "./helpers/test-redis.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

// ---------------------------------------------------------------- fakes

type RpcOutcome =
  | Readonly<{ ok: Record<string, unknown> }>
  | Readonly<{ error: Error }>
  | Readonly<{ run: (params: Record<string, unknown>) => Record<string, unknown> | Promise<Record<string, unknown>> }>;

const ok = (value: Record<string, unknown>): RpcOutcome => ({ ok: value });
const times = (count: number, outcome: RpcOutcome): RpcOutcome[] => Array.from({ length: count }, () => outcome);

const scriptRpc = (scripts: Record<string, readonly RpcOutcome[]>, repeatLast: readonly string[] = ["thread/loaded/list"]): RetireRpc & {
  calls: ReadonlyArray<Readonly<{ method: string; params: Record<string, unknown>; kind: string }>>;
} => {
  const calls: Array<{ method: string; params: Record<string, unknown>; kind: string }> = [];
  const used = new Map<string, number>();
  return {
    calls,
    async request(method, params, kind) {
      calls.push({ method, params: { ...params }, kind });
      const index = used.get(method) ?? 0;
      used.set(method, index + 1);
      const script = scripts[method] ?? [];
      const outcome = script[index] ?? (repeatLast.includes(method) ? script[script.length - 1] : undefined);
      if (outcome === undefined) throw new Error(`unexpected RPC ${method} call #${index + 1}`);
      if ("error" in outcome) throw outcome.error;
      if ("run" in outcome) return await outcome.run(params);
      return structuredClone(outcome.ok);
    },
  };
};

type FakeStore = RetireStore & {
  strings: Map<string, string>;
  hashes: Map<string, Map<string, string>>;
  zsets: Map<string, Array<Readonly<{ member: string; score: number }>>>;
  lists: Map<string, string[]>;
  failures: Map<string, Error>;
  /** Called before each zcount with its 1-based call index (scripted guard flips). */
  zcountHook: { fn: ((call: number) => void) | null };
};

const fakeStore = (seed: Readonly<{
  strings?: Record<string, string>;
  hashes?: Record<string, Record<string, string>>;
  zsets?: Record<string, ReadonlyArray<readonly [string, number]>>;
  lists?: Record<string, readonly string[]>;
}> = {}): FakeStore => {
  const strings = new Map(Object.entries(seed.strings ?? {}));
  const hashes = new Map(Object.entries(seed.hashes ?? {}).map(([key, fields]) => [key, new Map(Object.entries(fields))]));
  const zsets = new Map(Object.entries(seed.zsets ?? {}).map(([key, members]) => [key, members.map(([member, score]) => ({ member, score }))]));
  const lists = new Map(Object.entries(seed.lists ?? {}).map(([key, values]) => [key, [...values]]));
  const failures = new Map<string, Error>();
  const zcountHook: FakeStore["zcountHook"] = { fn: null };
  let zcountCalls = 0;
  const guard = async (command: string): Promise<void> => {
    const error = failures.get(command);
    if (error) throw error;
  };
  return {
    strings, hashes, zsets, lists, failures, zcountHook,
    async time() { await guard("time"); return ["1700000000", "0"]; },
    async scan(cursor, pattern, count) {
      await guard("scan");
      const regex = new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`);
      const keys = [...strings.keys()].filter((key) => regex.test(key)).sort();
      const start = cursor === "0" ? 0 : Number(cursor);
      const page = keys.slice(start, start + count);
      return [start + count < keys.length ? String(start + count) : "0", page];
    },
    async get(key) { await guard("get"); return strings.get(key) ?? null; },
    async zcount(key, min, max) {
      await guard("zcount");
      zcountCalls += 1;
      zcountHook.fn?.(zcountCalls);
      const lo = min === "-inf" ? -Infinity : Number(min);
      const hi = max === "+inf" ? Infinity : Number(max);
      return (zsets.get(key) ?? []).filter((entry) => entry.score >= lo && entry.score <= hi).length;
    },
    async llen(key) { await guard("llen"); return (lists.get(key) ?? []).length; },
    async exists(key) { await guard("exists"); return strings.has(key) ? 1 : 0; },
    async hget(key, field) { await guard("hget"); return hashes.get(key)?.get(field) ?? null; },
  };
};

// ---------------------------------------------------------------- fixture

const DAEMON: DaemonRef = {
  pid: 4242, startTicks: "777",
  argv: ["/usr/local/bin/codex", "app-server", "--listen", "unix:///x.sock", "--managed-daemon"],
  basis: "socket-owner", socketPath: "/x.sock",
};
const SIDECAR: ChildRef = { pid: 8100, startTicks: "222", argv: ["node", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis://x"] };
const OTHER_CHILD: ChildRef = { pid: 8200, startTicks: "223", argv: ["codex", "exec"] };
const SIDE_PIN: ProcPinRef = { pid: 8100, startTicks: "222", ppid: 4242, argv: SIDECAR.argv };

const idleThread = (threadId: string, extra: Record<string, unknown> = {}): Record<string, unknown> => ({
  thread: { id: threadId, cwd: "/work/dir", status: { type: "idle" }, threadSource: "codex", ...extra },
});

const MAPPING_KEY = SESSION_KEYS.runtimeMailbox("codex", "t1");

const happyStore = (): ReturnType<typeof fakeStore> => fakeStore({
  strings: {
    "gptq:runtime-binding:mb1": JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e1", token: "owner1", working_directory: "/w" }),
    [MAPPING_KEY]: JSON.stringify({ agent: "mb1", working_directory: "/w" }),
  },
  hashes: { "gptq:registry": { mb1: JSON.stringify({ name: "mb1", role: "codex shell", registered_at: "2026-10-01T00:00:00Z", pid: 8100 }) } },
});

type Fixture = Readonly<{
  rpc: ReturnType<typeof scriptRpc>;
  store: FakeStore;
  run: () => Promise<RetireOutcome>;
  events: readonly RetireEvent[];
  pins: Map<number, ProcPinRef | null | Error>;
  discoverError: { error: Error | null };
  snapshotErrors: Error[];
  setPostArchive: (value: boolean) => void;
  setStore: (store: RetireStore) => void;
}>;

const makeFixture = (
  options: Partial<RetireOptions>,
  rpcScripts: Record<string, readonly RpcOutcome[]>,
  store: FakeStore = happyStore(),
  overrides: Partial<RetireDeps> = {},
): Fixture => {
  const rpc = scriptRpc(rpcScripts);
  const events: RetireEvent[] = [];
  const pins = new Map<number, ProcPinRef | null | Error>([[8100, SIDE_PIN]]);
  const discoverError: { error: Error | null } = { error: null };
  const snapshotErrors: Error[] = [];
  let nowMs = 1_700_000_000_000;
  let postArchive = false;
  let snapshotCalls = 1;
  let activeStore: RetireStore = store;
  const snapshotFor = (call: number): DaemonSnapshotRef => {
    const error = snapshotErrors[call - 1];
    if (error) throw error;
    return postArchive
      ? { daemon: DAEMON, children: [OTHER_CHILD], sidecars: [] }
      : { daemon: DAEMON, children: [SIDECAR, OTHER_CHILD], sidecars: [SIDECAR] };
  };
  const deps = {
    store: {
      time: () => activeStore.time(),
      scan: (...args: Parameters<RetireStore["scan"]>) => activeStore.scan(...args),
      get: (...args: Parameters<RetireStore["get"]>) => activeStore.get(...args),
      zcount: (...args: Parameters<RetireStore["zcount"]>) => activeStore.zcount(...args),
      llen: (...args: Parameters<RetireStore["llen"]>) => activeStore.llen(...args),
      exists: (...args: Parameters<RetireStore["exists"]>) => activeStore.exists(...args),
      hget: (...args: Parameters<RetireStore["hget"]>) => activeStore.hget(...args),
    },
    rpc,
    discover: async (): Promise<DaemonRef> => {
      if (discoverError.error) throw discoverError.error;
      return DAEMON;
    },
    snapshot: async () => snapshotFor(snapshotCalls++),
    pinProcess: async (pid: number) => {
      const pin = pins.get(pid);
      if (pin instanceof Error) throw pin;
      return pin ?? null;
    },
    monotonic: () => nowMs,
    clock: () => nowMs,
    sleep: async (ms: number) => { nowMs += ms; },
    onEvent: (event: RetireEvent) => events.push(event),
    ...overrides,
  };
  return {
    rpc, store, events, pins, discoverError, snapshotErrors,
    setPostArchive: (value: boolean) => { postArchive = value; },
    setStore: (replacement: RetireStore) => { activeStore = replacement; },
    run: () => runRetireThread({
      threadId: "t1", expectedMailbox: "mb1", expectNoMailbox: false, dryRun: false,
      allowPendingMail: false, verifyTimeoutMs: 5_000, socketPath: "/x.sock", ...options,
    }, deps),
  };
};

const archiveCalls = (fixture: Fixture): number => fixture.rpc.calls.filter((call) => call.method === "thread/archive").length;
const problemsOf = (event: RetireEvent | undefined): string[] => (event?.problems as string[] | undefined) ?? [];

const steps = (fixture: Fixture): string[] => fixture.events.map((event) => event.step);

describe("retire-thread argument parsing", () => {
  const base = ["t1", "mb1", "--redis-url", "redis://127.0.0.1:6379/1"];

  it("parses the full surface", () => {
    const parsed = parseRetireThreadArgs([...base, "--dry-run", "--allow-pending-mail", "--socket", "/s.sock", "--verify-timeout-ms", "1234"]);
    expect(parsed).toMatchObject({ threadId: "t1", expectedMailbox: "mb1", dryRun: true, allowPendingMail: true, redisUrl: "redis://127.0.0.1:6379/1", socketPath: "/s.sock", verifyTimeoutMs: 1234 });
  });

  it("parses --expect-no-mailbox", () => {
    const parsed = parseRetireThreadArgs(["t1", "--expect-no-mailbox", "--redis-url", "redis://u"]);
    expect(parsed).toMatchObject({ expectedMailbox: null, expectNoMailbox: true, verifyTimeoutMs: DEFAULT_VERIFY_TIMEOUT_MS });
  });

  it("refuses usage errors", () => {
    expect(parseRetireThreadArgs(["t1", "--expect-no-mailbox"])).toBe("Required --redis-url (the Redis endpoint is explicit; there is no default)");
    expect(parseRetireThreadArgs(["t1", "mb1"])).toBe("Required --redis-url (the Redis endpoint is explicit; there is no default)");
    expect(parseRetireThreadArgs(["t1", "mb1", "--expect-no-mailbox", "--redis-url", "redis://u"])).toBe("Pass either <expected-mailbox> or --expect-no-mailbox, not both");
    expect(parseRetireThreadArgs(["mb1", "--redis-url", "redis://u"])).toBe("Required <expected-mailbox> (or --expect-no-mailbox)");
    expect(parseRetireThreadArgs(["t1", "mb1", "--redis-url", "redis://u", "--nope"])).toBe("Unknown option --nope");
    expect(parseRetireThreadArgs(["t1", "mb1", "--redis-url", "redis://u", "--redis-url", "redis://v"])).toBe("Duplicate option --redis-url");
    expect(parseRetireThreadArgs(["t1", "mb1", "--redis-url", "redis://u", "--dry-run", "--dry-run"])).toBe("Duplicate flag --dry-run");
    expect(parseRetireThreadArgs(["t1", "mb1", "--redis-url", "redis://u", "--verify-timeout-ms", "0"])).toBe("--verify-timeout-ms must be an integer from 1 to 300000 milliseconds");
    expect(parseRetireThreadArgs(["t1", "mb1", "x", "--redis-url", "redis://u"])).toBe("Unexpected argument x");
    expect(parseRetireThreadArgs(["t1", "mb1", "--redis-url"])).toBe("Option --redis-url requires a value");
  });
});

describe("retire-thread guards (injected fakes)", () => {
  it("happy path: one archive request, verified receipt, restore hint names the selected socket", async () => {
    const fixture = makeFixture({}, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { fixture.pins.set(8100, null); fixture.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(0);
    expect(result.outcome).toBe("verified");
    expect(archiveCalls(fixture)).toBe(1);
    expect(steps(fixture)).toEqual(["check", "archived", "verify"]);
    const check = fixture.events[0]!;
    expect(check).toMatchObject({
      threadId: "t1", cwd: "/work/dir", status: "idle", parent: null, parent_status: null,
      bound_mailbox: "mb1", mapped_mailbox: "mb1", expected_mailbox: "mb1",
      unexpired_claims: 0, queued: 0, sidecar_pid: 8100, sidecar_evidence: "pinned",
      codex_sidecars_total: 1, daemon_children: 2, daemon: { pid: 4242, basis: "socket-owner" },
    });
    const verify = fixture.events[2]!;
    expect(verify).toMatchObject({ outcome: "verified", unloaded: true, heartbeat: false, sidecar_exited: true, sidecar_evidence: "pinned", exited_sidecar_pids: [8100], codex_sidecars_before: 1, codex_sidecars_after: 0, daemon_children_before: 2, daemon_children_after: 1 });
    expect(verify.restore).toContain("codex resume t1 --remote unix:///x.sock -C /work/dir");
  });

  it("notLoaded is an accurate no-op: no archive, real counts, no universal children claim", async () => {
    const fixture = makeFixture({}, {
      "thread/read": [ok(idleThread("t1", { status: { type: "notLoaded" } }))],
      "thread/archive": [ok({})],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(0);
    expect(result.outcome).toBe("already_unloaded");
    expect(archiveCalls(fixture)).toBe(0);
    expect(steps(fixture)).toEqual(["check", "already_unloaded"]);
    const noop = fixture.events[1]!;
    expect(noop).toMatchObject({ codex_sidecars_total: 1, daemon_children: 2 });
    expect(JSON.stringify(noop)).not.toContain("holds no MCP children");
  });

  it("dry-run: preconditions hold, nothing changes, no archive", async () => {
    const fixture = makeFixture({ dryRun: true }, {
      "thread/read": [ok(idleThread("t1"))],
      "thread/archive": [ok({})],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(0);
    expect(result.outcome).toBe("dry_run");
    expect(archiveCalls(fixture)).toBe(0);
    expect(steps(fixture)).toEqual(["check", "dry_run"]);
  });

  it("refuses a non-idle thread", async () => {
    const fixture = makeFixture({}, {
      "thread/read": [ok(idleThread("t1", { status: { type: "running" } }))],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(3);
    expect(fixture.events[1]!.problems).toContain("thread status is running; retire only an idle thread");
    expect(archiveCalls(fixture)).toBe(0);
  });

  it("sub-agent: refuses unless the parent is positively idle (active and unknown refuse; unreadable read is exit 4)", async () => {
    const subThread = idleThread("t1", { parentThreadId: "p1", status: { type: "idle" } });
    const active = makeFixture({}, {
      "thread/read": [ok(subThread), ok(idleThread("p1", { status: { type: "active" } }))],
    });
    const activeResult = await active.run();
    expect(activeResult.exitCode).toBe(3);
    expect(active.events[1]!.problems).toContain("parent thread p1 is active; retire a sub-agent thread only while its parent is positively idle");
    expect(archiveCalls(active)).toBe(0);

    const unknown = makeFixture({}, {
      "thread/read": [ok(subThread), ok({ thread: { id: "p1", cwd: "/w" } })], // no status
    });
    const unknownResult = await unknown.run();
    expect(unknownResult.exitCode).toBe(3);
    expect(problemsOf(unknown.events[1])[0]).toContain("parent thread p1 is unknown");
    expect(archiveCalls(unknown)).toBe(0);

    const unreadable = makeFixture({}, {
      "thread/read": [ok(subThread), { error: new Error("parent read failed") }],
    });
    const unreadableResult = await unreadable.run();
    expect(unreadableResult.exitCode).toBe(4);
    expect(unreadableResult.code).toBe("rpc_error");
    expect(archiveCalls(unreadable)).toBe(0);
  });

  it("mailbox must be proven by binding AND mapping evidence", async () => {
    // A different mailbox is bound.
    const other = happyStore();
    other.strings.delete("gptq:runtime-binding:mb1");
    other.strings.set("gptq:runtime-binding:other", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e", token: "owner1", working_directory: "/w" }));
    const boundOther = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, other);
    expect((await boundOther.run()).exitCode).toBe(3);
    expect(boundOther.events[1]!.problems).toContain("the mailbox bound to this thread is other, not mb1");

    // Mapping names another agent.
    const mappedOther = happyStore();
    mappedOther.strings.set(MAPPING_KEY, JSON.stringify({ agent: "other", working_directory: "/w" }));
    const mappedFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, mappedOther);
    expect((await mappedFixture.run()).exitCode).toBe(3);
    expect(mappedFixture.events[1]!.problems).toContain("the thread's durable mailbox mapping names other, not mb1");

    // No evidence at all ties the thread to the expected mailbox.
    const none = happyStore();
    none.strings.delete("gptq:runtime-binding:mb1");
    none.strings.delete(MAPPING_KEY);
    const noneFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, none);
    expect((await noneFixture.run()).exitCode).toBe(3);
    expect(problemsOf(noneFixture.events[1]).some(problem => problem.includes("no mailbox evidence ties this thread to mb1"))).toBe(true);
    for (const fixture of [boundOther, mappedFixture, noneFixture]) expect(archiveCalls(fixture)).toBe(0);
  });

  it("--expect-no-mailbox: refuses when a binding or mapping exists; succeeds with neither", async () => {
    const store = happyStore();
    const fixture = makeFixture({ expectNoMailbox: true, expectedMailbox: null }, { "thread/read": [ok(idleThread("t1"))] }, store);
    const refused = await fixture.run();
    expect(refused.exitCode).toBe(3);
    expect(fixture.events[1]!.problems).toContain("a mailbox is bound to this thread (mb1); pass it instead of --expect-no-mailbox");
    expect(archiveCalls(fixture)).toBe(0);

    const mappingOnlyStore = happyStore();
    mappingOnlyStore.strings.delete("gptq:runtime-binding:mb1"); // mapping alone still refuses
    const mappingOnly = makeFixture({ expectNoMailbox: true, expectedMailbox: null }, {
      "thread/read": [ok(idleThread("t1"))],
    }, mappingOnlyStore);
    const mappingOnlyResult = await mappingOnly.run();
    expect(mappingOnlyResult.exitCode).toBe(3);
    expect(problemsOf(mappingOnly.events[1])[0]).toContain("the thread's mailbox mapping names mb1");

    const clean = happyStore();
    clean.strings.delete("gptq:runtime-binding:mb1");
    clean.strings.delete(MAPPING_KEY);
    const success = makeFixture({ expectNoMailbox: true, expectedMailbox: null }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { success.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    }, clean);
    const result = await success.run();
    expect(result.exitCode).toBe(0);
    expect(success.events[0]!.expected_mailbox).toBeNull();
    expect(success.events[2]!).toMatchObject({ heartbeat: null, sidecar_exited: null, sidecar_evidence: "none" });
  });

  it("claims and queued mail refuse unless expired / allowed", async () => {
    const claimed = happyStore();
    claimed.zsets.set(CLAIM_KEYS.index("mb1"), [{ member: "c1", score: 1_700_000_005_000 }]);
    const claimedFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, claimed);
    expect((await claimedFixture.run()).exitCode).toBe(3);
    expect(claimedFixture.events[1]!.problems).toContain("mb1 holds 1 unexpired claim(s); have the agent acknowledge them first");
    expect(archiveCalls(claimedFixture)).toBe(0);

    // Expired-only claims do not block, but queued mail does without the override.
    const expired = happyStore();
    expired.zsets.set(CLAIM_KEYS.index("mb1"), [{ member: "c0", score: 1_699_999_000_000 }]);
    expired.lists.set(SESSION_KEYS.queue("mb1"), ["m1", "m2"]);
    const queuedFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, expired);
    expect((await queuedFixture.run()).exitCode).toBe(3);
    expect(queuedFixture.events[1]!.problems).toContain("mb1 has 2 queued message(s); drain them or pass --allow-pending-mail");

    const allowed = makeFixture({ allowPendingMail: true }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { allowed.pins.set(8100, null); allowed.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    }, expired);
    const result = await allowed.run();
    expect(result.exitCode).toBe(0);
    expect(allowed.events[0]!.queued).toBe(2);
    expect(archiveCalls(allowed)).toBe(1);
  });

  it("duplicate or malformed binding/mapping/registry evidence refuses or fails closed, never guesses", async () => {
    const duplicate = happyStore();
    duplicate.strings.set("gptq:runtime-binding:mb2", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e", token: "owner1", working_directory: "/w" }));
    const duplicateFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, duplicate);
    const duplicateResult = await duplicateFixture.run();
    expect(duplicateResult.exitCode).toBe(3);
    const refused = duplicateFixture.events.find((event) => event.step === "refused")!;
    expect(problemsOf(refused)[0]).toContain("more than one mailbox is bound to this thread");

    const malformed = happyStore();
    malformed.strings.set("gptq:runtime-binding:unrelated", "{not json");
    const malformedFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, malformed);
    const malformedResult = await malformedFixture.run();
    expect(malformedResult.exitCode).toBe(4);
    expect(malformedResult.code).toBe("binding_unreadable");
    expect(archiveCalls(malformedFixture)).toBe(0);

    const noRuntimeId = happyStore();
    noRuntimeId.strings.set("gptq:runtime-binding:unrelated", JSON.stringify({ client: "codex" }));
    const noRuntimeIdFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, noRuntimeId);
    expect(((await noRuntimeIdFixture.run())).code).toBe("binding_unreadable");

    const badMapping = happyStore();
    badMapping.strings.set(MAPPING_KEY, "{not json");
    const badMappingFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, badMapping);
    const badMappingResult = await badMappingFixture.run();
    expect(badMappingResult.exitCode).toBe(4);
    expect(badMappingResult.code).toBe("mapping_unreadable");

    const badRegistry = happyStore();
    badRegistry.hashes.get("gptq:registry")!.set("mb1", "{not json");
    const badRegistryFixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, badRegistry);
    const badRegistryResult = await badRegistryFixture.run();
    expect(badRegistryResult.exitCode).toBe(4);
    expect(badRegistryResult.code).toBe("registry_unreadable");
    expect(archiveCalls(badRegistryFixture)).toBe(0);
  });

  it("any Redis read error fails closed with exit 4 before any archive", async () => {
    for (const command of ["scan", "get", "time", "zcount", "llen", "hget"]) {
      const store = happyStore();
      store.failures.set(command, new Error(`injected ${command} failure`));
      const fixture = makeFixture({}, { "thread/read": times(2, ok(idleThread("t1"))), "thread/archive": [ok({})] }, store);
      const result = await fixture.run();
      expect(result.exitCode, command).toBe(4);
      expect(result.code, command).toBe("redis_error");
      expect(archiveCalls(fixture), command).toBe(0);
    }
  });

  it("a heartbeat read error during verification is unknown, never false emptiness", async () => {
    const store = happyStore();
    store.failures.set("exists", new Error("redis died after the archive"));
    const fixture = makeFixture({ verifyTimeoutMs: 120 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { fixture.pins.set(8100, null); fixture.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    }, store);
    const result = await fixture.run();
    expect(result.exitCode).toBe(1);
    expect(archiveCalls(fixture)).toBe(1);
    expect(fixture.events[fixture.events.length - 1]!.heartbeat).toBeNull();
  });

  it("RPC failure or an unexpected thread shape is an environment error, not a refusal", async () => {
    const rpcFailure = makeFixture({}, { "thread/read": [{ error: new Error("socket dead") }] });
    const rpcResult = await rpcFailure.run();
    expect(rpcResult.exitCode).toBe(4);
    expect(rpcResult.code).toBe("rpc_error");

    const shape = makeFixture({}, { "thread/read": [ok({ data: 5 })] });
    const shapeResult = await shape.run();
    expect(shapeResult.exitCode).toBe(4);
    expect(shapeResult.code).toBe("unexpected_response");
    for (const fixture of [rpcFailure, shape]) expect(archiveCalls(fixture)).toBe(0);
  });

  it("guards are re-read immediately before the archive: a late claim refuses with nothing sent", async () => {
    const store = happyStore();
    store.zcountHook.fn = (call) => {
      if (call >= 2) store.zsets.set(CLAIM_KEYS.index("mb1"), [{ member: "late", score: 1_700_000_010_000 }]);
    };
    const fixture = makeFixture({}, { "thread/read": times(2, ok(idleThread("t1"))), "thread/archive": [ok({})] }, store);
    const result = await fixture.run();
    expect(result.exitCode).toBe(3);
    expect(result.code).toBe("guards_changed");
    expect(fixture.events[fixture.events.length - 1]!.problems).toContain("mb1 now holds 1 unexpired claim(s)");
    expect(archiveCalls(fixture)).toBe(0);
  });

  it("daemon discovery failures fail loudly, including in dry-run", async () => {
    for (const dryRun of [false, true]) {
      const fixture = makeFixture({ dryRun }, { "thread/read": [ok(idleThread("t1"))] });
      fixture.discoverError.error = Object.assign(new Error("no daemon"), { mode: "socket_not_listed" });
      const result = await fixture.run();
      expect(result.exitCode, `dryRun=${dryRun}`).toBe(4);
      expect(result.code, `dryRun=${dryRun}`).toBe("daemon_socket_not_listed");
      expect(archiveCalls(fixture)).toBe(0);
    }
  });

  it("a replaced daemon identity between discovery and the archive refuses with nothing sent", async () => {
    const fixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] });
    fixture.snapshotErrors.push(Object.assign(new Error("replaced"), { mode: "daemon_replaced" }));
    const result = await fixture.run();
    expect(result.exitCode).toBe(4);
    expect(result.code).toBe("daemon_replaced");
    expect(archiveCalls(fixture)).toBe(0);
  });

  it("a lost archive response reports outcome unknown and never retries", async () => {
    const fixture = makeFixture({ verifyTimeoutMs: 150 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ error: new Error("connection reset mid-archive") }],
      "thread/loaded/list": [ok({ data: [] })],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(1);
    expect(result.outcome).toBe("outcome_unknown");
    expect(archiveCalls(fixture)).toBe(1);
    const archived = fixture.events.find((event) => event.step === "archived")!;
    expect(archived).toMatchObject({ outcome: "unknown", error: "archive request did not produce a trusted acknowledgement" });
  });

  it("an error field in the archive response is ambiguous: outcome unknown, one request", async () => {
    const fixture = makeFixture({ verifyTimeoutMs: 150 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [ok({ error: { code: "confused" } })],
      "thread/loaded/list": [ok({ data: [] })],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(1);
    expect(result.outcome).toBe("outcome_unknown");
    expect(archiveCalls(fixture)).toBe(1);
  });

  it("verification failure: still-loaded thread, live heartbeat and pinned sidecar exit 1 after the timeout", async () => {
    const store = happyStore();
    store.strings.set(SESSION_KEYS.heartbeat("mb1"), "alive");
    const fixture = makeFixture({ verifyTimeoutMs: 120 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => ({}) }], // thread stays loaded, sidecar stays pinned
      "thread/loaded/list": [ok({ data: ["t1"] })],
    }, store);
    const result = await fixture.run();
    expect(result.exitCode).toBe(1);
    expect(result.outcome).toBe("verify_failed");
    expect(archiveCalls(fixture)).toBe(1);
    const verify = fixture.events[fixture.events.length - 1]!;
    expect(verify).toMatchObject({ unloaded: false, heartbeat: true, sidecar_exited: false });
  });

  it("an unreadable sidecar identity during verification is unknown, never exit", async () => {
    const fixture = makeFixture({ verifyTimeoutMs: 120 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { fixture.pins.set(8100, new Error("procfs denied")); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    });
    const result = await fixture.run();
    expect(result.exitCode).toBe(1);
    const verify = fixture.events[fixture.events.length - 1]!;
    expect(verify.sidecar_exited).toBeNull();
    expect(verify.note).toContain("unreadable");
  });

  it.each([null, { pid: 8100, startTicks: "900", ppid: 4242, argv: ["python3", "unrelated.py"] }])("refuses an unpinned sidecar before archive: %j", async (pin) => {
    const fixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] });
    fixture.pins.set(8100, pin);
    expect((await fixture.run()).exitCode).toBe(3);
    expect(archiveCalls(fixture)).toBe(0);
    expect(fixture.events.some(event => event.sidecar_exited === true)).toBe(false);
  });

  // P004-SOL-FINAL-1: a stale registry PID reused by an unrelated process that
  // merely carries the sidecar filename and --client codex among its own
  // arguments is NOT positively pinned; the sidecar-identity refusal fires
  // before any archive request.
  it.each([
    { label: "python3 decoy", argv: ["python3", "/tmp/other.py", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis://x"] },
    { label: "node decoy", argv: ["node", "/tmp/other.js", "/opt/gptqueue/bin/gptqueue-session", "--client", "codex", "--redis-url", "redis://x"] },
  ])("a registry-named decoy argv never pins the sidecar: $label", async ({ argv }) => {
    const fixture = makeFixture({}, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { throw new Error("archive must not be reached"); } }],
    });
    fixture.pins.set(8100, { pid: 8100, startTicks: "222", ppid: 4242, argv });
    const result = await fixture.run();
    expect(result.exitCode).toBe(3);
    expect(result.outcome).toBe("refused");
    expect(problemsOf(fixture.events[1])).toContain("mailbox sidecar was not positively pinned before archive");
    expect(archiveCalls(fixture)).toBe(0);
  });

  it.each([
    { label: "conflicting client", args: ["--client", "codex", "--client", "pi", "--redis-url", "redis://x"] },
    { label: "identical client", args: ["--client", "codex", "--redis-url", "redis://x", "--client", "codex"] },
    { label: "incomplete duplicate client", args: ["--client", "codex", "--redis-url", "redis://x", "--client"] },
    { label: "duplicate Redis", args: ["--client", "codex", "--redis-url", "redis://x", "--redis-url", "redis://x"] },
    { label: "missing Redis", args: ["--client", "codex"] },
    { label: "incomplete Redis", args: ["--client", "codex", "--redis-url"] },
    { label: "unknown option", args: ["--client", "codex", "--redis-url", "redis://x", "--unknown", "value"] },
    { label: "equals client", args: ["--client=codex", "--redis-url", "redis://x"] },
    { label: "equals Redis", args: ["--client", "codex", "--redis-url=redis://x"] },
    { label: "wrong client", args: ["--client", "pi", "--redis-url", "redis://x"] },
    { label: "invalid Redis", args: ["--client", "codex", "--redis-url", "redis:///x.sock"] },
  ])("registry-named rejected native options refuse before archive: $label", async ({ args }) => {
    for (const command of [["node", "/opt/gptqueue/bin/gptqueue-session"], ["/opt/gptqueue/bin/gptqueue-session"]]) {
      const fixture = makeFixture({}, {
        "thread/read": times(2, ok(idleThread("t1"))),
        "thread/archive": [{ run: () => { throw new Error("archive must not be reached"); } }],
      });
      fixture.pins.set(8100, { ...SIDE_PIN, argv: [...command, ...args] });
      const result = await fixture.run();
      expect(result.exitCode).toBe(3);
      expect(result.outcome).toBe("refused");
      expect(problemsOf(fixture.events[1])).toContain("mailbox sidecar was not positively pinned before archive");
      expect(archiveCalls(fixture)).toBe(0);
    }
  });

  it("loaded-list pagination: follows cursors and never assumes the first 500 entries are complete", async () => {
    const fullPage = Array.from({ length: 500 }, (_, index) => `other-${index}`);
    const paginated = makeFixture({}, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { paginated.setPostArchive(true); paginated.pins.set(8100, null); return {}; } }],
      "thread/loaded/list": [ok({ data: fullPage, nextCursor: "page-2" }), ok({ data: ["unrelated"] })],
    });
    const paginatedResult = await paginated.run();
    expect(paginatedResult.exitCode).toBe(0);
    const listCalls = paginated.rpc.calls.filter((call) => call.method === "thread/loaded/list");
    expect(listCalls[1]!.params).toMatchObject({ cursor: "page-2" });

    // Exactly one full page with no cursor information cannot prove absence.
    const incomplete = makeFixture({ verifyTimeoutMs: 120 }, {
      "thread/read": times(2, ok(idleThread("t1"))),
      "thread/archive": [{ run: () => { incomplete.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: fullPage })],
    });
    const incompleteResult = await incomplete.run();
    expect(incompleteResult.exitCode).toBe(1);
    expect(incomplete.events[incomplete.events.length - 1]!.unloaded).toBeNull();
  });
});

describe("retire-thread preflight budget on a slow daemon", () => {
  it("the pre-archive re-read gets its own budget: a slow real run still archives", async () => {
    // Each thread/read costs 12 s of observation time (a busy daemon with
    // hundreds of children). Discovery plus the first read fit in one 20 s
    // budget, as a dry run shows; the re-read must not inherit what is left.
    let t = 0;
    const slowRead = (): RpcOutcome => ({ run: () => { t += 12_000; return idleThread("t1"); } });
    const f: Fixture = makeFixture({}, {
      "thread/read": [slowRead(), slowRead()],
      "thread/archive": [{ run: () => { f.pins.set(8100, null); f.setPostArchive(true); return {}; } }],
      "thread/loaded/list": [ok({ data: [] })],
    }, happyStore(), { monotonic: () => t });
    const result = await f.run();
    expect(result.code).toBeNull();
    expect(result.exitCode).toBe(0);
    expect(archiveCalls(f)).toBe(1);
    expect(steps(f)).toEqual(["check", "archived", "verify"]);
  });

  it("the same slow daemon still passes a dry run within one budget", async () => {
    let t = 0;
    const f = makeFixture({ dryRun: true }, {
      "thread/read": [{ run: () => { t += 12_000; return idleThread("t1"); } }],
    }, happyStore(), { monotonic: () => t });
    expect((await f.run()).outcome).toBe("dry_run");
  });

  const abortRejection = (error: () => Error) => (signal?: AbortSignal): Promise<never> =>
    new Promise<never>((_, reject) => { signal?.addEventListener("abort", () => reject(error()), { once: true }); });

  it.each([
    ["a /proc snapshot (proc_unreadable on abort)", "snapshot"],
    ["an RPC (plain error on abort)", "rpc"],
  ] as const)("an exhausted budget is reported as deadline_exceeded, not as the abort of %s", async (_label, stalled) => {
    vi.useFakeTimers();
    try {
      const overrides: Partial<RetireDeps> = { monotonic: () => 0 };
      if (stalled === "snapshot") {
        overrides.snapshot = abortRejection(() => Object.assign(new Error("cannot classify process 1"), { mode: "proc_unreadable" }));
      } else {
        overrides.rpc = { request: (_method, _params, _kind, signal) => abortRejection(() => new Error("socket closed"))(signal) };
      }
      const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, happyStore(), overrides);
      const pending = f.run();
      await vi.advanceTimersByTimeAsync(20_001);
      const result = await pending;
      expect(result.exitCode).toBe(4);
      expect(result.code).toBe("deadline_exceeded");
      expect(archiveCalls(f)).toBe(0);
    } finally { vi.useRealTimers(); }
  });
});

describe("P004 source-review safety regressions", () => {
  it("R7 finite preflight budget cancels and settles a nonreplying scan before any archive", async () => {
    vi.useFakeTimers();
    let cancelScan: (() => void) | undefined; let settled = false;
    const store = happyStore();
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store, {
      monotonic: () => 0,
      cancel: async () => { cancelScan?.(); },
    });
    f.setStore({ ...store, scan: async () => {
      try { return await new Promise<readonly [string, readonly string[]]>((_, reject) => { cancelScan = () => reject(new Error("cancelled")); }); }
      finally { settled = true; }
    } });
    try {
      const pending = f.run(); await vi.advanceTimersByTimeAsync(0); expect(cancelScan).toBeDefined();
      await vi.advanceTimersByTimeAsync(20_001);
      expect((await pending).exitCode).toBe(4); expect(settled).toBe(true); expect(archiveCalls(f)).toBe(0);
    } finally { vi.useRealTimers(); }
  });
  it("R7 verification cancels a stalled store observation without retaining a pending task", async () => {
    let release: (() => void) | undefined; let settled = false;
    const store = happyStore();
    const f = makeFixture({ verifyTimeoutMs: 35 }, { "thread/read": times(2, ok(idleThread("t1"))), "thread/archive": [ok({})], "thread/loaded/list": [ok({ data: [] })] }, store, {
      monotonic: () => performance.now(), cancel: async () => { release?.(); },
    });
    f.setStore({ ...store, exists: async () => {
      try { return await new Promise<number>((_, reject) => { release = () => reject(new Error("cancelled")); }); }
      finally { settled = true; }
    } });
    const start = performance.now(); expect((await f.run()).exitCode).toBe(1);
    expect(performance.now() - start).toBeLessThan(400); expect(settled).toBe(true); expect(archiveCalls(f)).toBe(1);
  });
  it.each([1, 2])("R9 refusal of Redis TIME at observation %d fails closed", async observation => {
    const store = happyStore(); let reads = 0;
    const f = makeFixture({}, { "thread/read": times(2, ok(idleThread("t1"))) }, store);
    f.setStore({ ...store, time: async () => { if (++reads === observation) throw new Error("NOPERM TIME"); return ["1700000000", "0"]; } });
    expect((await f.run()).exitCode).toBe(4); expect(archiveCalls(f)).toBe(0);
  });

  it("R7 one verification deadline bounds slow pagination and settles cancellation", async () => {
    const store = happyStore(); let pages = 0, settled = 0;
    const f = makeFixture({ verifyTimeoutMs: 65 }, {}, store, {
      monotonic: () => performance.now(), sleep: ms => new Promise(resolve => setTimeout(resolve, ms)),
      cancel: async () => undefined,
      rpc: { request: async (method, _params, _kind, signal) => {
        if (method === "thread/read") return idleThread("t1");
        if (method === "thread/archive") { f.pins.set(8100, null); f.setPostArchive(true); return {}; }
        pages++;
        try { await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(done, 25);
          function abort() { clearTimeout(timer); signal?.removeEventListener("abort", abort); reject(new Error("aborted")); }
          function done() { signal?.removeEventListener("abort", abort); resolve(); }
          signal?.addEventListener("abort", abort, { once: true });
        }); return { data: [], nextCursor: `p${pages}` }; }
        finally { settled++; }
      } },
    });
    const start = performance.now(); const result = await f.run();
    expect(result.exitCode).toBe(1); expect(performance.now() - start).toBeLessThan(400);
    expect(pages).toBeGreaterThan(1); expect(pages).toBeLessThan(5); expect(settled).toBe(pages);
    const count = pages; await new Promise(resolve => setTimeout(resolve, 40)); expect(pages).toBe(count);
  });

  it("R7 nonreplying Redis verification is cancelled and its command settles within the shared deadline", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptq-nonreply-")); const path = join(root, "sock");
    const peers = new Set<Socket>();
    const server = createServer(socket => {
      peers.add(socket); socket.on("close", () => peers.delete(socket));
      socket.on("data", data => { if (!data.toString().includes("exists")) socket.write("+OK\r\n".repeat(data.toString().match(/\*\d+\r\n/g)?.length ?? 1)); });
    });
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(path, resolve); });
    const redis = new Redis(path, { enableReadyCheck: false, maxRetriesPerRequest: 0, retryStrategy: () => null, commandTimeout: 500 }); redis.on("error", () => undefined);
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("silent Redis fixture ready timeout")), 1000);
        redis.once("ready", () => { clearTimeout(timer); resolve(); }); redis.once("error", error => { clearTimeout(timer); reject(error); });
      });
      const store = happyStore(); let commandSettled = false;
      const f = makeFixture({ verifyTimeoutMs: 40 }, { "thread/read": times(2, ok(idleThread("t1"))), "thread/archive": [ok({})], "thread/loaded/list": [ok({ data: [] })] }, store, {
        monotonic: () => performance.now(), sleep: ms => new Promise(resolve => setTimeout(resolve, ms)),
        cancel: async () => { redis.disconnect(); },
      });
      f.setStore({ ...store, exists: async key => { try { return await redis.exists(key); } finally { commandSettled = true; } } });
      const start = performance.now(); expect((await f.run()).exitCode).toBe(1);
      expect(performance.now() - start).toBeLessThan(400); expect(commandSettled).toBe(true); expect(archiveCalls(f)).toBe(1);
      expect(redis.status).toBe("end");
    } finally { redis.disconnect(); for (const peer of peers) peer.destroy(); await new Promise<void>(resolve => server.close(() => resolve())); await rm(root, { recursive: true, force: true }); }
  });

  it("R12 restore shell text preserves every variable argv element literally", async () => {
    const threadId = "t $HOME `printf bad` ' space";
    const socketPath = "/s $HOME `printf bad` ' space.sock";
    const cwd = "/w $HOME `printf bad` ' space";
    const f = makeFixture({ threadId, socketPath }, { "thread/read": [ok(idleThread(threadId, { cwd, status: { type: "notLoaded" } }))] });
    expect((await f.run()).outcome).toBe("already_unloaded");
    const restore = f.events.find(event => event.step === "already_unloaded")!.restore as string;
    const shell = restore.slice(restore.indexOf("codex resume"));
    const argv = execFileSync("/bin/sh", ["-c", `set -- ${shell}; printf '%s\n' "$@"`]).toString().trimEnd().split("\n");
    expect(argv).toEqual(["codex", "resume", threadId, "--remote", `unix://${socketPath}`, "-C", cwd, "-c", "check_for_update_on_startup=false"]);
    expect(archiveCalls(f)).toBe(0);
  });

  it.each(["gptq:runtime-binding:mb1", MAPPING_KEY])("R1 requires both records: missing %s", async (key) => {
    const store = happyStore(); store.strings.delete(key);
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store);
    expect((await f.run()).exitCode).toBe(3); expect(archiveCalls(f)).toBe(0);
  });

  it.each(["binding", "mapping", "epoch", "token", "sidecar", "sidecarArgv", "registryOwner", "parent", "noMailbox"])('R2 refuses fresh ownership changes: %s', async (change) => {
    const store = happyStore();
    if (change === "noMailbox") { store.strings.delete(MAPPING_KEY); store.strings.delete("gptq:runtime-binding:mb1"); }
    const f = makeFixture(change === "noMailbox" ? { expectNoMailbox: true, expectedMailbox: null } : {}, {
      "thread/read": [ok(idleThread("t1")), { run: () => {
        if (change === "binding") store.strings.delete("gptq:runtime-binding:mb1");
        if (change === "mapping") store.strings.delete(MAPPING_KEY);
        if (["epoch", "token", "noMailbox"].includes(change)) store.strings.set("gptq:runtime-binding:mb1", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: change === "epoch" ? "new" : "e1", token: "new", working_directory: "/w" }));
        if (change === "registryOwner") store.hashes.get("gptq:registry")!.set("mb1", JSON.stringify({ name: "mb1", pid: 8100, metadata: { client: "codex", uuid: "replacement" } }));
        if (change === "sidecarArgv") f.pins.set(8100, { ...SIDE_PIN, argv: [...SIDE_PIN.argv!, "--extra"] });
        if (change === "sidecar") f.pins.set(8100, { ...SIDE_PIN, startTicks: "new" });
        return idleThread("t1", change === "parent" ? { parentThreadId: "p1" } : {});
      } }, ok(idleThread("p1"))],
      "thread/archive": [ok({})],
    }, store);
    expect((await f.run()).exitCode).toBe(3); expect(archiveCalls(f)).toBe(0);
  });

  it("R2 permits harmless registry heartbeat timestamps to advance", async () => {
    const store = happyStore();
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1")), { run: () => {
      store.hashes.get("gptq:registry")!.set("mb1", JSON.stringify({ ...JSON.parse(store.hashes.get("gptq:registry")!.get("mb1")!), last_seen: "later" }));
      return idleThread("t1");
    } }], "thread/archive": [{ run: () => { f.pins.set(8100, null); f.setPostArchive(true); return {}; } }], "thread/loaded/list": [ok({ data: [] })] }, store);
    expect((await f.run()).exitCode).toBe(0);
  });

  it.each(["redis://", "rediss://", "redis:///0", "localhost:6379", "redis://user:secret@", "//run/redis.sock", "/run/redis.sock?db=16", "redis://host?path=/other.sock", "redis://host#other"])('R6 rejects default-dependent endpoints: %s', url => {
    const result = parseRetireThreadArgs(["t1", "--expect-no-mailbox", "--redis-url", url]);
    expect(typeof result).toBe("string"); expect(String(result)).not.toContain("secret");
  });
  it.each(["/run/redis.sock?db=15", "redis://host:6379/1", "rediss://user:password@host:6380/0"])('R6 accepts explicit native endpoints: %s', url => {
    expect(typeof parseRetireThreadArgs(["t1", "--expect-no-mailbox", "--redis-url", url])).toBe("object");
  });
  it.each(["9".repeat(400), "Infinity", "300001"])('R7 rejects unbounded budgets: %s', timeout => {
    expect(typeof parseRetireThreadArgs(["t1", "--expect-no-mailbox", "--redis-url", "redis://host", "--verify-timeout-ms", timeout])).toBe("string");
  });

  it("R8 bounds SCAN calls even when all MATCH pages are empty", async () => {
    const store = happyStore(); let calls = 0;
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store);
    f.setStore({ ...store, scan: async () => [String(++calls), []] });
    expect((await f.run()).code).toBe("binding_scan_exceeded"); expect(calls).toBe(100); expect(archiveCalls(f)).toBe(0);
  });

  it.each([1, 2])("R9 uses Redis TIME despite an ahead-of-server local clock (observation %d)", async (observation) => {
    const store = happyStore(); let timesRead = 0;
    const f = makeFixture({}, { "thread/read": times(2, ok(idleThread("t1"))) }, store);
    f.setStore({ ...store, time: async () => {
      if (++timesRead === observation) store.zsets.set(CLAIM_KEYS.index("mb1"), [{ member: "live", score: 1001 }]);
      return ["1", "0"];
    } });
    expect((await f.run()).exitCode).toBe(3); expect(archiveCalls(f)).toBe(0);
  });
  it.each([["oops", "0"], ["1", "1000000"], ["Infinity", "0"]])("R9 invalid server TIME fails closed: %j", async (seconds, micros) => {
    const store = happyStore(); const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store);
    f.setStore({ ...store, time: async () => [seconds!, micros!] });
    expect((await f.run()).exitCode).toBe(4); expect(archiveCalls(f)).toBe(0);
  });

  it.each([{ runtime_id: "t1" }, { client: "pi", runtime_id: "t1", epoch: "e", token: "secret-token", working_directory: "/w" }, { client: "codex", runtime_id: "t1", epoch: "e", working_directory: "/w", surprise: true }])("R10 rejects invalid or foreign stored bindings", async (binding) => {
    const store = happyStore(); store.strings.set("gptq:runtime-binding:mb1", JSON.stringify(binding));
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store);
    expect([3,4]).toContain((await f.run()).exitCode); expect(archiveCalls(f)).toBe(0); expect(JSON.stringify(f.events)).not.toContain("secret-token");
  });
  it.each([5, {}, [], ""])('R10 malformed parent never becomes a root', async parentThreadId => {
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1", { parentThreadId }))] });
    expect((await f.run()).code).toBe("unexpected_response"); expect(archiveCalls(f)).toBe(0);
  });
  it("R10 rejects a mapping containing only agent", async () => {
    const store = happyStore(); store.strings.set(MAPPING_KEY, JSON.stringify({ agent: "mb1" }));
    const f = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] }, store);
    expect((await f.run()).code).toBe("mapping_unreadable"); expect(archiveCalls(f)).toBe(0);
  });
});

// ---------------------------------------------------------------- real Redis

describe("retire-thread guards (private test Redis via storeOf)", () => {
  let TEST_REDIS_URL: string;
  let redis: Redis;

  beforeEach(async () => {
    TEST_REDIS_URL = testRedisUrl();
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("dry-run passes over real Redis state, with real SCAN pagination", async () => {
    // 1200 unrelated bindings force three COUNT-batched pages before mb1's.
    for (let index = 0; index < 1200; index += 1) {
      await redis.set(`gptq:runtime-binding:bulk-${index}`, JSON.stringify({ client: "codex", runtime_id: `bulk-${index}`, epoch: "e", token: "owner1", working_directory: "/w" }));
    }
    await redis.set("gptq:runtime-binding:mb1", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e", token: "owner1", working_directory: "/w" }));
    await redis.set(MAPPING_KEY, JSON.stringify({ agent: "mb1", working_directory: "/w" }));
    await redis.hset("gptq:registry", "mb1", JSON.stringify({ name: "mb1", registered_at: "2026-10-01T00:00:00Z", pid: 8100 }));
    await redis.zadd(CLAIM_KEYS.index("mb1"), "1699999940000", "expired-claim"); // before the fixture clock epoch

    const fixture = makeFixture({ dryRun: true }, { "thread/read": [ok(idleThread("t1"))] });
    // Swap the fake store for the real adapter; the fixture keeps its knobs.
    fixture.setStore(storeOf(redis));
    const result = await fixture.run();
    expect(result.exitCode).toBe(0);
    expect(fixture.events[0]!).toMatchObject({ bound_mailbox: "mb1", mapped_mailbox: "mb1", unexpired_claims: 0, queued: 0 });
  });

  it("an unexpired real claim refuses", async () => {
    await redis.set("gptq:runtime-binding:mb1", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e", token: "owner1", working_directory: "/w" }));
    await redis.set(MAPPING_KEY, JSON.stringify({ agent: "mb1", working_directory: "/w" }));
    await redis.hset("gptq:registry", "mb1", JSON.stringify({ name: "mb1", pid: 8100 }));
    const [seconds, micros] = await redis.time();
    await redis.zadd(CLAIM_KEYS.index("mb1"), String(Number(seconds) * 1000 + Math.floor(Number(micros) / 1000) + 60_000), "live-claim");
    const fixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] });
    fixture.setStore(storeOf(redis));
    const result = await fixture.run();
    expect(result.exitCode).toBe(3);
    expect(problemsOf(fixture.events[1])[0]).toContain("holds 1 unexpired claim(s)");
  });

  it("a malformed binding anywhere in the real keyspace fails closed", async () => {
    await redis.set("gptq:runtime-binding:broken", "{oops");
    await redis.set("gptq:runtime-binding:mb1", JSON.stringify({ client: "codex", runtime_id: "t1", epoch: "e", token: "owner1", working_directory: "/w" }));
    const fixture = makeFixture({}, { "thread/read": [ok(idleThread("t1"))] });
    fixture.setStore(storeOf(redis));
    const result = await fixture.run();
    expect(result.exitCode).toBe(4);
    expect(result.code).toBe("binding_unreadable");
  });
});
