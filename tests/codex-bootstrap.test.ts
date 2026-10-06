import { describe, expect, it, vi } from "vitest";
import { bootstrapCodexBinding, findOwnCodexThread, withBindingState, type CodexBindingState } from "../src/registered-shell/codex-bootstrap.js";

const own = "01a10062-ae13-76e3-8760-5289291f4d03";
const other = "01a10063-5642-72c2-91e3-f19d65a82b54";
const agent = "owned-agent";
const cwd = "/workspace/owned";
const rpc = (statuses: Record<string, Record<string, unknown> | undefined>) => ({
  close: vi.fn(async () => undefined),
  request: vi.fn(async (method: string, params: Record<string, unknown>) => {
    if (method === "thread/loaded/list") return { data: Object.keys(statuses) };
    if (method === "thread/read") return { thread: { id: params.threadId, cwd } };
    if (method === "mcpServer/tool/call") return { structuredContent: statuses[String(params.threadId)] };
    throw Error("unexpected method");
  }),
});

describe("Codex native startup association", () => {
  it("binds before a model turn only to the native thread owning this exact mailbox", async () => {
    const client = rpc({ [own]: { status: "ok", agent, runtime: null }, [other]: { status: "ok", agent: "foreign", runtime: null } });
    const bind = vi.fn(async (value) => ({ activation_ready: true, runtime: value }));
    expect(await bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, AbortSignal.timeout(1000), client)).toBe(true);
    expect(bind).toHaveBeenCalledExactlyOnceWith({ client: "codex", runtime_id: own, epoch: own, working_directory: cwd });
    expect(client.close).toHaveBeenCalledOnce();
  });

  it("refuses foreign and ambiguous native ownership", async () => {
    const foreign = rpc({ [other]: { status: "ok", agent: "foreign", runtime: null } });
    expect(await findOwnCodexThread(foreign, agent, cwd, AbortSignal.timeout(1000))).toBeUndefined();
    const ambiguous = rpc({ [own]: { status: "ok", agent, runtime: null }, [other]: { status: "ok", agent, runtime: null } });
    expect(await findOwnCodexThread(ambiguous, agent, cwd, AbortSignal.timeout(1000))).toBeUndefined();
  });

  it("refuses malformed native inventory and mismatched runtime identity", async () => {
    const client = rpc({ [own]: { status: "ok", agent, runtime: { runtime_id: other } } });
    expect(await findOwnCodexThread(client, agent, cwd, AbortSignal.timeout(1000))).toBeUndefined();
    client.request.mockImplementationOnce(async () => ({ data: Array(129).fill(own) }));
    expect(await findOwnCodexThread(client, agent, cwd, AbortSignal.timeout(1000))).toBeUndefined();
  });

  it("stops on shutdown without binding and closes the daemon connection", async () => {
    const stop = new AbortController();
    const client = rpc({});
    const bind = vi.fn(async () => ({ activation_ready: true }));
    const waiting = bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, stop.signal, client);
    stop.abort();
    expect(await waiting).toBe(false);
    expect(bind).not.toHaveBeenCalled();
    expect(client.close).toHaveBeenCalledOnce();
  });

  it("closes on a failed bind and never reports readiness from another runtime", async () => {
    const client = rpc({ [own]: { status: "ok", agent, runtime: null } });
    const bind = vi.fn(async () => ({ activation_ready: true, runtime: { runtime_id: other } }));
    expect(await bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, AbortSignal.timeout(1000), client)).toBe(false);
    expect(client.close).toHaveBeenCalledOnce();
  });
});

describe("Codex binding supervision (WK-6)", () => {
  const fast = { fastAttempts: 3, fastDelayMs: 2, maxBackoffMs: 10, random: () => 1 };
  /** A daemon that refuses `downFor` inventory calls, then serves `statuses`. */
  const flaky = (downFor: number, statuses: Record<string, Record<string, unknown>>) => {
    let inventories = 0;
    const listeners = new Set<(n: { method: string; params: Record<string, unknown> }) => void>();
    const client = {
      inventories: () => inventories,
      notify: (method: string) => { for (const l of listeners) l({ method, params: { threadId: own } }); },
      onNotification: (l: (n: { method: string; params: Record<string, unknown> }) => void) => { listeners.add(l); return () => listeners.delete(l); },
      close: vi.fn(async () => undefined),
      request: vi.fn(async (method: string, params: Record<string, unknown>) => {
        if (method === "thread/loaded/list") {
          inventories += 1;
          if (inventories <= downFor) throw new Error("connect ECONNREFUSED /run/codex.sock");
          return { data: Object.keys(statuses) };
        }
        if (method === "thread/read") return { thread: { id: params.threadId, cwd } };
        if (method === "mcpServer/tool/call") return { structuredContent: statuses[String(params.threadId)] };
        throw Error("unexpected method");
      }),
    };
    return client;
  };

  it("keeps retrying past the old 30-attempt budget and binds once the daemon recovers", async () => {
    const client = flaky(40, { [own]: { status: "ok", agent, runtime: null } });
    const bind = vi.fn(async (value) => ({ activation_ready: true, runtime: value }));
    const states: string[] = [];
    const bound = await bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, AbortSignal.timeout(4_000), client,
      { ...fast, onState: (s) => states.push(`${s.readiness}:${s.last_binding_outcome}`) });
    expect(bound).toBe(true);
    expect(client.inventories()).toBe(41);
    expect(bind).toHaveBeenCalledExactlyOnceWith({ client: "codex", runtime_id: own, epoch: own, working_directory: cwd });
    // State changes only, not one entry per attempt.
    expect(states).toEqual(["unbound_retrying:null", "unbound_retrying:daemon_unavailable", "unbound_retrying:daemon_unavailable", "bound:bound"]);
    expect(client.close).toHaveBeenCalledOnce();
  }, 10_000);

  it("never guesses between several owned threads, and reports the retrying state truthfully", async () => {
    const client = flaky(0, { [own]: { status: "ok", agent, runtime: null }, [other]: { status: "ok", agent, runtime: null } });
    const bind = vi.fn(async (value) => ({ activation_ready: true, runtime: value }));
    let last: CodexBindingState | undefined;
    const stop = new AbortController();
    const run = bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, stop.signal, client,
      { ...fast, now: () => 1_000_000, onState: (s) => { last = s; } });
    await vi.waitFor(() => expect(client.inventories()).toBeGreaterThan(5));
    expect(last).toMatchObject({ readiness: "unbound_retrying", last_binding_outcome: "no_unique_owned_thread" });
    expect(Date.parse(last!.next_attempt_at!)).toBeGreaterThan(1_000_000);
    stop.abort();
    expect(await run).toBe(false);
    expect(bind).not.toHaveBeenCalled();
    expect(last).toMatchObject({ readiness: "stopped", next_attempt_at: null });
  });

  it("stops retrying once bound (by itself or by the hook's bind_runtime) and on shutdown", async () => {
    // Bound here: no daemon calls after binding.
    const client = flaky(2, { [own]: { status: "ok", agent, runtime: null } });
    const bind = vi.fn(async (value) => ({ activation_ready: true, runtime: value }));
    expect(await bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, AbortSignal.timeout(4_000), client, fast)).toBe(true);
    const after = client.request.mock.calls.length;
    await new Promise((r) => setTimeout(r, 50));
    expect(client.request.mock.calls.length).toBe(after);

    // Bound by the hook first: the supervisor sees the runtime and binds nothing.
    let runtime: unknown = null;
    const hookClient = flaky(1_000, {});
    const hookBind = vi.fn(async () => ({ activation_ready: true }));
    const waiting = bootstrapCodexBinding({ bind: hookBind, status: () => ({ runtime, activation_ready: runtime !== null }) }, agent, cwd, AbortSignal.timeout(4_000), hookClient, fast);
    await vi.waitFor(() => expect(hookClient.inventories()).toBeGreaterThan(2));
    runtime = { runtime_id: own };
    expect(await waiting).toBe(true);
    expect(hookBind).not.toHaveBeenCalled();

    // Shutdown: no attempt after the abort.
    const down = flaky(1_000, {});
    const stop = new AbortController();
    const stopped = bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, stop.signal, down, fast);
    await vi.waitFor(() => expect(down.inventories()).toBeGreaterThan(4));
    stop.abort();
    const atAbort = down.inventories();
    expect(await stopped).toBe(false);
    const atStop = down.inventories();
    expect(atStop - atAbort).toBeLessThanOrEqual(1); // at most the attempt already in flight
    await new Promise((r) => setTimeout(r, 50));
    expect(down.inventories()).toBe(atStop);
    expect(down.close).toHaveBeenCalledOnce();
  });

  it("a daemon notification ends a long backoff wait at once", async () => {
    const client = flaky(4, { [own]: { status: "ok", agent, runtime: null } });
    const bind = vi.fn(async (value) => ({ activation_ready: true, runtime: value }));
    const run = bootstrapCodexBinding({ bind, status: () => ({}) }, agent, cwd, AbortSignal.timeout(8_000), client,
      { fastAttempts: 1, fastDelayMs: 60_000, maxBackoffMs: 60_000, random: () => 1 });
    await vi.waitFor(() => expect(client.inventories()).toBe(1));
    for (let i = 0; i < 4; i++) {
      await new Promise((r) => setTimeout(r, 20));
      client.notify("thread/status/changed");
    }
    expect(await run).toBe(true);
  });
});

describe("get_runtime_status while unbound (WK-6)", () => {
  const retrying: CodexBindingState = { readiness: "unbound_retrying", binding_attempts: 31,
    next_attempt_at: "2026-10-06T05:00:30.000Z", last_binding_outcome: "daemon_unavailable" };
  it("reports unbound_retrying with the next attempt time, and nothing once a runtime is bound", () => {
    const unbound = { status: "ok", agent, activation_ready: false, runtime: null };
    expect(withBindingState(unbound, retrying)).toEqual({ ...unbound, readiness: "unbound_retrying", binding_attempts: 31,
      next_attempt_at: "2026-10-06T05:00:30.000Z", last_binding_outcome: "daemon_unavailable" });
    const bound = { status: "ok", agent, activation_ready: true, runtime: { runtime_id: own } };
    expect(withBindingState(bound, retrying)).toEqual(bound);
    expect(withBindingState(unbound, undefined)).toEqual(unbound);
  });
});

describe("an incomplete hook bind does not stop binding supervision (review round 3)", () => {
  it("keeps retrying while the runtime is recorded but not activation-ready, then binds", async () => {
    // The hook's bind recorded the runtime, then failed (mailbox restore / dispatcher start):
    // runtime is set, activation_ready is false.
    let status: Record<string, unknown> = { status: "ok", agent, activation_ready: false, runtime: { runtime_id: own } };
    let inventories = 0;
    const client = {
      close: vi.fn(async () => undefined),
      request: vi.fn(async (method: string, params: Record<string, unknown>) => {
        if (method === "thread/loaded/list") { inventories += 1; return { data: inventories < 4 ? [] : [own] }; }
        if (method === "thread/read") return { thread: { id: params.threadId, cwd } };
        if (method === "mcpServer/tool/call") return { structuredContent: { status: "ok", agent, runtime: { runtime_id: own } } };
        throw Error("unexpected method");
      }),
    };
    const bind = vi.fn(async (value: { runtime_id: string }) => {
      status = { status: "ok", agent, activation_ready: true, runtime: value };
      return status;
    });
    const states: CodexBindingState[] = [];
    const bound = await bootstrapCodexBinding({ bind, status: () => status }, agent, cwd, AbortSignal.timeout(4_000), client,
      { fastAttempts: 3, fastDelayMs: 2, maxBackoffMs: 10, random: () => 1, onState: (s) => states.push(s) });
    expect(bound).toBe(true);
    expect(bind).toHaveBeenCalledOnce();
    expect(states.some((s) => s.readiness === "binding_incomplete")).toBe(true);
    expect(states.at(-1)).toMatchObject({ readiness: "bound" });
  });

  it("get_runtime_status reports binding_incomplete for a runtime without a live dispatcher", () => {
    const state: CodexBindingState = { readiness: "unbound_retrying", binding_attempts: 2,
      next_attempt_at: "2026-10-06T06:00:00.000Z", last_binding_outcome: "bind_failed" };
    const incomplete = { status: "ok", agent, activation_ready: false, runtime: { runtime_id: own } };
    expect(withBindingState(incomplete, state)).toMatchObject({ readiness: "binding_incomplete", binding_attempts: 2,
      next_attempt_at: "2026-10-06T06:00:00.000Z", last_binding_outcome: "bind_failed" });
  });
});
