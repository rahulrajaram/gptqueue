import { describe, expect, it, vi } from "vitest";
import { bootstrapCodexBinding, findOwnCodexThread } from "../src/registered-shell/codex-bootstrap.js";

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
