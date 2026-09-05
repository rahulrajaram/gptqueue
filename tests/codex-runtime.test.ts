import { describe, expect, it } from "vitest";
import { createCodexRuntime, type CodexRpcClient } from "../src/registered-shell/codex-runtime.js";

const binding = { client: "codex" as const, runtime_id: "thread-1", epoch: "e1", working_directory: "/workspace" };
const fake = (responses: Record<string, Record<string, unknown>>): CodexRpcClient & { calls: string[] } => {
  const value = { calls: [] as string[], request: async (method: string) => { value.calls.push(method); return responses[method] ?? {}; }, close: async () => {} };
  return value;
};

describe("Codex runtime", () => {
  it("validates exact thread and starts an idle turn", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "turn/start": { turn: { id: "turn-1" } } });
    const runtime = await createCodexRuntime(binding, { client }); const result = await runtime.activate({ operation_id: "op-1", prompt: "hello" }, new AbortController().signal);
    expect(result).toEqual({ status: "started", turn_id: "turn-1" }); expect(client.calls).toEqual(["thread/read", "thread/read", "turn/start"]);
  });
  it("returns busy and never steers an active thread", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "t", status: "inProgress", items: [] }] } }); const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-2", prompt: "hello" }, new AbortController().signal)).toEqual({ status: "busy" }); expect(client.calls).not.toContain("turn/start");
  });
  it("rejects a mismatched exact thread", async () => {
    await expect(createCodexRuntime(binding, { client: fake({ "thread/read": { id: "thread-1", cwd: "/other" } }) })).rejects.toThrow(/identity/);
  });
  it("requires the exact GPTQueue owner for an expected agent", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "mcpServer/tool/call": { structuredContent: { agent: "agent-a" } } });
    await expect(createCodexRuntime(binding, { client, expectedAgent: "agent-a" })).resolves.toBeDefined();
    await expect(createCodexRuntime(binding, { client: fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [] }, "mcpServer/tool/call": { structuredContent: { agent: "agent-b" } } }), expectedAgent: "agent-a" })).rejects.toThrow(/own this GPTQueue/);
  });
  it("recovers terminal history without replaying and honors recover_only", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "turn-old", status: "failed", items: [{ type: "userMessage", clientId: "op-old", content: [{ type: "text", text: "old" }] }] }] } });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-old", prompt: "old" }, new AbortController().signal)).toEqual({ status: "completed", turn_id: "turn-old" });
    expect(await runtime.activate({ operation_id: "op-new", prompt: "new", recover_only: true }, new AbortController().signal)).toEqual({ status: "ambiguous" });
    expect(client.calls).not.toContain("turn/start");
  });
  it("deduplicates an existing client operation", async () => {
    const client = fake({ "thread/read": { id: "thread-1", cwd: "/workspace", turns: [{ id: "turn-existing", status: "inProgress", items: [{ type: "userMessage", clientId: "op-1", content: [{ type: "text", text: "hello" }] }] }] } });
    const runtime = await createCodexRuntime(binding, { client });
    expect(await runtime.activate({ operation_id: "op-1", prompt: "hello" }, new AbortController().signal)).toEqual({ status: "started", turn_id: "turn-existing" });
    expect(client.calls).not.toContain("turn/start");
  });
});
