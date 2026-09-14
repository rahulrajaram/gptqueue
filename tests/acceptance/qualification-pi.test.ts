import { describe, expect, it } from "vitest";
import { createPiAdapters, hasPiAssistantRuntimeEvidence, piQualificationRoute, validatePiRedisUrl } from "./qualification-pi.js";

const signal = new AbortController().signal;

describe("Pi qualification adapters", () => {
  it("exposes the RPC route without collapsing it into another Pi mode", () => {
    const set = createPiAdapters({ installedRoot: "/does/not/launch" });
    expect(set.adapters).toHaveLength(1);
    expect(set.adapters[0]?.spec.id).toBe(piQualificationRoute);
    expect(set.adapters[0]?.spec.host).toBe("pi");
  });

  it("requires a private loopback Redis db15 endpoint", () => {
    expect(() => validatePiRedisUrl("redis://127.0.0.1:6379/15")).not.toThrow();
    expect(() => validatePiRedisUrl("redis://127.0.0.1:6379/0")).toThrow(/db15/iu);
    expect(() => validatePiRedisUrl("redis://example.invalid:6379/15")).toThrow(/loopback/iu);
  });

  it("keeps process launch behind preflight and launch", async () => {
    const result = await createPiAdapters({ installedRoot: "/does/not/launch" }).adapters[0]!.preflight(signal);
    expect(result.kind).toBe("setup_gap");
  });

  it("rejects a marker that appears only in the user prompt or tool output", () => {
    const userOnly = [{ role: "user", content: [{ type: "text", text: "PI_RPC_READY" }] }];
    const toolOnly = [{ role: "assistant", content: [{ type: "toolCall", id: "call-1", name: "get_runtime_status", arguments: {} }] }, {
      role: "toolResult", toolCallId: "call-1", toolName: "get_runtime_status", content: [{ type: "text", text: '{"status":"ok","agent":"agent-a","runtime":{"client":"pi","runtime_id":"runtime-a"},"marker":"PI_RPC_READY"}' }],
    }];
    const expected = { marker: "PI_RPC_READY", agent: "agent-a", runtimeId: "runtime-a" } as const;
    expect(hasPiAssistantRuntimeEvidence(userOnly, expected)).toBe(false);
    expect(hasPiAssistantRuntimeEvidence(toolOnly, expected)).toBe(false);
  });

  it("requires the exact runtime binding for the assistant response", () => {
    const history = [
      { role: "assistant", content: [{ type: "toolCall", id: "call-2", name: "get_runtime_status", arguments: {} }, { type: "text", text: "PI_RPC_READY" }] },
      { role: "toolResult", toolCallId: "call-2", toolName: "get_runtime_status", content: [{ type: "text", text: '{"status":"ok","agent":"other-agent","runtime":{"client":"pi","runtime_id":"runtime-a"}}' }] },
    ];
    expect(hasPiAssistantRuntimeEvidence(history, { marker: "PI_RPC_READY", agent: "agent-a", runtimeId: "runtime-a" })).toBe(false);
    expect(hasPiAssistantRuntimeEvidence(history.map(row => row.role === "toolResult" ? { ...row, content: [{ type: "text", text: '{"status":"ok","agent":"agent-a","runtime":{"client":"pi","runtime_id":"runtime-a"}}' }] } : row), { marker: "PI_RPC_READY", agent: "agent-a", runtimeId: "runtime-a" })).toBe(true);
  });
});
