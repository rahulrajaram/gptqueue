import { describe, expect, it } from "vitest";
import { createPiInteractiveAdapter, hasFreshPiInteractiveTurnEvidence, piInteractiveQualificationRoute } from "./qualification-pi-interactive.js";

describe("Pi interactive qualification adapter", () => {
  it("declares an actual PTY route separately from RPC", () => {
    const adapter = createPiInteractiveAdapter({ installedRoot: "/does/not/launch" });
    expect(adapter.spec.id).toBe(piInteractiveQualificationRoute);
    expect(adapter.spec.host).toBe("pi");
    expect(adapter.spec.modelBacked).toBe(true);
  });

  it("preflights the CLI and built extension without launching it", async () => {
    await expect(createPiInteractiveAdapter({ installedRoot: "/does/not/launch" }).preflight(new AbortController().signal)).resolves.toEqual({
      kind: "setup_gap",
      detail: "Installed Pi CLI, built GPTQueue extension, or existing Pi auth is unavailable",
    });
  });

  it("requires a fresh completed assistant marker and correlated runtime result", () => {
    const expected = { marker: "PI_READY", agent: "pi-agent", runtimeId: "pi-session" } as const;
    const history = [
      { type: "message", message: { role: "assistant", content: [{ type: "text", text: "PI_READY" }], stopReason: "stop" } },
      { type: "message", message: { role: "user", content: [{ type: "text", text: "Call get_runtime_status then PI_READY" }] } },
      { type: "message", message: { role: "assistant", content: [{ type: "toolCall", id: "runtime-1", name: "get_runtime_status", arguments: {} }] } },
      { type: "message", message: { role: "toolResult", toolCallId: "runtime-1", toolName: "get_runtime_status", content: [{ type: "text", text: JSON.stringify({ status: "ok", agent: "pi-agent", runtime: { client: "pi", runtime_id: "pi-session" } }) }] } },
      { type: "message", message: { role: "assistant", content: [{ type: "text", text: "PI_READY" }], stopReason: "stop" } },
    ];
    expect(hasFreshPiInteractiveTurnEvidence(history, 2, expected)).toBe(true);
    expect(hasFreshPiInteractiveTurnEvidence(history, 0, expected)).toBe(true);
    expect(hasFreshPiInteractiveTurnEvidence(history.slice(0, -1), 2, expected)).toBe(false);
    expect(hasFreshPiInteractiveTurnEvidence(history.map((entry, index) => index === 3
      ? { ...entry, message: { ...(entry as any).message, content: [{ type: "text", text: JSON.stringify({ status: "ok", agent: "other", runtime: { client: "pi", runtime_id: "pi-session" } }) }] } }
      : entry), 2, expected)).toBe(false);
  });
});
