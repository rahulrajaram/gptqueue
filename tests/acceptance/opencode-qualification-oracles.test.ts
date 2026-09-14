import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { childReadinessEvidence } from "./opencode-qualification-oracles.js";

type JsonObject = Record<string, unknown>;
const cd3aPath = ".gptqueue/repair-qualification/20260912/opencode-repair/opencode-conformance-2026-09-12T21-06-52-202Z-cd3a3a5a-14b4-427b-bb33-d762670c9f3c/receipt.json";
const historicalPath = ".gptqueue/repair-qualification/20260912/opencode-repair/1134a06c-8ad6-49ec-8797-195c8e1a1d67/receipt.json";
const cd3aAgent = "gptqueue-opencode-ses_f688db6f8ffeux0BXlWpqhAisb";
const cd3aRuntimeID = "ses_f688db6f8ffeux0BXlWpqhAisb";
const historicalAgent = "gptqueue-opencode-ses_f68db6a35ffegDFtJfLBOMahE2";
const historicalRuntimeID = "ses_f68db6a35ffegDFtJfLBOMahE2";

const readJson = (path: string): JsonObject => JSON.parse(readFileSync(path, "utf8")) as JsonObject;
const childHistory = (receipt: JsonObject): unknown => {
  const diagnostic = receipt.native_task_diagnostic as JsonObject | undefined;
  const child = diagnostic?.child as JsonObject | undefined;
  if (child?.history !== undefined) return child.history;
  const creation = receipt.creation_phase as JsonObject | undefined;
  return creation?.child_history;
};

const syntheticHistory = (assistantText: string, runtimeID: string, agent = `gptqueue-opencode-${runtimeID}`, role = "assistant"): readonly unknown[] => [
  { info: { role }, parts: [{ type: "text", text: assistantText }] },
  { info: { role: "assistant" }, parts: [{ type: "tool", tool: "gptqueue_get_runtime_status", state: {
    status: "completed", output: { structuredContent: { agent, runtime: { runtime_id: runtimeID } } },
  } }] },
];

describe("OpenCode child readiness oracle formats", () => {
  it("accepts the actual labeled-bound-agent format from the retained cd3a diagnostic", () => {
    const evidence = childReadinessEvidence(childHistory(readJson(cd3aPath)), cd3aAgent, cd3aRuntimeID);
    expect(evidence).toMatchObject({ reportedAgent: cd3aAgent, runtimeID: cd3aRuntimeID });
  });

  it("accepts the actual inline format from the older retained receipt", () => {
    const evidence = childReadinessEvidence(childHistory(readJson(historicalPath)), historicalAgent, historicalRuntimeID);
    expect(evidence).toMatchObject({ reportedAgent: historicalAgent, runtimeID: historicalRuntimeID });
  });

  it("rejects a wrong bound name even when the expected name appears in a peer list", () => {
    const text = `CHILD_READY\n\nBound gptqueue-opencode name: gptqueue-opencode-wrong\nAgents listed: ${cd3aAgent}`;
    expect(childReadinessEvidence(syntheticHistory(text, cd3aRuntimeID, cd3aAgent), cd3aAgent, cd3aRuntimeID)).toBeUndefined();
  });

  it("rejects duplicate or conflicting bound-agent fields", () => {
    const text = `CHILD_READY\nBound gptqueue-opencode name: ${cd3aAgent}\nBound gptqueue-opencode name: ${cd3aAgent}`;
    expect(childReadinessEvidence(syntheticHistory(text, cd3aRuntimeID, cd3aAgent), cd3aAgent, cd3aRuntimeID)).toBeUndefined();
    const conflicting = `CHILD_READY\nBound gptqueue-opencode name: ${cd3aAgent}\nBound gptqueue-opencode name: gptqueue-opencode-other`;
    expect(childReadinessEvidence(syntheticHistory(conflicting, cd3aRuntimeID, cd3aAgent), cd3aAgent, cd3aRuntimeID)).toBeUndefined();
  });

  it("rejects markers from user or tool content and wrong runtime identity", () => {
    const userOnly = syntheticHistory(`CHILD_READY ${cd3aAgent}`, cd3aRuntimeID, cd3aAgent, "user");
    expect(childReadinessEvidence(userOnly, cd3aAgent, cd3aRuntimeID)).toBeUndefined();
    const toolOnly = [{ info: { role: "assistant" }, parts: [{ type: "tool", tool: "gptqueue_get_runtime_status", state: {
      status: "completed", output: { structuredContent: { agent: cd3aAgent, runtime: { runtime_id: cd3aRuntimeID } } },
    } }] }];
    expect(childReadinessEvidence(toolOnly, cd3aAgent, cd3aRuntimeID)).toBeUndefined();
    expect(childReadinessEvidence(syntheticHistory(`CHILD_READY ${cd3aAgent}`, "wrong-runtime", cd3aAgent), cd3aAgent, cd3aRuntimeID)).toBeUndefined();
  });
});
