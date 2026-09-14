import { describe, expect, it } from "vitest";
import { frozenPairMatrix, frozenRoutes, pairKey } from "./qualification-routes.js";
import { isModelParticipant, type Participant } from "./qualification-types.js";

describe("qualification route contracts", () => {
  it("freezes the complete 25-route population and 625 unique ordered pairs", () => {
    const pairs = frozenPairMatrix;
    expect(frozenRoutes).toHaveLength(25);
    expect(pairs).toHaveLength(625);
    expect(new Set(pairs.map(({ pairId }) => pairId)).size).toBe(625);
    expect(pairs.filter(({ sender, receiver }) => sender === receiver)).toHaveLength(25);
    expect(pairs).toContainEqual(expect.objectContaining({ pairId: pairKey("codex-headless", "codex-headless") }));
    expect(pairs).toContainEqual(expect.objectContaining({ pairId: pairKey("codex-headless", "pi-sdk") }));
    expect(pairs).toContainEqual(expect.objectContaining({ pairId: pairKey("pi-sdk", "codex-headless") }));
  });

  it("keeps model and generic participant capabilities disjoint", () => {
    const model = { kind: "model" } as Participant;
    const generic = { kind: "generic" } as Participant;
    expect(isModelParticipant(model)).toBe(true);
    expect(isModelParticipant(generic)).toBe(false);
  });

  it("preserves blocked prerequisites separately from setup gaps", () => {
    const blocked = { kind: "blocked_prerequisite", detail: "provider tier" } as const;
    const gap = frozenRoutes.find(({ id }) => id === "gemini-cli")?.availability;
    expect(blocked.kind).toBe("blocked_prerequisite");
    expect(gap?.kind).toBe("setup_gap");
    expect(blocked.kind).not.toBe(gap?.kind);
  });
});
