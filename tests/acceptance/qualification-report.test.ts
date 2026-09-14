import { describe, expect, it } from "vitest";
import { buildQualificationReport } from "./qualification-report.js";
import { frozenDimensionObligations, frozenPairMatrix, frozenRoutes } from "./qualification-routes.js";
import type { ParticipantIdentity, RouteSpec } from "./qualification-types.js";

const availableRoutes: readonly RouteSpec[] = frozenRoutes.map((route) => ({ ...route, availability: { kind: "available" } }));
const identity = (route: ParticipantIdentity["route"], agent: string): ParticipantIdentity => ({ participantId: agent, route, agent, hostRuntimeId: `${agent}-runtime`, cwdHash: `${agent}-cwd`, profileHash: `${agent}-profile`, epochHash: `${agent}-epoch` });
const communication = (pairId: string, attemptId: string, senderRoute: ParticipantIdentity["route"] = "codex-appserver", receiverRoute: ParticipantIdentity["route"] = senderRoute, status: "completed" | "failed" = "completed", outcome: "meets" | "does_not_meet" = status === "completed" ? "meets" : "does_not_meet") => ({ kind: "communication" as const, attemptId, pairId, sender: identity(senderRoute, `${attemptId}-sender`), receiver: identity(receiverRoute, `${attemptId}-receiver`), status, outcome, execution: { status: status === "completed" ? "completed" as const : "failed" as const }, raw: [{ path: "raw.json", sha256: "hash", sourceRevision: "source", oracleRevision: "oracle" }] });

describe("qualification report ledger", () => {
  it("materializes every 625 obligation and never treats inventory as executed", () => {
    const report = buildQualificationReport({ routes: availableRoutes, obligations: frozenPairMatrix, attempts: [] });
    expect(report.rows).toHaveLength(625);
    expect(report.counts.unrun).toBe(625);
    expect(report.counts.completed).toBe(0);
  });

  it("distinguishes blocked and setup-gap prerequisites", () => {
    const routes = availableRoutes.map((route) => route.id === "codex-appserver" ? { ...route, availability: { kind: "blocked_prerequisite" as const, detail: "auth" } } : route.id === "codex-interactive" ? { ...route, availability: { kind: "setup_gap" as const, detail: "launcher" } } : route);
    const report = buildQualificationReport({ routes, obligations: [{ pairId: "codex-appserver->codex-interactive", sender: "codex-appserver", receiver: "codex-interactive" }], attempts: [] });
    expect(report.rows.map(({ status }) => status)).toEqual(["blocked"]);
    const gap = buildQualificationReport({ routes: routes.map((route) => route.id === "codex-appserver" ? { ...route, availability: { kind: "available" as const } } : route), obligations: [{ pairId: "codex-appserver->codex-interactive", sender: "codex-appserver", receiver: "codex-interactive" }], attempts: [] });
    expect(gap.rows[0]?.status).toBe("setup_gap");
  });

  it("requires explicit duplicate admission and raw identity evidence", () => {
    const pair = { pairId: "codex-appserver->codex-appserver", sender: "codex-appserver" as const, receiver: "codex-appserver" as const };
    const first = communication(pair.pairId, "first");
    const second = communication(pair.pairId, "second", pair.sender, pair.receiver, "failed");
    expect(() => buildQualificationReport({ routes: availableRoutes, obligations: [pair], attempts: [first, second] })).toThrow(/explicit admittedAttemptIds/);
    const report = buildQualificationReport({ routes: availableRoutes, obligations: [pair], attempts: [first, second], admittedAttemptIds: ["first"] });
    expect(report.counts.completed).toBe(1);
    expect(report.rows[0]?.attemptId).toBe("first");
  });

  it("does not invent automatic or initiative passes", () => {
    const report = buildQualificationReport({ routes: availableRoutes, obligations: [], attempts: [] });
    expect(report.rows).toHaveLength(0);
    expect(report.counts.completed).toBe(0);
  });

  it("does not claim an overall pass from 625 communication passes alone", () => {
    const attempts = frozenPairMatrix.map((pair) => communication(pair.pairId, `attempt-${pair.pairId}`, pair.sender, pair.receiver));
    const report = buildQualificationReport({ routes: availableRoutes, obligations: frozenPairMatrix, attempts, expectedDimensionObligations: [
      { id: "codex-appserver-auto-task", kind: "automatic_tasks", route: "codex-appserver", required: true },
      { id: "codex-appserver-initiative-1", kind: "initiative", route: "codex-appserver", required: true },
    ] });
    expect(report.counts.completed).toBe(625);
    expect(report.total.outcome).toBe("uncertain");
    expect(report.total.unresolved).toContain("codex-appserver-auto-task");
  });

  it("materializes the frozen behavioral obligations and rejects a different route's evidence", () => {
    expect(frozenDimensionObligations).toHaveLength(250);
    expect(frozenDimensionObligations.filter((row) => row.required)).toHaveLength(220);
    const report = buildQualificationReport({ routes: availableRoutes, obligations: frozenPairMatrix,
      expectedDimensionObligations: frozenDimensionObligations, attempts: [{
        kind: "initiative", attemptId: "wrong-route", rowId: "codex-appserver:initiative:1", route: "pi-rpc-cli",
        status: "completed", outcome: "meets", execution: { status: "completed" },
      }] });
    expect(report.rows).toHaveLength(875);
    expect(report.rows.find((row) => row.id === "codex-appserver:initiative:1")?.outcome).toBe("does_not_meet");
    expect(report.total.outcome).not.toBe("meets");
  });

  it("lets an executed semantic failure dominate an unrun obligation", () => {
    const pair = { pairId: "codex-appserver->codex-appserver", sender: "codex-appserver" as const, receiver: "codex-appserver" as const };
    const report = buildQualificationReport({ routes: availableRoutes, obligations: [pair, { pairId: "codex-interactive->codex-interactive", sender: "codex-interactive", receiver: "codex-interactive" }], attempts: [communication(pair.pairId, "failed", pair.sender, pair.receiver, "completed", "does_not_meet")] });
    expect(report.counts.completed).toBe(1);
    expect(report.total.outcome).toBe("does_not_meet");
    expect(report.total.unresolved).toContain("codex-interactive->codex-interactive");
  });
});
