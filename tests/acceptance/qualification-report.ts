import { evaluateTotalVerdict, type AcceptanceDimension, type ExecutionEvidence, type Outcome, type TotalVerdict } from "./oracle.js";
import type { ParticipantIdentity, RawEvidenceRef, RouteId, RouteSpec, PairTemplate } from "./qualification-types.js";

export type ReportStatus = "completed" | "failed" | "blocked" | "setup_gap" | "unrun";
export type ExpectedDimension = "automatic_tasks" | "initiative" | "registration";
export type CommunicationAttempt = Readonly<{ kind: "communication"; attemptId: string; pairId: string; sender: ParticipantIdentity; receiver: ParticipantIdentity; status: ReportStatus; outcome: Outcome; execution: ExecutionEvidence; raw: readonly RawEvidenceRef[]; detail?: string }>;
export type DimensionAttempt = Readonly<{ kind: "automatic_tasks" | "initiative"; attemptId: string; rowId: string; route: RouteId; status: ReportStatus; outcome: Outcome; execution: ExecutionEvidence; detail?: string }>;
export type RegistrationAttempt = Readonly<{ kind: "registration"; attemptId: string; rowId: string; route: RouteId; status: ReportStatus; outcome: Outcome; execution: ExecutionEvidence; detail?: string }>;
export type AttemptRow = CommunicationAttempt | DimensionAttempt | RegistrationAttempt;
export type ExpectedDimensionObligation = Readonly<{ id: string; kind: ExpectedDimension; route: RouteId; required: boolean }>;
export type ReportRow = Readonly<{ id: string; dimension: AcceptanceDimension; route?: RouteId; pairId?: string; status: ReportStatus; outcome: Outcome; execution: ExecutionEvidence; required: boolean; attemptId?: string; detail: string }>;
export type QualificationReport = Readonly<{ rows: readonly ReportRow[]; counts: Readonly<Record<ReportStatus, number>>; outcomes: Readonly<Record<Outcome, number>>; duplicateObligations: readonly string[]; total: TotalVerdict }>;
export type QualificationReportInput = Readonly<{ routes: readonly RouteSpec[]; obligations: readonly PairTemplate[]; attempts: readonly AttemptRow[]; expectedDimensionObligations?: readonly ExpectedDimensionObligation[]; admittedAttemptIds?: readonly string[] }>;

const statuses: readonly ReportStatus[] = ["completed", "failed", "blocked", "setup_gap", "unrun"];
const outcomes: readonly Outcome[] = ["meets", "does_not_meet", "uncertain", "not_applicable"];
const routeAvailability = (routes: readonly RouteSpec[]): Map<RouteId, RouteSpec["availability"]> => new Map(routes.map((route) => [route.id, route.availability]));
const fallbackStatus = (pair: PairTemplate, routes: Map<RouteId, RouteSpec["availability"]>): ReportStatus => {
  const availability = [routes.get(pair.sender), routes.get(pair.receiver)];
  if (availability.some((value) => value?.kind === "blocked_prerequisite")) return "blocked";
  if (availability.some((value) => value?.kind === "setup_gap")) return "setup_gap";
  return "unrun";
};
const validIdentity = (identity: ParticipantIdentity, route: RouteId): boolean => identity.route === route && [identity.participantId, identity.agent, identity.hostRuntimeId, identity.cwdHash, identity.profileHash, identity.epochHash].every((value) => value.length > 0);
const validCommunication = (attempt: CommunicationAttempt, pair: PairTemplate): boolean => attempt.pairId === pair.pairId && validIdentity(attempt.sender, pair.sender) && validIdentity(attempt.receiver, pair.receiver) && attempt.sender.participantId !== attempt.receiver.participantId && attempt.sender.agent !== attempt.receiver.agent && attempt.raw.length > 0 && attempt.raw.every((ref) => ref.path.length > 0 && ref.sha256.length > 0 && ref.sourceRevision.length > 0 && ref.oracleRevision.length > 0);
const keyOf = (attempt: AttemptRow): string => attempt.kind === "communication" ? attempt.pairId : attempt.rowId;
const dimensionOf = (kind: AttemptRow["kind"] | ExpectedDimension): AcceptanceDimension => kind === "initiative" || kind === "automatic_tasks" ? kind : "communication";
const missingRow = (id: string, dimension: AcceptanceDimension, route: RouteId | undefined, required: boolean, status: ReportStatus, detail: string): ReportRow => Object.freeze({ id, dimension, route, status, outcome: status === "unrun" || status === "blocked" || status === "setup_gap" ? "uncertain" : "does_not_meet", execution: { status: "not_run" as const, detail }, required, detail });

export const buildQualificationReport = (input: QualificationReportInput): QualificationReport => {
  const routes = routeAvailability(input.routes), admitted = new Set(input.admittedAttemptIds ?? []), grouped = new Map<string, AttemptRow[]>(), selected = new Map<string, AttemptRow>(), duplicateObligations: string[] = [];
  for (const attempt of input.attempts) grouped.set(keyOf(attempt), [...(grouped.get(keyOf(attempt)) ?? []), attempt]);
  for (const [key, candidates] of grouped) {
    const chosen = candidates.filter((attempt) => admitted.has(attempt.attemptId));
    if (candidates.length > 1 && chosen.length !== 1) duplicateObligations.push(key);
    else if (chosen.length === 1) selected.set(key, chosen[0]!);
    else if (candidates.length === 1 && input.admittedAttemptIds === undefined) selected.set(key, candidates[0]!);
  }
  if (duplicateObligations.length > 0) throw new Error(`duplicate attempts require explicit admittedAttemptIds: ${[...new Set(duplicateObligations)].join(", ")}`);
  const rows: ReportRow[] = input.obligations.map((pair) => {
    const attempt = selected.get(pair.pairId);
    if (!attempt) return missingRow(pair.pairId, "communication", pair.sender, true, fallbackStatus(pair, routes), "no admitted communication attempt");
    if (attempt.kind !== "communication" || !validCommunication(attempt, pair)) return Object.freeze({ id: pair.pairId, dimension: "communication", route: pair.sender, pairId: pair.pairId, status: "failed", outcome: "does_not_meet", execution: { status: "failed" as const, detail: "communication attempt lacks exact identities or raw evidence hashes" }, required: true, attemptId: attempt.attemptId, detail: "communication attempt lacks exact identities or raw evidence hashes" });
    return Object.freeze({ id: pair.pairId, dimension: "communication", route: pair.sender, pairId: pair.pairId, status: attempt.status, outcome: attempt.outcome, execution: attempt.execution, required: true, attemptId: attempt.attemptId, detail: attempt.detail ?? attempt.outcome });
  });
  for (const expected of input.expectedDimensionObligations ?? []) {
    const attempt = selected.get(expected.id);
    if (!attempt) rows.push(missingRow(expected.id, dimensionOf(expected.kind), expected.route, expected.required, "unrun", "expected qualification obligation was not run"));
    else if (attempt.kind !== expected.kind || attempt.route !== expected.route) rows.push(missingRow(expected.id, dimensionOf(expected.kind), expected.route, expected.required, "failed", "attempt kind or route does not match expected obligation"));
    else rows.push(Object.freeze({ id: expected.id, dimension: dimensionOf(expected.kind), route: expected.route, status: attempt.status, outcome: attempt.outcome, execution: attempt.execution, required: expected.required, attemptId: attempt.attemptId, detail: attempt.detail ?? attempt.outcome }));
  }
  const acceptanceRows = rows.map(({ id, dimension, route, outcome, execution, required, detail }) => ({ id, dimension, route, outcome, execution, required, detail }));
  const counts = Object.fromEntries(statuses.map((status) => [status, rows.filter((row) => row.status === status).length])) as Record<ReportStatus, number>;
  const outcomeCounts = Object.fromEntries(outcomes.map((outcome) => [outcome, rows.filter((row) => row.outcome === outcome).length])) as Record<Outcome, number>;
  return Object.freeze({ rows: Object.freeze(rows), counts: Object.freeze(counts), outcomes: Object.freeze(outcomeCounts), duplicateObligations: Object.freeze([...new Set(duplicateObligations)]), total: evaluateTotalVerdict(acceptanceRows) });
};
