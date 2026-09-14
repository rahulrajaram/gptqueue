import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { collectNativeExchange, extractNativeTraces, type NativeTrace } from "./qualification-evidence.js";
import { checkExchangeEvidence } from "./oracle.js";
import type { ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";

const sameRouteEnabled = process.env.GPTQUEUE_RETAINED_PI_SAME_ROUTE_ADJUDICATION === "1";
const legacyEnabled = process.env.GPTQUEUE_RETAINED_PI_LEGACY_ADJUDICATION === "1";
const repo = resolve(import.meta.dirname, "../..");
const correctedRunRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-cohort-pilot/1dea7d7c-f2d6-4104-aa36-1a2480721a17");
const legacyRunRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-cohort-pilot/8993b003-121b-4a9b-aa28-778a488afc81");
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
type Json = Record<string, unknown>;
type ActorRow = Readonly<{ participant_id: string; route: ParticipantIdentity["route"]; host_runtime_id: string; agent: string; cwd_hash: string; profile_hash: string; epoch_hash: string }>;
type Phase = Readonly<{ label: string; value: unknown }>;
type Receipt = Readonly<{ run_id: string; execution: Json; source_hashes: Record<string, string>; source_hashes_after: Record<string, string>; cleanup: readonly Readonly<{ name: string; status: string }>[]; phases: readonly Phase[] }>;
type SamePiRow = Readonly<{ id: string; sender: string; receiver: string; nonce: string; status: string; error: string; raw: readonly RawEvidenceRef[] }>;
type Consumption = "claim_ack" | "legacy_receive";
type Case = Readonly<{ label: string; runRoot: string; expectedFullNonce: string; expectedExpression: string; expectedDecimal: number; consumption: Consumption; rowPhase: string; supersededPath?: string }>;

const actor = (value: ActorRow): ParticipantIdentity => ({ participantId: value.participant_id, route: value.route, hostRuntimeId: value.host_runtime_id, agent: value.agent, cwdHash: value.cwd_hash, profileHash: value.profile_hash, epochHash: value.epoch_hash });
const phase = (receipt: Receipt, label: string): Json => {
  const found = receipt.phases.find(item => item.label === label)?.value;
  if (!found || typeof found !== "object" || Array.isArray(found)) throw new Error(`receipt lacks phase ${label}`);
  return found as Json;
};
const actorRows = (value: Json): readonly ActorRow[] => {
  const participants = value.participants;
  if (!Array.isArray(participants)) throw new Error("launch phase lacks participants");
  return participants.filter((item): item is ActorRow => typeof item === "object" && item !== null && !Array.isArray(item) && (item as Json).route === "pi-rpc-cli") as readonly ActorRow[];
};
const samePi = (value: Json): SamePiRow => {
  if (value.id !== "same-pi" || typeof value.sender !== "string" || typeof value.receiver !== "string" || typeof value.nonce !== "string" || typeof value.status !== "string" || typeof value.error !== "string" || !Array.isArray(value.raw)) throw new Error("same-pi retained row is malformed");
  return value as unknown as SamePiRow;
};
const oracleFiles = [
  "tests/acceptance/qualification-evidence.ts",
  "tests/acceptance/oracle.ts",
  "tests/acceptance/qualification-types.ts",
  "tests/acceptance/qualification-pi-same-route-adjudication.test.ts",
] as const;
const sourceHashes = async (): Promise<Record<string, string>> => Object.fromEntries(
  await Promise.all(oracleFiles.map(async file => [file, hash(await readFile(join(repo, file), "utf8"))] as const)),
);
const rawReference = async (reference: RawEvidenceRef, sourceRevision: string, oracleRevision: string): Promise<Readonly<{ history: unknown; raw: RawEvidenceRef }>> => {
  const text = await readFile(reference.path, "utf8");
  if (hash(text) !== reference.sha256) throw new Error(`immutable retained history hash changed: ${reference.path}`);
  if (reference.sourceRevision !== sourceRevision || reference.oracleRevision !== oracleRevision) throw new Error(`retained history source revision mismatch: ${reference.path}`);
  return { history: JSON.parse(text) as unknown, raw: reference };
};
const traceSummary = (traces: readonly NativeTrace[]): readonly Json[] => traces.map(trace => ({ source_id: trace.sourceId, name: trace.name, actor: trace.actor.agent, runtime_id: trace.runtimeId, successful: trace.successful, runtime_bound: trace.runtimeBound }));

const cases: readonly Case[] = [
  {
    label: "corrected",
    runRoot: correctedRunRoot,
    expectedFullNonce: "1dea7d7c-f2d6-4104-aa36-1a2480721a17:same-pi:da2efee2-f7d7-4eef-97c5-5681079c5e8f",
    expectedExpression: "19+23",
    expectedDecimal: 42,
    consumption: "claim_ack",
    rowPhase: "row-same-pi-failed",
    supersededPath: join(correctedRunRoot, "offline-adjudication-pi-same-route.json"),
  },
  {
    label: "legacy-receive",
    runRoot: legacyRunRoot,
    expectedFullNonce: "8993b003-121b-4a9b-aa28-778a488afc81:same-pi:48e54165-2269-4727-bbcd-25a73dfebf0b",
    expectedExpression: "19+23",
    expectedDecimal: 42,
    consumption: "legacy_receive",
    rowPhase: "row-same-pi-failed",
  },
];

const adjudicate = async (testCase: Case): Promise<void> => {
  const retainedReceiptPath = join(testCase.runRoot, "receipt-sanitized.json");
  const receiptText = await readFile(retainedReceiptPath, "utf8");
  const originalReceiptHash = hash(receiptText);
  const supersededHashBefore = testCase.supersededPath ? hash(await readFile(testCase.supersededPath, "utf8")) : null;
  const receipt = JSON.parse(receiptText) as Receipt;
  const launch = actorRows(phase(receipt, "launched"));
  expect(launch).toHaveLength(2);
  const sender = actor(launch[0]!);
  const receiver = actor(launch[1]!);
  const row = samePi(phase(receipt, testCase.rowPhase));
  const sourceBefore = receipt.source_hashes;
  const sourceAfter = receipt.source_hashes_after;
  expect(sourceBefore).toEqual(sourceAfter);
  expect(row.status).toBe("failed");
  expect(row.error).toContain("180000ms deadline");
  expect(row.nonce).toBe(testCase.expectedFullNonce);
  expect(receipt.cleanup.length).toBeGreaterThan(0);
  expect(receipt.cleanup.every(item => item.status === "fulfilled")).toBe(true);

  const beforeOracleHashes = await sourceHashes();
  const oracleRevision = hash(JSON.stringify(beforeOracleHashes));
  const driverRevision = sourceBefore["tests/acceptance/qualification-driver.ts"];
  if (!driverRevision) throw new Error("retained receipt lacks qualification-driver source revision");
  // Validate original provenance against the retained references, then evaluate with the current oracle. Later extractor fixes are recorded separately and do not rewrite the receipt.
  const histories = await Promise.all(row.raw.map(reference => rawReference(reference, driverRevision, sourceBefore["tests/acceptance/oracle.ts"]!)));
  const traces = histories.map((entry, index) => extractNativeTraces(entry.history, index === 0 ? sender : receiver, entry.raw));
  const requestContent = `qualification ${testCase.expectedFullNonce}: calculate ${testCase.expectedExpression}`;
  const expectedReplyContent = `answer ${testCase.expectedFullNonce}: ${testCase.expectedDecimal}`;
  const collected = collectNativeExchange({
    sender: { actor: sender, traces: traces[0]! }, receiver: { actor: receiver, traces: traces[1]! },
    nonce: testCase.expectedFullNonce, requestContent, expectedReplyContent, consumption: testCase.consumption,
  });
  const verdict = checkExchangeEvidence(collected.evidence);
  expect(verdict.outcome).toBe("meets");
  expect(collected.evidence.request?.content).toBe(requestContent);
  expect(collected.evidence.reply?.content).toBe(expectedReplyContent);
  expect(collected.evidence.request_consumption?.consumed).toBe(true);
  expect(collected.evidence.reply_consumption?.consumed).toBe(true);
  if (testCase.consumption === "claim_ack") {
    expect(collected.evidence.request_consumption?.acknowledged).toBe(true);
    expect(collected.evidence.reply_consumption?.acknowledged).toBe(true);
    expect(collected.evidence.request_consumption?.claim_id).toBeTruthy();
    expect(collected.evidence.reply_consumption?.claim_id).toBeTruthy();
  } else {
    expect(collected.evidence.request_consumption?.acknowledged).toBe(false);
    expect(collected.evidence.reply_consumption?.acknowledged).toBe(false);
    expect(collected.evidence.request_requires_ack).toBe(false);
    expect(collected.evidence.reply_requires_ack).toBe(false);
  }

  const afterOracleHashes = await sourceHashes();
  expect(afterOracleHashes).toEqual(beforeOracleHashes);
  expect(hash(await readFile(retainedReceiptPath, "utf8"))).toBe(originalReceiptHash);
  const supersededHashAfter = testCase.supersededPath ? hash(await readFile(testCase.supersededPath, "utf8")) : null;
  expect(supersededHashAfter).toBe(supersededHashBefore);
  const originalAdjudication = testCase.supersededPath
    ? { path: testCase.supersededPath, sha256_before: supersededHashBefore, sha256_after: supersededHashAfter, immutable: true, superseded_context_only: true }
    : { path: null, sha256_before: null, sha256_after: null, immutable: true, superseded_context_only: false };
  const adjudicationId = randomUUID();
  const artifactDir = join(testCase.runRoot, "offline-adjudications", testCase.label === "corrected" ? "pi-same-route-corrected" : "pi-same-route-legacy-receive", adjudicationId);
  const output = {
    schema_version: 1,
    kind: testCase.label === "corrected" ? "offline-pi-same-route-adjudication-corrected" : "offline-pi-same-route-adjudication-legacy-receive",
    generated_at: new Date().toISOString(),
    original_receipt: { path: retainedReceiptPath, sha256: originalReceiptHash },
    original_execution: { status: receipt.execution.status, detail: receipt.execution.detail, row_status: row.status, row_error: row.error, timeout_preserved: true },
    original_adjudication: originalAdjudication,
    source_verification: {
      retained_source_hashes_before: sourceBefore, retained_source_hashes_after: sourceAfter, retained_source_hashes_match: JSON.stringify(sourceBefore) === JSON.stringify(sourceAfter),
      oracle_hashes_before: beforeOracleHashes, oracle_hashes_after: afterOracleHashes, oracle_revision: oracleRevision,
      oracle_hashes_unchanged: JSON.stringify(beforeOracleHashes) === JSON.stringify(afterOracleHashes),
      recorded_oracle_revision: sourceBefore["tests/acceptance/oracle.ts"], recorded_driver_revision: driverRevision,
      fixed_oracle_evaluation: true, current_oracle_correction_permitted: true,
    },
    launched_pi_identities: { sender, receiver },
    row: { id: row.id, sender: row.sender, receiver: row.receiver, full_nonce: testCase.expectedFullNonce, independently_expected_expression: testCase.expectedExpression, independently_expected_decimal: testCase.expectedDecimal, request_content: requestContent, expected_reply_content: expectedReplyContent, consumption_mode: testCase.consumption },
    retained_raw_histories: histories.map(entry => entry.raw),
    traces: { sender: traceSummary(traces[0]!), receiver: traceSummary(traces[1]!) },
    evidence: collected.evidence,
    verdict,
    cleanup: receipt.cleanup,
    native_execution_performed: false,
  };
  await mkdir(artifactDir, { recursive: true, mode: 0o700 });
  await writeFile(join(artifactDir, "adjudication.json"), `${JSON.stringify(output, null, 2)}\n`, { mode: 0o600 });
  console.log(`Generated ${testCase.label} Pi same-route adjudication: ${join(artifactDir, "adjudication.json")}`);
};

describe.skipIf(!sameRouteEnabled)("retained Pi same-route offline adjudication", () => {
  it("recomputes claim and acknowledgement evidence from immutable raw histories", async () => { await adjudicate(cases[0]!); });
});

describe.skipIf(!legacyEnabled)("retained Pi legacy receive offline adjudication", () => {
  it("recomputes legacy receive evidence from immutable raw histories", async () => { await adjudicate(cases[1]!); });
});
