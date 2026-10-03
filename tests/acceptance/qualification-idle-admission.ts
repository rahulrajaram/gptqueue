import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { assertIdleObservation, assertSetupTurnCompleted } from "./qualification-idle-task.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import { extractGenericTraces, extractNativeTraces } from "./qualification-evidence.js";
import { collectIdleClaimExchange } from "./qualification-idle-claim.js";
import { checkExchangeEvidence, evaluateTotalVerdict, type AcceptanceRow } from "./oracle.js";
import type { GenericCallRecord, ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";
import { CODEX_BIN } from "./local-tools.js";

type Json = Record<string, unknown>;
export type IdleAdmissionInput = Readonly<{
  receipt: Json;
  baselineModel: unknown;
  baselinePeer: unknown;
  finalModel: unknown;
  finalPeer: unknown;
  modelRef: RawEvidenceRef;
  peerRef: RawEvidenceRef;
  baselineModelRef: RawEvidenceRef;
  baselinePeerRef: RawEvidenceRef;
}>;
export type IdleAdmission = Readonly<{
  attempt: Readonly<{ kind: "automatic_tasks"; attemptId: string; rowId: string; route: "codex-appserver"; status: "completed"; outcome: "meets"; execution: Readonly<{ status: "completed"; detail: string }>; detail: string }>;
  provenance: Json;
}>;
export type IdleAdmissionTrust = Readonly<{
  repoRoot?: string;
  evidenceRoot?: string;
  approvedExecutables?: Readonly<{ node?: string; codex?: string }>;
}>;

const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const requiredString = (value: unknown, label: string): string => { if (typeof value !== "string" || value.length === 0) throw new Error(`${label} is required`); return value; };
const hash = (bytes: Uint8Array | string): string => createHash("sha256").update(bytes).digest("hex");
const within = (root: string, candidate: string): boolean => { const rel = relative(root, candidate); return rel.length > 0 && !isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`); };
/** Roots restrict which files are read; controller-owned provenance remains required and this is not cryptographic authentication. */
export const trustedPath = async (candidate: string, root: string, label: string): Promise<string> => {
  const rootReal = await realpath(resolve(root)), candidateReal = await realpath(resolve(candidate));
  if (!within(rootReal, candidateReal)) throw new Error(`${label} escapes trusted root`);
  return candidateReal;
};
export const trustedSourceLabel = async (label: string, repoRoot: string): Promise<string> => {
  if (isAbsolute(label) || label !== label.replaceAll("\\", "/") || label.split("/").some(part => part.length === 0 || part === "." || part === "..")) throw new Error(`source label is not normalized repo-relative: ${label}`);
  return trustedPath(join(resolve(repoRoot), label), repoRoot, `source ${label}`);
};
const identity = (value: unknown, label: string): ParticipantIdentity => {
  const row = object(value); if (!row) throw new Error(`${label} identity is malformed`);
  return {
    participantId: requiredString(row.participantId, `${label}.participantId`), route: requiredString(row.route, `${label}.route`) as ParticipantIdentity["route"],
    hostRuntimeId: requiredString(row.hostRuntimeId, `${label}.hostRuntimeId`), agent: requiredString(row.agent, `${label}.agent`),
    cwdHash: requiredString(row.cwdHash, `${label}.cwdHash`), profileHash: requiredString(row.profileHash, `${label}.profileHash`), epochHash: requiredString(row.epochHash, `${label}.epochHash`),
  };
};
const rawRef = (value: unknown, label: string): RawEvidenceRef => {
  const row = object(value); if (!row) throw new Error(`${label} raw reference is malformed`);
  return { path: requiredString(row.path, `${label}.path`), sha256: requiredString(row.sha256, `${label}.sha256`), sourceRevision: requiredString(row.sourceRevision, `${label}.sourceRevision`), oracleRevision: requiredString(row.oracleRevision, `${label}.oracleRevision`) };
};
const readVerifiedRaw = async (ref: RawEvidenceRef, evidenceRoot: string): Promise<unknown> => {
  const path = await trustedPath(ref.path, evidenceRoot, "raw evidence");
  const bytes = await readFile(path);
  if (hash(bytes) !== ref.sha256) throw new Error(`raw evidence changed: ${ref.path}`);
  return JSON.parse(bytes.toString("utf8")) as unknown;
};
const phase = (receipt: Json, label: string): Json => {
  const phases = Array.isArray(receipt.phases) ? receipt.phases : [];
  const found = phases.map(object).find(row => row?.label === label);
  const value = object(found?.value); if (!value) throw new Error(`receipt phase ${label} is missing`); return value;
};
const genericRecords = (history: unknown): readonly GenericCallRecord[] => {
  if (!Array.isArray(history)) throw new Error("generic history is not an array");
  return history.map((value, index) => { const row = object(value); if (!row || !object(row.request)) throw new Error(`generic history row ${index} is malformed`); return { sourceId: requiredString(row.sourceId, `generic history row ${index} sourceId`), name: requiredString(row.name, `generic history row ${index} name`), request: row.request as Record<string, unknown>, response: row.response }; });
};
const turnIds = (history: unknown): readonly string[] => {
  const turns = object(history)?.turns; if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.map((turn, index) => requiredString(object(turn)?.id, `native turn ${index} id`));
};
const postCallIds = (history: unknown, baseline: readonly string[]): readonly string[] => {
  const turns = object(history)?.turns; if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.flatMap(turn => { const row = object(turn); if (!row || baseline.includes(String(row.id)) || !Array.isArray(row.items)) return []; return row.items.flatMap(item => { const value = object(item); return value?.type === "mcpToolCall" && typeof value.id === "string" ? [value.id] : []; }); });
};

export const adjudicateIdleEvidence = async (input: IdleAdmissionInput): Promise<IdleAdmission> => {
  const receipt = input.receipt, rowId = "codex-appserver:automatic:idle-task";
  if (receipt.row_id !== rowId) throw new Error(`receipt row_id must be ${rowId}`);
  const identities = object(receipt.identities), model = identity(identities?.model, "model"), peer = identity(identities?.peer, "peer");
  if (model.route !== "codex-appserver" || peer.route !== "generic-stdio" || model.agent === peer.agent) throw new Error("idle receipt identities are not the expected distinct routes");
  const plan = object(receipt.plan); const nonce = requiredString(plan?.nonce, "plan.nonce");
  const left = plan?.left, right = plan?.right;
  if (typeof left !== "number" || !Number.isSafeInteger(left) || typeof right !== "number" || !Number.isSafeInteger(right)) throw new Error("plan arithmetic is not integral");
  const requestContent = `Task ${nonce}: calculate ${left} + ${right}. Send a correlated result with content "answer ${nonce}: <decimal answer>".`;
  const expectedReplyContent = `answer ${nonce}: ${left + right}`;
  if (plan?.requestContent !== requestContent || plan?.expectedReplyContent !== expectedReplyContent) throw new Error("receipt plan does not match independently computed arithmetic");

  const setup = phase(receipt, "setup-prompt-returned");
  const setupTurnId = assertSetupTurnCompleted(input.baselineModel, setup);
  if (setupTurnId !== receipt.setup_turn_id) throw new Error("setup turn identity is not retained exactly");
  const baseline = object(receipt.baseline); if (!baseline || !Array.isArray(baseline.turn_ids)) throw new Error("receipt baseline is incomplete");
  const baselineTurnIds = baseline.turn_ids.map((value, index) => requiredString(value, `baseline.turn_ids[${index}]`));
  const baselineTurns = object(input.baselineModel)?.turns;
  if (!Array.isArray(baselineTurns) || baselineTurns.some(turn => !["completed", "succeeded"].includes(String(object(turn)?.status)))) throw new Error("baseline has an unfinished or unsuccessful native turn");
  const idle = object(baseline.idle); if (idle?.kind !== "idle" || idle.runtimeId !== model.hostRuntimeId) throw new Error("baseline does not retain the exact idle runtime identity");
  const nativeBaseline = extractNativeTraces(input.baselineModel, model, input.baselineModelRef);
  const genericBaseline = extractGenericTraces(genericRecords(input.baselinePeer), peer, input.baselinePeerRef, true);
  const binding = nativeBaseline.find(trace => trace.name === "get_runtime_status" && trace.successful === true && trace.runtimeBound === true && trace.output?.activation_ready === true && object(trace.output.runtime)?.runtime_id === model.hostRuntimeId && trace.output.agent === model.agent && typeof object(trace.output.runtime)?.working_directory === "string" && hash(resolve(String(object(trace.output.runtime)?.working_directory))) === model.cwdHash);
  if (!binding) throw new Error("baseline lacks exact activation-ready native binding");
  const modelDiscovery = collectPeerDiscovery(model, peer, nativeBaseline), peerDiscovery = collectPeerDiscovery(peer, model, genericBaseline);
  if (modelDiscovery.rawHistoryRef.path !== input.baselineModelRef.path || peerDiscovery.rawHistoryRef.path !== input.baselinePeerRef.path) throw new Error("discovery provenance is not bound to baseline histories");

  const proof = object(receipt.proof); if (!proof) throw new Error("receipt proof is missing");
  const finalTurnIds = turnIds(input.finalModel), baselineActualTurnIds = turnIds(input.baselineModel), boundary = proof.boundary;
  if (typeof boundary !== "number" || !Number.isSafeInteger(boundary) || boundary !== object(phase(receipt, "stimulus"))?.boundary) throw new Error("stimulus boundary is missing or inconsistent");
  if (JSON.stringify(baselineTurnIds) !== JSON.stringify(baselineActualTurnIds)) throw new Error("receipt baseline turn IDs do not match baseline history");
  if (object(input.baselineModel)?.id !== model.hostRuntimeId || object(input.finalModel)?.id !== model.hostRuntimeId) throw new Error("native history belongs to another thread");
  const controllerEvents = Array.isArray(receipt.controller_events) ? receipt.controller_events.map(value => { const row = object(value); if (!row || typeof row.kind !== "string" || typeof row.at !== "number" || !Number.isSafeInteger(row.at)) throw new Error("controller event is malformed"); return { kind: row.kind, at: row.at }; }) : [];
  if (!controllerEvents.some(event => event.kind === "stimulus_boundary" && event.at === boundary) || !controllerEvents.some(event => event.kind === "controller_prompt" && event.at < boundary)) throw new Error("controller event ledger lacks setup or stimulus boundary");
  assertIdleObservation({ boundary, nativeTurnIds: finalTurnIds, controllerEvents }, baselineTurnIds);
  const postStimulusCallIds = postCallIds(input.finalModel, baselineTurnIds); if (postStimulusCallIds.length === 0) throw new Error("no post-stimulus native calls");
  const native = extractNativeTraces(input.finalModel, model, input.modelRef);
  const generic = extractGenericTraces(genericRecords(input.finalPeer), peer, input.peerRef, true);
  const exchange = collectIdleClaimExchange({ peer, model, genericTraces: generic, nativeTraces: native, postStimulusCallIds, nonce, requestContent, expectedReplyContent });
  const verdict = checkExchangeEvidence(exchange); if (verdict.outcome !== "meets") throw new Error(`idle exchange failed current oracle: ${verdict.reasons.join("; ")}`);
  if (!Array.isArray(proof.final_turn_ids) || JSON.stringify(proof.final_turn_ids) !== JSON.stringify(finalTurnIds) || JSON.stringify(proof.post_stimulus_call_ids) !== JSON.stringify(postStimulusCallIds)) throw new Error("receipt proof does not match recomputed native observation");
  const attemptId = `native-idle-task-${requiredString(receipt.run_id, "receipt.run_id")}`;
  return { attempt: { kind: "automatic_tasks", attemptId, rowId, route: "codex-appserver", status: "completed", outcome: "meets", execution: { status: "completed", detail: "current collectors rechecked the exact post-stimulus native claim, reply and acknowledgement" }, detail: "one owned-profile native idle-task observation; lifecycle and repeatability are separate judgments" }, provenance: { receiptRunId: receipt.run_id, receiptRowId: rowId, receiptPassedLabel: receipt.passed, model, peer, baselineTurnIds, finalTurnIds, postStimulusCallIds, boundary, modelRef: input.modelRef, peerRef: input.peerRef, baselineModelRef: input.baselineModelRef, baselinePeerRef: input.baselinePeerRef, discovery: { modelToPeer: modelDiscovery, peerToModel: peerDiscovery }, exchange, verdict } };
};

export const admitIdleTaskCheckpoint = async (basePath: string, receiptPath: string, outputRoot: string, trust: IdleAdmissionTrust = {}): Promise<string> => {
  const repoRoot = resolve(trust.repoRoot ?? resolve(import.meta.dirname, "../.."));
  const trustedBase = await trustedPath(basePath, repoRoot, "base checkpoint");
  const evidenceRoot = resolve(trust.evidenceRoot ?? dirname(resolve(receiptPath)));
  const trustedReceipt = await trustedPath(receiptPath, evidenceRoot, "idle receipt");
  const approved = { node: trust.approvedExecutables?.node ?? process.execPath, codex: trust.approvedExecutables?.codex ?? CODEX_BIN };
  const baseText = await readFile(trustedBase, "utf8"), base = object(JSON.parse(baseText)); if (!base) throw new Error("base checkpoint is malformed");
  if (hash(baseText) !== hash(await readFile(trustedBase))) throw new Error("base checkpoint changed while reading");
  if (base.kind !== "offline-discovery-qualified-checkpoint" || object(base.contract)?.routeCount !== 25 || object(base.contract)?.pairCount !== 625 || object(base.contract)?.dimensionObligationCount !== 250 || object(base.contract)?.requiredDimensionObligations !== 220) throw new Error("base checkpoint is not the frozen 25/625/250/220 checkpoint");
  const report = object(base.report), rows = Array.isArray(report?.rows) ? report.rows.map(object) : []; if (rows.length !== 875) throw new Error("base report does not contain 625+250 rows");
  const target = rows.find(row => row?.id === "codex-appserver:automatic:idle-task"); if (!target || target.status !== "unrun" || target.outcome !== "uncertain") throw new Error("base idle-task row is not the expected unrun obligation");
  const receiptText = await readFile(trustedReceipt, "utf8"), receipt = object(JSON.parse(receiptText)); if (!receipt) throw new Error("idle receipt is malformed");
  const cleanup = Array.isArray(receipt.cleanup) ? receipt.cleanup.map(object) : [];
  if (object(receipt.execution)?.status !== "completed" || object(receipt.lifecycle)?.status !== "idle" || cleanup.length !== 6 || cleanup.some(item => item?.status !== "fulfilled")) throw new Error("idle receipt lacks completed execution, idle lifecycle, or complete cleanup");
  const before = object(receipt.source_hashes_before), after = object(receipt.source_hashes_after); if (!before || !after || JSON.stringify(before) !== JSON.stringify(after)) throw new Error("idle receipt source before/after hashes differ");
  for (const label of ["tests/acceptance/qualification-idle-task.test.ts", "tests/acceptance/qualification-idle-claim.ts", "tests/acceptance/qualification-codex.ts", "tests/acceptance/oracle.ts", "node_executable", "codex_executable"]) {
    if (typeof before[label] !== "string" || !/^[a-f0-9]{64}$/u.test(before[label])) throw new Error(`idle source hash missing: ${label}`);
  }
  const sourceExecutables = object(receipt.source_executables); if (!sourceExecutables) throw new Error("idle receipt executable provenance is missing");
  for (const [label, expected] of Object.entries(before)) {
    const path = label === "node_executable" ? approved.node : label === "codex_executable" ? approved.codex : await trustedSourceLabel(label, repoRoot);
    if (label === "node_executable" || label === "codex_executable") {
      const declared = requiredString(sourceExecutables[label], `source_executables.${label}`);
      if (await realpath(declared) !== await realpath(path)) throw new Error(`${label} does not match approved executable`);
    }
    if (hash(await readFile(await realpath(path))) !== expected) throw new Error(`idle source changed: ${label}`);
  }
  const sourceExec = sourceExecutables;
  const baseline = object(receipt.baseline), proof = object(receipt.proof); if (!baseline || !proof) throw new Error("idle receipt retained evidence is incomplete");
  const baselineModelRef = rawRef(object(baseline.binding)?.rawHistoryRef, "baseline model");
  const discovery = object(baseline.discovery); const baselinePeerRef = rawRef(object(discovery?.peer_to_model)?.rawHistoryRef, "baseline peer");
  const modelRef = rawRef(proof.model_history, "final model"), peerRef = rawRef(proof.peer_history, "final peer");
  const refs = [baselineModelRef, baselinePeerRef, modelRef, peerRef];
  if (refs.some(ref => ref.sourceRevision !== hash(JSON.stringify(before)) || ref.oracleRevision !== before["tests/acceptance/oracle.ts"])) throw new Error("raw reference source/oracle binding differs from executed sources");
  const [baselineModel, baselinePeer, finalModel, finalPeer] = await Promise.all(refs.map(ref => readVerifiedRaw(ref, evidenceRoot)));
  const admission = await adjudicateIdleEvidence({ receipt, baselineModel, baselinePeer, finalModel, finalPeer, modelRef, peerRef, baselineModelRef, baselinePeerRef });
  const updatedRows = rows.map(row => row?.id === admission.attempt.rowId ? { ...row, status: admission.attempt.status, outcome: admission.attempt.outcome, execution: admission.attempt.execution, required: true, attemptId: admission.attempt.attemptId, detail: admission.attempt.detail, route: "codex-appserver", dimension: "automatic_tasks" } : row);
  const acceptanceRows = updatedRows.map(row => ({ id: String(row?.id), dimension: row?.dimension, route: row?.route, outcome: row?.outcome, execution: row?.execution, required: row?.required, detail: row?.detail })) as AcceptanceRow[];
  const counts = Object.fromEntries(["completed", "failed", "blocked", "setup_gap", "unrun"].map(status => [status, updatedRows.filter(row => row?.status === status).length]));
  const outcomes = Object.fromEntries(["meets", "does_not_meet", "uncertain", "not_applicable"].map(outcome => [outcome, updatedRows.filter(row => row?.outcome === outcome).length]));
  if (counts.completed !== 26 || counts.unrun !== 249 || counts.setup_gap !== 459 || counts.blocked !== 141 || counts.failed !== 0) throw new Error("derived checkpoint changed more than the one admitted obligation");
  if (hash(await readFile(trustedBase)) !== hash(baseText) || hash(await readFile(trustedReceipt)) !== hash(receiptText)) throw new Error("input checkpoint or receipt changed during adjudication");
  const trustProvenance = { repoRoot: await realpath(repoRoot), evidenceRoot: await realpath(evidenceRoot), approvedExecutables: { node: await realpath(approved.node), codex: await realpath(approved.codex) } };
  const run = randomUUID(), output = { ...base, run, kind: "offline-idle-task-admission-checkpoint", generatedAt: new Date().toISOString(), baseCheckpoint: { path: trustedBase, sha256: hash(baseText), preserved: true }, idleTaskAdmission: { receipt: trustedReceipt, receiptSha256: hash(receiptText), admittedAttemptIds: [admission.attempt.attemptId], attempt: admission.attempt, provenance: admission.provenance, trust: trustProvenance, sourceExecutablePaths: sourceExec }, report: { ...report, rows: updatedRows, counts, outcomes, total: evaluateTotalVerdict(acceptanceRows) }, counts: { status: counts, outcome: outcomes, total: evaluateTotalVerdict(acceptanceRows).outcome } };
  const dir = join(resolve(outputRoot), run); await mkdir(dir, { recursive: true }); await writeFile(join(dir, "checkpoint.json"), `${JSON.stringify(output, null, 2)}\n`, { mode: 0o600 });
  await writeFile(join(dir, "checkpoint.md"), `# Offline idle-task admission checkpoint ${run}\n\n- Base checkpoint preserved by SHA-256 reference.\n- Admitted row: ${admission.attempt.rowId}.\n- This is one automatic-task observation; initiative, lifecycle families, and overall qualification remain unresolved.\n`, { mode: 0o600 });
  return join(dir, "checkpoint.json");
};
