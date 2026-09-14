import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative as relativePath, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { collectGenericExchange, collectNativeExchange, extractGenericTraces, extractNativeTraces, type NativeTrace } from "./qualification-evidence.js";
import { collectPeerDiscovery, type PeerDiscoveryEvidence } from "./qualification-discovery.js";
import { checkExchangeEvidence } from "./oracle.js";
import { buildQualificationReport, type AttemptRow } from "./qualification-report.js";
import { frozenDimensionObligations, frozenPairMatrix, frozenRoutes } from "./qualification-routes.js";
import type { GenericActorHistory } from "./qualification-evidence.js";
import type { GenericCallRecord, ParticipantIdentity, RawEvidenceRef, RouteSpec } from "./qualification-types.js";

type Json = Record<string, unknown>;
const enabled = process.env.GPTQUEUE_QUALIFICATION_CHECKPOINT === "1";
const discoveryEnabled = process.env.GPTQUEUE_DISCOVERY_CHECKPOINT === "1";
const retained = resolve(".gptqueue/repair-qualification/20260912/generic-qualification/52c5a060-62d3-4675-af88-d8194b7ac381");
const artifactRoot = resolve(".gptqueue/repair-qualification/20260912/checkpoints");
const qualificationArtifactBase = resolve(".gptqueue/repair-qualification/20260912");
const historicalExchangeOnlyPath = join(artifactRoot, "aa85c328-2ec1-4910-a4ce-aa28eb9b7389", "checkpoint.json");
const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const string = (value: unknown, field: string): string => {
  if (typeof value !== "string" || value.length === 0) throw new Error(`checkpoint evidence has no ${field}`);
  return value;
};
const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");
const sha256Bytes = (value: Uint8Array): string => createHash("sha256").update(value).digest("hex");
const hashJson = (value: unknown): string => sha256(JSON.stringify(value));
const identity = (value: unknown): ParticipantIdentity => {
  const row = object(value);
  if (!row) throw new Error("generic receipt has no participant identity");
  return {
    participantId: string(row.participantId, "participantId"), route: string(row.route, "route") as ParticipantIdentity["route"],
    hostRuntimeId: string(row.hostRuntimeId, "hostRuntimeId"), agent: string(row.agent, "agent"),
    cwdHash: string(row.cwdHash, "cwdHash"), profileHash: string(row.profileHash, "profileHash"), epochHash: string(row.epochHash, "epochHash"),
  };
};
const phaseIdentity = (value: unknown): ParticipantIdentity => {
  const row = object(value);
  if (!row) throw new Error("pilot phase lacks participant identity");
  return identity({ participantId: row.participant_id, route: row.route, hostRuntimeId: row.host_runtime_id, agent: row.agent, cwdHash: row.cwd_hash, profileHash: row.profile_hash, epochHash: row.epoch_hash });
};
const genericRecord = (value: unknown): GenericCallRecord => {
  const row = object(value);
  if (!row || !object(row.request)) throw new Error("generic history record is malformed");
  return { sourceId: string(row.sourceId, "sourceId"), name: string(row.name, "tool name"), request: row.request as Record<string, unknown>, response: row.response };
};
const historyActor = (value: unknown): GenericActorHistory => {
  const row = object(value);
  if (!row || !Array.isArray(row.participant) && !object(row.participant)) throw new Error("generic history participant is malformed");
  return { actor: identity(row.participant), records: Array.isArray(row.calls) ? row.calls.map(genericRecord) : [] };
};

type ReceiptRow = Readonly<{ sender: ParticipantIdentity; receiver: ParticipantIdentity; sent: unknown; received: unknown; replied: unknown; returned: unknown; verdict: { outcome: string } }>;
const receiptRow = (value: unknown): ReceiptRow => {
  const row = object(value);
  if (!row) throw new Error("generic receipt row is malformed");
  const verdict = object(row.verdict);
  return {
    sender: identity(row.sender), receiver: identity(row.receiver), sent: row.sent, received: row.received,
    replied: row.replied, returned: row.returned, verdict: { outcome: string(verdict?.outcome, "verdict outcome") },
  };
};

const pilotRoot = resolve(".gptqueue/repair-qualification/20260912/qualification-cohort-pilot/8993b003-121b-4a9b-aa28-778a488afc81");
const cohortPilotRoot = resolve(".gptqueue/repair-qualification/20260912/qualification-cohort-pilot/fed37cf9-651c-42fd-b509-06867cfa0e58");
const cohortPilotReceiptPath = join(cohortPilotRoot, "receipt-sanitized.json");
const evidenceIndexPath = resolve(".gptqueue/repair-qualification/20260912/handoff/evidence-index.json");
const crossAdjudicationPath = resolve(".gptqueue/repair-qualification/20260912/cross-adjudications/9b91104a-d17e-4d76-a503-28cf476a7e59/adjudication.json");
const piAdjudicationPath = resolve(".gptqueue/repair-qualification/20260912/qualification-cohort-pilot/1dea7d7c-f2d6-4104-aa36-1a2480721a17/offline-adjudications/pi-same-route-corrected/a3a60457-6efa-425e-a8b4-df07dc6876cd/adjudication.json");
const independentArithmetic: Readonly<Record<string, Readonly<{ left: number; right: number }>>> = {
  "same-codex": { left: 17, right: 25 }, "same-pi": { left: 19, right: 23 },
  "cross-codex-to-pi": { left: 31, right: 11 }, "cross-pi-to-codex": { left: 27, right: 14 },
  "mixed-generic-to-codex": { left: 8, right: 34 }, "mixed-codex-to-generic": { left: 42, right: 7 },
  "mixed-generic-to-pi": { left: 16, right: 29 }, "mixed-pi-to-generic": { left: 38, right: 6 },
  "codex-to-pi": { left: 17, right: 25 },
};
const discoveryArithmetic: Readonly<Record<string, Readonly<{ left: number; right: number }>>> = {
  ...independentArithmetic,
  "codex-to-http": { left: 13, right: 29 }, "http-to-codex": { left: 37, right: 5 },
  "pi-to-http": { left: 22, right: 18 }, "http-to-pi": { left: 41, right: 9 },
  "codex-to-stateless": { left: 7, right: 35 }, "stateless-to-codex": { left: 26, right: 16 },
  "pi-to-stateless": { left: 18, right: 24 }, "stateless-to-pi": { left: 32, right: 12 },
};
const rawEvidenceRef = (value: unknown): RawEvidenceRef => {
  const row = object(value);
  if (!row) throw new Error("retained evidence raw reference is malformed");
  return { path: string(row.path, "raw path"), sha256: string(row.sha256, "raw hash"), sourceRevision: string(row.sourceRevision, "source revision"), oracleRevision: string(row.oracleRevision, "oracle revision") };
};
const verifyRawEvidence = async (value: unknown): Promise<RawEvidenceRef> => {
  const ref = rawEvidenceRef(value);
  if (sha256(await readFile(ref.path, "utf8")) !== ref.sha256) throw new Error(`retained raw evidence changed: ${ref.path}`);
  return ref;
};
const verifiedArtifact = async (path: string, expectedHash: string): Promise<Record<string, unknown>> => {
  const text = await readFile(path, "utf8");
  if (sha256(text) !== expectedHash) throw new Error(`retained adjudication changed: ${path}`);
  const parsed = object(JSON.parse(text));
  if (!parsed) throw new Error(`retained adjudication is malformed: ${path}`);
  return parsed;
};
const pilotPhase = (name: string): string => join(pilotRoot, {
  "same-codex": "0005-row-same-codex-passed.json",
  "mixed-generic-to-codex": "0009-row-mixed-generic-to-codex-passed.json",
  "mixed-codex-to-generic": "0010-row-mixed-codex-to-generic-passed.json",
}[name] ?? "");
const independentExchange = (value: unknown, label: string): Readonly<{ sender: ParticipantIdentity; receiver: ParticipantIdentity; raw: readonly RawEvidenceRef[]; evidence: Parameters<typeof checkExchangeEvidence>[0]; pairId: string }> => {
  const row = object(value), lease = object(row?.lease), exchange = object(row?.exchange) ?? object(row?.evidence), arithmetic = independentArithmetic[label];
  const traceRows = object(row?.traces);
  const launched = object(row?.launched_pi_identities);
  const sender = identity(lease?.sender ?? row?.sender ?? launched?.sender ?? (Array.isArray(traceRows?.sender) ? object(traceRows.sender[0])?.actor : undefined));
  const receiver = identity(lease?.receiver ?? row?.receiver ?? launched?.receiver ?? (Array.isArray(traceRows?.receiver) ? object(traceRows.receiver[0])?.actor : undefined));
  if (!arithmetic || !exchange) throw new Error(`retained ${label} evidence is incomplete`);
  const requestObserved = object(exchange?.request);
  const nonceFromContent = typeof requestObserved?.content === "string" ? requestObserved.content.match(/^qualification (.+): calculate \d+\+\d+$/)?.[1] : undefined;
  const nonce = string(row?.nonce ?? object(row?.row)?.full_nonce ?? nonceFromContent, "qualification nonce");
  const requestContent = `qualification ${nonce}: calculate ${arithmetic.left}+${arithmetic.right}`;
  const expectedReplyContent = `answer ${nonce}: ${arithmetic.left + arithmetic.right}`;
  const request = requestObserved, reply = object(exchange.reply);
  if (string(request?.content, "request content") !== requestContent || string(reply?.content, "reply content") !== expectedReplyContent) throw new Error(`retained ${label} exchange does not match independent fixture arithmetic`);
  const rawValues = Array.isArray(row?.raw) ? row.raw : Array.isArray(row?.retained_raw_histories) ? row.retained_raw_histories : [];
  const evidence = { ...exchange, expected_reply_content: expectedReplyContent } as Parameters<typeof checkExchangeEvidence>[0];
  return { sender, receiver, raw: rawValues.map((ref) => rawEvidenceRef(ref)), evidence, pairId: `${sender.route}->${receiver.route}` };
};
const genericRoute = (route: string): boolean => route.startsWith("generic-");
type PilotAdmission = Readonly<{ attempt: AttemptRow; promptLifecycle: "settled" | "timed_out"; provenance: Readonly<Record<string, unknown>> }>;

const ownedDiscoveryReceiptPath = (value: string | undefined, field: string): string => {
  if (!value || value.trim().length === 0) throw new Error(`${field} is required when discovery checkpoint is enabled`);
  const path = resolve(value);
  const relative = relativePath(qualificationArtifactBase, path);
  if (relative.length === 0 || isAbsolute(relative) || relative.startsWith("..")) throw new Error(`${field} must be under the owned qualification artifact base`);
  return path;
};
const withinArtifact = (root: string, path: string): boolean => {
  const relative = relativePath(resolve(root), resolve(path));
  return relative.length > 0 && !isAbsolute(relative) && !relative.startsWith("..");
};
const parsedArtifact = async (path: string, label: string): Promise<Json> => {
  const value = object(JSON.parse(await readFile(path, "utf8")));
  if (!value) throw new Error(`${label} is malformed`);
  return value;
};
const equalHashMaps = (left: Record<string, unknown>, right: Record<string, unknown>): boolean => {
  const normalize = (value: Record<string, unknown>): ReadonlyArray<readonly [string, string]> => Object.entries(value).map(([path, hash]) => [path, string(hash, `${path} hash`)] as const).sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(normalize(left)) === JSON.stringify(normalize(right));
};
const verifyDiscoverySourceFreeze = async (receipt: Json, label: string): Promise<Readonly<Record<string, string>>> => {
  const before = object(receipt.source_hashes), after = object(receipt.source_hashes_after);
  if (!before || !after || receipt.source_hashes_match !== true || !equalHashMaps(before, after)) throw new Error(`${label} lacks an unchanged before/after source snapshot`);
  const expected = Object.fromEntries(Object.entries(before).map(([path, hash]) => [path, string(hash, `${label} source hash`)]));
  for (const [path, hash] of Object.entries(expected)) {
    const current = sha256Bytes(await readFile(resolve(path)));
    if (current !== hash) throw new Error(`${label} source drift detected: ${path}`);
  }
  return expected;
};
const verifyDiscoveryRawRef = async (value: unknown, receiptPath: string, label: string): Promise<RawEvidenceRef> => {
  const ref = rawEvidenceRef(value);
  if (!withinArtifact(dirname(receiptPath), ref.path)) throw new Error(`${label} raw history escapes its receipt artifact`);
  const bytes = await readFile(resolve(ref.path));
  if (sha256Bytes(bytes) !== ref.sha256) throw new Error(`${label} raw history bytes changed: ${ref.path}`);
  return ref;
};
const identityEqual = (left: ParticipantIdentity, right: ParticipantIdentity): boolean => JSON.stringify(left) === JSON.stringify(right);
const requireKnownRoute = (participant: ParticipantIdentity, label: string): ParticipantIdentity => {
  if (!frozenRoutes.some((route) => route.id === participant.route)) throw new Error(`${label} has an unknown route ${participant.route}`);
  return participant;
};
const requireTransportLifecycle = (receipt: Json, label: string, afterField: "transport_evidence_after_cleanup" | "transport_evidence_after_close"): void => {
  const before = object(receipt.transport_evidence_before_cleanup), after = object(receipt[afterField]);
  const validHash = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/iu.test(value);
  const positiveInteger = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;
  const terminal = after?.exit_code !== null && after?.exit_code !== undefined || after?.signal_code !== null && after?.signal_code !== undefined;
  const beforePid = before?.child_pid, afterPid = after?.child_pid, beforePort = before?.port, afterPort = after?.port;
  if (!before || !after || before.transport !== "http" || after.transport !== "http" || before.startup_ready !== true || after.startup_ready !== true || before.terminated !== false || after.terminated !== true || !positiveInteger(beforePid) || !positiveInteger(afterPid) || beforePid !== afterPid || !positiveInteger(beforePort) || !positiveInteger(afterPort) || beforePort !== afterPort || beforePort > 65_535 || afterPort > 65_535 || typeof before.node_path !== "string" || before.node_path.length === 0 || typeof after.node_path !== "string" || after.node_path.length === 0 || before.node_path !== after.node_path || typeof before.script_path !== "string" || before.script_path.length === 0 || typeof after.script_path !== "string" || after.script_path.length === 0 || before.script_path !== after.script_path || !validHash(before.script_sha256) || !validHash(after.script_sha256) || before.script_sha256 !== after.script_sha256 || !validHash(before.node_executable_sha256) || !validHash(after.node_executable_sha256) || before.node_executable_sha256 !== after.node_executable_sha256 || !terminal) throw new Error(`${label} lacks owned HTTP readiness/termination evidence`);
};
const discoveryStored = (value: unknown, label: string): Json => {
  const row = object(value);
  if (!row || typeof row.sourceId !== "string" || !object(row.rawHistoryRef) || !object(row.observedPeer)) throw new Error(`${label} lacks complete discovery provenance`);
  return row;
};
const assertDiscoveryStored = (stored: Json, observed: PeerDiscoveryEvidence, label: string): void => {
  const storedActor = identity(stored.actor), storedPeer = identity(stored.peer);
  if (!identityEqual(storedActor, observed.actor) || !identityEqual(storedPeer, observed.peer) || stored.sourceId !== observed.sourceId || JSON.stringify(rawEvidenceRef(stored.rawHistoryRef)) !== JSON.stringify(observed.rawHistoryRef) || JSON.stringify(stored.observedPeer) !== JSON.stringify(observed.observedPeer)) throw new Error(`${label} discovery provenance does not match its raw trace`);
};
const extractDiscoveryTraces = async (participant: ParticipantIdentity, ref: RawEvidenceRef): Promise<readonly NativeTrace[]> => {
  const history = JSON.parse(await readFile(resolve(ref.path), "utf8")) as unknown;
  if (genericRoute(participant.route)) {
    const callsValue = Array.isArray(history) ? history : object(history)?.calls;
    if (!Array.isArray(callsValue)) throw new Error(`generic discovery history is not a call array for ${participant.agent}`);
    return extractGenericTraces(callsValue.map(genericRecord), participant, ref, true);
  }
  return extractNativeTraces(history, participant, ref);
};
const collectStoredDiscovery = async (storedValue: unknown, actor: ParticipantIdentity, peer: ParticipantIdentity, receiptPath: string, label: string): Promise<Readonly<{ evidence: PeerDiscoveryEvidence; raw: RawEvidenceRef }>> => {
  const stored = discoveryStored(storedValue, label);
  const raw = await verifyDiscoveryRawRef(stored.rawHistoryRef, receiptPath, `${label} discovery`);
  const evidence = collectPeerDiscovery(actor, peer, await extractDiscoveryTraces(actor, raw));
  assertDiscoveryStored(stored, evidence, label);
  return { evidence, raw };
};
const genericParticipantToken = (participant: ParticipantIdentity): string => participant.participantId.replace(/[^A-Za-z0-9._-]+/g, "_");
const requireGenericDiscoveryRef = async (storedValue: unknown, participant: ParticipantIdentity, receiptPath: string, refs: readonly RawEvidenceRef[], label: string): Promise<RawEvidenceRef> => {
  const ref = await verifyDiscoveryRawRef(object(storedValue)?.rawHistoryRef, receiptPath, label);
  if (!refs.some((candidate) => JSON.stringify(candidate) === JSON.stringify(ref)) || !basename(ref.path).includes(genericParticipantToken(participant))) throw new Error(`${label} is not bound to the participant's retained discovery history`);
  return ref;
};
const genericFinalHistory = async (receiptPath: string, sourceHashes: Readonly<Record<string, string>>, receipt: Json): Promise<Readonly<{ histories: ReadonlyMap<string, GenericActorHistory>; raw: RawEvidenceRef }>> => {
  if (receipt.cleanup !== true || receipt.history_complete !== true) throw new Error("generic discovery receipt lacks complete cleanup/history evidence");
  const path = join(dirname(receiptPath), "history.json");
  const bytes = await readFile(path);
  const parsed = JSON.parse(bytes.toString("utf8")) as unknown;
  if (!Array.isArray(parsed)) throw new Error("generic discovery history is not a participant array");
  const manifest = await parsedArtifact(join(dirname(receiptPath), "hashes.json"), "generic discovery hash manifest");
  const manifestSources = object(manifest.source_hashes);
  if (!manifestSources || !equalHashMaps(manifestSources, sourceHashes) || manifest.history !== hashJson(parsed) || manifest.rows_hash !== hashJson(receipt.rows) || manifest.discovery_history !== hashJson(receipt.discovery_history_refs)) throw new Error("generic discovery hash manifest does not match its canonical receipt/history data");
  const histories = parsed.map(historyActor);
  const raw = { path, sha256: sha256Bytes(bytes), sourceRevision: hashJson(sourceHashes), oracleRevision: sourceHashes["tests/acceptance/oracle.ts"] ?? hashJson(sourceHashes) };
  return { histories: new Map(histories.map((entry) => [entry.actor.participantId, entry])), raw };
};
const genericDiscoveryAttempts = async (receiptPath: string, receipt: Json, sourceHashes: Readonly<Record<string, string>>): Promise<Readonly<{ attempts: readonly AttemptRow[]; provenance: readonly Json[] }>> => {
  if (receipt.run === undefined || !Array.isArray(receipt.rows) || receipt.rows.length !== 9) throw new Error("generic discovery receipt must contain exactly nine rows");
  requireTransportLifecycle(receipt, "generic discovery receipt", "transport_evidence_after_cleanup");
  const recordedRefs = Array.isArray(receipt.discovery_history_refs) ? await Promise.all(receipt.discovery_history_refs.map((ref) => verifyDiscoveryRawRef(ref, receiptPath, "generic discovery history"))) : [];
  if (recordedRefs.length === 0) throw new Error("generic discovery receipt has no discovery history references");
  const final = await genericFinalHistory(receiptPath, sourceHashes, receipt);
  const attempts: AttemptRow[] = [], provenance: Json[] = [];
  const seen = new Set<string>();
  for (const [index, value] of receipt.rows.entries()) {
    const row = object(value);
    if (!row) throw new Error(`generic row ${index} is malformed`);
    const verdict = object(row.verdict), sender = requireKnownRoute(identity(row.sender), `generic row ${index} sender`), receiver = requireKnownRoute(identity(row.receiver), `generic row ${index} receiver`);
    if (!genericRoute(sender.route) || !genericRoute(receiver.route) || sender.participantId === receiver.participantId || !identityEqual(final.histories.get(sender.participantId)?.actor ?? sender, sender) || !identityEqual(final.histories.get(receiver.participantId)?.actor ?? receiver, receiver)) throw new Error(`generic row ${index} identity/history binding failed`);
    const pairId = `${sender.route}->${receiver.route}`;
    if (seen.has(pairId) || row.cleanup !== undefined) throw new Error(`generic row ${index} duplicates a route pair or has malformed cleanup`);
    seen.add(pairId);
    const senderHistory = final.histories.get(sender.participantId), receiverHistory = final.histories.get(receiver.participantId);
    if (!senderHistory || !receiverHistory) throw new Error(`generic row ${index} lacks final actor history`);
    const discovery = object(row.discovery), senderStored = discovery?.sender_to_receiver, receiverStored = discovery?.receiver_to_sender;
    const senderRaw = await requireGenericDiscoveryRef(senderStored, sender, receiptPath, recordedRefs, `generic row ${index} sender discovery`);
    const receiverRaw = await requireGenericDiscoveryRef(receiverStored, receiver, receiptPath, recordedRefs, `generic row ${index} receiver discovery`);
    const senderDiscovery = await collectStoredDiscovery(senderStored, sender, receiver, receiptPath, `generic row ${index} sender_to_receiver`);
    const receiverDiscovery = await collectStoredDiscovery(receiverStored, receiver, sender, receiptPath, `generic row ${index} receiver_to_sender`);
    if (senderRaw.path === receiverRaw.path) throw new Error(`generic row ${index} reuses one discovery history`);
    const request = object(row.received), requestPayload = object(request?.payload), requestContent = string(requestPayload?.content, `generic row ${index} request content`), expectedReplyContent = `reply:${requestContent}`;
    const traces = [...extractGenericTraces(senderHistory.records, sender, final.raw, true), ...extractGenericTraces(receiverHistory.records, receiver, final.raw, true)];
    const collected = collectGenericExchange({ sender, receiver, nonce: requestContent, requestContent, expectedReplyContent, sent: row.sent, received: row.received, replied: row.replied, returned: row.returned, traces, histories: { sender: senderHistory, receiver: receiverHistory } });
    const checked = checkExchangeEvidence(collected.evidence);
    if (checked.outcome !== "meets" || verdict?.outcome !== "meets") throw new Error(`generic row ${index} failed exact exchange oracle`);
    const attemptId = `discovery-generic-${index}-${String(receipt.run)}`;
    attempts.push({ kind: "communication", attemptId, pairId, sender, receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: "generic exchange and bidirectional list_agents discovery rechecked from same-run histories" }, raw: [final.raw, senderDiscovery.raw, receiverDiscovery.raw] });
    provenance.push({ row: index, attemptId, discovery: { sender_to_receiver: senderDiscovery.evidence, receiver_to_sender: receiverDiscovery.evidence }, raw: [final.raw, senderDiscovery.raw, receiverDiscovery.raw] });
  }
  if (seen.size !== 9) throw new Error(`generic discovery receipt covers ${seen.size} unique route pairs, expected 9`);
  return { attempts, provenance };
};
const cohortDiscoveryAttempts = async (receiptPath: string, receipt: Json): Promise<Readonly<{ attempts: readonly AttemptRow[]; provenance: readonly Json[] }>> => {
  if (receipt.run_id === undefined || object(receipt.execution)?.status !== "completed" || receipt.passed !== true || !Array.isArray(receipt.rows) || receipt.rows.length !== 16 || receipt.rows.some((value) => object(value)?.status !== "passed")) throw new Error("cohort discovery receipt must contain exactly sixteen passed rows");
  const declaredPlan = object(receipt.declared_plan), cleanup = Array.isArray(receipt.cleanup) ? receipt.cleanup : [];
  if (declaredPlan?.id !== "transport" || !Array.isArray(declaredPlan.rows) || declaredPlan.rows.length !== 16 || declaredPlan.transport_evidence !== true || receipt.unfinished_rows === undefined || !Array.isArray(receipt.unfinished_rows) || receipt.unfinished_rows.length !== 0 || cleanup.length !== 15 || cleanup.some((entry) => object(entry)?.status !== "fulfilled")) throw new Error("cohort discovery receipt lacks the complete transport plan, cleanup, or row accounting");
  requireTransportLifecycle(receipt, "cohort discovery receipt", "transport_evidence_after_close");
  const launchedPhase = Array.isArray(receipt.phases) ? receipt.phases.map(object).find((phase) => phase?.label === "launched") : undefined;
  const launchedValue = object(launchedPhase?.value), launchedRows = Array.isArray(launchedValue?.participants) ? launchedValue.participants : [];
  if (launchedRows.length === 0) throw new Error("cohort discovery receipt lacks launched participant identities");
  const launchedById = new Map(launchedRows.map((entry) => { const participant = phaseIdentity(entry); return [participant.participantId, participant] as const; }));
  const attempts: AttemptRow[] = [], provenance: Json[] = [], seen = new Set<string>(), lifecycleCounts = { timed_out: 0, settled: 0 };
  for (const [index, value] of receipt.rows.entries()) {
    const row = object(value);
    if (!row) throw new Error(`cohort row ${index} is malformed`);
    const lease = object(row.lease), pair = object(lease?.pair), arithmetic = typeof row.id === "string" ? discoveryArithmetic[row.id] : undefined;
    if (!row || !lease || !pair || !arithmetic) throw new Error(`cohort row ${index} lacks an independently known fixture`);
    const sender = requireKnownRoute(identity(lease.sender), `cohort row ${index} sender`), receiver = requireKnownRoute(identity(lease.receiver), `cohort row ${index} receiver`);
    const launchedSender = launchedById.get(sender.participantId), launchedReceiver = launchedById.get(receiver.participantId);
    if (!launchedSender || !launchedReceiver || !identityEqual(launchedSender, sender) || !identityEqual(launchedReceiver, receiver)) throw new Error(`cohort row ${index} lease identity is not bound to the launched phase`);
    const pairId = `${sender.route}->${receiver.route}`;
    if (seen.has(pairId) || pair.sender !== sender.route || pair.receiver !== receiver.route || pair.nonce !== row.nonce || row.expected_decimal !== arithmetic.left + arithmetic.right || sender.participantId === receiver.participantId) throw new Error(`cohort row ${index} has a lease/route/nonce/arithmetic mismatch`);
    seen.add(pairId);
    const rawValues = Array.isArray(row.raw) ? row.raw : [];
    if (rawValues.length !== 2) throw new Error(`cohort row ${index} lacks two exchange histories`);
    const refs = await Promise.all(rawValues.map((ref, refIndex) => verifyDiscoveryRawRef(ref, receiptPath, `cohort row ${index} exchange ${refIndex}`)));
    const senderIndex = genericRoute(sender.route) ? 1 : genericRoute(receiver.route) ? 0 : 0;
    const receiverIndex = genericRoute(receiver.route) ? 1 : genericRoute(sender.route) ? 0 : 1;
    const senderTraces = await extractDiscoveryTraces(sender, refs[senderIndex]!);
    const receiverTraces = await extractDiscoveryTraces(receiver, refs[receiverIndex]!);
    const requestContent = `qualification ${String(row.nonce)}: calculate ${arithmetic.left}+${arithmetic.right}`;
    const expectedReplyContent = `answer ${String(row.nonce)}: ${arithmetic.left + arithmetic.right}`;
    const exchange = collectNativeExchange({ sender: { actor: sender, traces: senderTraces }, receiver: { actor: receiver, traces: receiverTraces }, nonce: String(row.nonce), requestContent, expectedReplyContent, consumption: "legacy_receive" });
    const verdict = checkExchangeEvidence(exchange.evidence), storedVerdict = object(row.verdict);
    if (verdict.outcome !== "meets" || storedVerdict?.outcome !== "meets") throw new Error(`cohort row ${index} failed exact exchange oracle`);
    const lifecycle = /prompt lifecycle (timed_out|settled)/u.exec(string(object(row.execution)?.detail, `cohort row ${index} execution detail`))?.[1] as "timed_out" | "settled" | undefined;
    if (!lifecycle) throw new Error(`cohort row ${index} lacks prompt lifecycle evidence`);
    lifecycleCounts[lifecycle] += 1;
    const discovery = object(row.discovery), senderStored = discovery?.sender, receiverStored = discovery?.receiver;
    const senderDiscovery = await collectStoredDiscovery(senderStored, sender, receiver, receiptPath, `cohort row ${index} sender`);
    const receiverDiscovery = await collectStoredDiscovery(receiverStored, receiver, sender, receiptPath, `cohort row ${index} receiver`);
    const attemptId = `discovery-cohort-${String(row.id)}-${String(receipt.run_id)}`;
    attempts.push({ kind: "communication", attemptId, pairId, sender, receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: `cohort exchange and bidirectional list_agents discovery rechecked from same-run histories; prompt lifecycle ${lifecycle}` }, raw: [...refs, senderDiscovery.raw, receiverDiscovery.raw] });
    provenance.push({ row: row.id, attemptId, promptLifecycle: lifecycle, discovery: { sender: senderDiscovery.evidence, receiver: receiverDiscovery.evidence }, exchangeRaw: refs, lease });
  }
  if (seen.size !== 16) throw new Error(`cohort discovery receipt covers ${seen.size} unique route pairs, expected 16`);
  if (lifecycleCounts.timed_out !== 11 || lifecycleCounts.settled !== 5) throw new Error(`cohort prompt lifecycle counts changed: ${JSON.stringify(lifecycleCounts)}`);
  provenance.push({ lifecycleCounts });
  return { attempts, provenance };
};
const runDiscoveryCheckpoint = async (): Promise<void> => {
  const genericReceiptPath = ownedDiscoveryReceiptPath(process.env.GPTQUEUE_DISCOVERY_GENERIC_RECEIPT, "GPTQUEUE_DISCOVERY_GENERIC_RECEIPT");
  const cohortReceiptPath = ownedDiscoveryReceiptPath(process.env.GPTQUEUE_DISCOVERY_COHORT_RECEIPT, "GPTQUEUE_DISCOVERY_COHORT_RECEIPT");
  const genericManifestPath = join(dirname(genericReceiptPath), "hashes.json");
  const inputArtifactPaths = [genericReceiptPath, cohortReceiptPath, genericManifestPath] as const;
  const inputArtifactHashesBefore = Object.fromEntries(await Promise.all(inputArtifactPaths.map(async (path) => [path, sha256Bytes(await readFile(path))] as const)));
  const historicalBytes = await readFile(historicalExchangeOnlyPath);
  const historicalExchangeOnlyHash = sha256Bytes(historicalBytes);
  const [genericReceipt, cohortReceipt] = await Promise.all([
    parsedArtifact(genericReceiptPath, "generic discovery receipt"),
    parsedArtifact(cohortReceiptPath, "cohort discovery receipt"),
  ]);
  const [genericSourceHashes, cohortSourceHashes] = await Promise.all([
    verifyDiscoverySourceFreeze(genericReceipt, "generic discovery receipt"),
    verifyDiscoverySourceFreeze(cohortReceipt, "cohort discovery receipt"),
  ]);
  const [generic, cohort] = await Promise.all([
    genericDiscoveryAttempts(genericReceiptPath, genericReceipt, genericSourceHashes),
    cohortDiscoveryAttempts(cohortReceiptPath, cohortReceipt),
  ]);
  const inputArtifactHashesAfter = Object.fromEntries(await Promise.all(inputArtifactPaths.map(async (path) => [path, sha256Bytes(await readFile(path))] as const)));
  if (!equalHashMaps(inputArtifactHashesBefore, inputArtifactHashesAfter)) throw new Error("discovery input receipt or hash manifest changed during adjudication");
  const communicationAttempts = [...generic.attempts, ...cohort.attempts];
  if (generic.attempts.length !== 9 || cohort.attempts.length !== 16 || new Set(communicationAttempts.map((attempt) => attempt.kind === "communication" ? attempt.pairId : "")).size !== 25) throw new Error("discovery checkpoint does not contain 25 unique communication route pairs");
  const genericRoutes = new Set(frozenRoutes.filter((route) => !route.modelBacked).map((route) => route.id));
  const evidenced = new Set(["generic-stdio", "generic-http", "generic-stateless", "codex-appserver", "pi-rpc-cli"]);
  const routes: readonly RouteSpec[] = frozenRoutes.map((route) => {
    const blocked = route.id === "claude-cli" || route.id === "claude-native-child" || route.id === "gemini-cli";
    return { ...route, availability: blocked
      ? { kind: "blocked_prerequisite", detail: route.id === "gemini-cli" ? "historical tier blocker; no fresh claim" : "fresh authentication unavailable" }
      : evidenced.has(route.id)
        ? { kind: "available" }
        : { kind: "setup_gap", detail: "no discovery-qualified receipt for this route" } };
  });
  const attempts: AttemptRow[] = [...communicationAttempts];
  for (const obligation of frozenDimensionObligations) {
    if (!genericRoutes.has(obligation.route)) continue;
    attempts.push({
      kind: obligation.kind === "registration" ? "registration" : obligation.kind,
      attemptId: `discovery-generic-not-applicable-${obligation.id}`, rowId: obligation.id, route: obligation.route,
      status: "unrun", outcome: "not_applicable", execution: { status: "not_run", detail: "generic route has no model automatic or initiative contract" },
      detail: "explicit nonrequired generic dimension",
    });
  }
  const report = buildQualificationReport({ routes, obligations: frozenPairMatrix, attempts, expectedDimensionObligations: frozenDimensionObligations, admittedAttemptIds: [...communicationAttempts.map((attempt) => attempt.attemptId), ...attempts.filter((attempt) => attempt.kind !== "communication").map((attempt) => attempt.attemptId)] });
  const pairIds = new Set<string>(frozenPairMatrix.map((pair) => pair.pairId));
  const communicationRows = report.rows.filter((row) => pairIds.has(row.id));
  const availableCounts = Object.fromEntries(["completed", "unrun", "setup_gap", "blocked"].map((status) => [status, communicationRows.filter((row) => row.status === status).length]));
  if (report.total.outcome === "meets" || availableCounts.completed !== 25 || availableCounts.unrun !== 0 || availableCounts.setup_gap !== 459 || availableCounts.blocked !== 141) throw new Error(`discovery checkpoint produced unexpected report counts: ${JSON.stringify(availableCounts)}`);
  const run = randomUUID();
  const output = {
    run, kind: "offline-discovery-qualified-checkpoint", generatedAt: new Date().toISOString(),
    contract: { routeCount: frozenRoutes.length, pairCount: frozenPairMatrix.length, dimensionObligationCount: frozenDimensionObligations.length, requiredDimensionObligations: frozenDimensionObligations.filter((row) => row.required).length },
    receipts: { generic: genericReceiptPath, cohort: cohortReceiptPath, genericHashManifest: genericManifestPath, genericSourceHashes, cohortSourceHashes },
    historicalExchangeOnlyReference: { path: historicalExchangeOnlyPath, sha256: historicalExchangeOnlyHash },
    inputArtifactHashesBefore, inputArtifactHashesAfter,
    communication: { expectedGenericRows: 9, expectedCohortRows: 16, attempts: communicationAttempts, genericProvenance: generic.provenance, cohortProvenance: cohort.provenance, availableRoutes: [...evidenced], availablePairCounts: availableCounts },
    report,
    counts: { availablePairRows: availableCounts, total: report.total.outcome },
  };
  const dir = join(artifactRoot, run);
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "checkpoint.json"), `${JSON.stringify(output, null, 2)}\n`, "utf8");
  await writeFile(join(dir, "checkpoint.md"), `# Discovery-qualified checkpoint ${run}\n\n- Discovery-qualified communication pairs: 25/25 across five available routes.\n- Remaining available route pairs: 0.\n- Frozen contract: ${frozenRoutes.length} routes, ${frozenPairMatrix.length} ordered pairs, ${frozenDimensionObligations.length} dimension rows.\n- Historical exchange-only checkpoint retained at ${output.historicalExchangeOnlyReference.path}.\n- No native/model/Redis calls were made by this offline gate.\n`, "utf8");
  expect(output.contract.pairCount).toBe(625);
  expect(output.communication.attempts).toHaveLength(25);
  expect(output.communication.availablePairCounts).toEqual({ completed: 25, unrun: 0, setup_gap: 459, blocked: 141 });
};
const recheckCohortPilotRow = async (value: unknown): Promise<PilotAdmission> => {
  const row = object(value), lease = object(row?.lease), pair = object(lease?.pair), arithmetic = typeof row?.id === "string" ? independentArithmetic[row.id] : undefined;
  if (!row || !lease || !pair || !arithmetic || row.status !== "passed") throw new Error("fed37 cohort row lacks a passed lease/arithmetic record");
  const sender = identity(lease.sender), receiver = identity(lease.receiver);
  const nonce = string(row.nonce, "pilot nonce");
  if (pair.sender !== sender.route || pair.receiver !== receiver.route || pair.nonce !== nonce || row.expected_decimal !== arithmetic.left + arithmetic.right) throw new Error(`fed37 row ${String(row.id)} has a lease/nonce/arithmetic mismatch`);
  const rawValues = Array.isArray(row.raw) ? row.raw : [];
  const refs = await Promise.all(rawValues.map(verifyRawEvidence));
  if (refs.length !== 2) throw new Error(`fed37 row ${String(row.id)} lacks two raw histories`);
  const histories = await Promise.all(refs.map(async (ref) => JSON.parse(await readFile(ref.path, "utf8")) as unknown));
  const genericIndex = genericRoute(sender.route) || genericRoute(receiver.route) ? 1 : -1;
  const historyFor = (participant: ParticipantIdentity): Readonly<{ history: unknown; ref: RawEvidenceRef }> => {
    const index = genericIndex >= 0 ? (genericRoute(participant.route) ? 1 : 0) : participant === sender ? 0 : 1;
    const history = histories[index]; const ref = refs[index];
    if (history === undefined || ref === undefined) throw new Error(`fed37 row ${String(row.id)} cannot bind raw history to ${participant.agent}`);
    return { history, ref };
  };
  const tracesFor = (participant: ParticipantIdentity): readonly import("./qualification-evidence.js").NativeTrace[] => {
    const bound = historyFor(participant);
    return genericRoute(participant.route)
      ? extractGenericTraces(Array.isArray(bound.history) ? bound.history.map(genericRecord) : [], participant, bound.ref, true)
      : extractNativeTraces(bound.history, participant, bound.ref);
  };
  const requestContent = `qualification ${nonce}: calculate ${arithmetic.left}+${arithmetic.right}`;
  const expectedReplyContent = `answer ${nonce}: ${arithmetic.left + arithmetic.right}`;
  let collected: ReturnType<typeof collectNativeExchange>;
  try {
    collected = collectNativeExchange({ sender: { actor: sender, traces: tracesFor(sender) }, receiver: { actor: receiver, traces: tracesFor(receiver) }, nonce, requestContent, expectedReplyContent, consumption: "legacy_receive" });
  } catch (error) {
    throw new Error(`fed37 row ${String(row.id)} native collector failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
  const verdict = checkExchangeEvidence(collected.evidence);
  if (verdict.outcome !== "meets") throw new Error(`fed37 row ${String(row.id)} failed current oracle: ${verdict.reasons.join("; ")}`);
  const detail = string(object(row.execution)?.detail, "pilot execution detail");
  const lifecycle = /prompt lifecycle (timed_out|settled)/.exec(detail)?.[1] as "settled" | "timed_out" | undefined;
  if (!lifecycle) throw new Error(`fed37 row ${String(row.id)} lacks prompt lifecycle evidence`);
  const attemptId = `retained-cohort-pilot-${string(row.id, "pilot row id")}-fed37cf9`;
  return { attempt: { kind: "communication", attemptId, pairId: `${sender.route}->${receiver.route}`, sender, receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: `exact native exchange rechecked; prompt lifecycle ${lifecycle} retained separately` }, raw: refs }, promptLifecycle: lifecycle, provenance: { row: value, attemptId, lease, raw: refs, currentOracle: verdict, storedExchange: row.exchange } };
};

const runCheckpoint = async (): Promise<void> => {
  const run = randomUUID();
  const evidenceIndex = object(JSON.parse(await readFile(evidenceIndexPath, "utf8")));
  const references = object(evidenceIndex?.references);
  const pilotReceiptPath = join(pilotRoot, "receipt-sanitized.json");
  const pilot = await verifiedArtifact(pilotReceiptPath, string(object(references?.latest_legacy_pilot)?.sha256, "pilot receipt hash"));
  if (pilot.source_hashes_match !== true || hashJson(pilot.source_hashes) !== hashJson(pilot.source_hashes_after) || !Array.isArray(pilot.cleanup) || pilot.cleanup.length !== 11 || !pilot.cleanup.every((entry) => object(entry)?.status === "fulfilled")) throw new Error("retained pilot lacks source freeze or complete cleanup");
  const cohortPilot = await verifiedArtifact(cohortPilotReceiptPath, "1f0b700233796e14325fceb2b62e9433422f473b09fb69451717bfb6f02db3d1");
  if (cohortPilot.execution === undefined || object(cohortPilot.execution)?.status !== "completed" || cohortPilot.passed !== true || !Array.isArray(cohortPilot.rows) || cohortPilot.rows.length !== 8 || cohortPilot.rows.some((row) => object(row)?.status !== "passed") || (Array.isArray(cohortPilot.unfinished_rows) && cohortPilot.unfinished_rows.length !== 0) || cohortPilot.source_hashes_match !== true || hashJson(cohortPilot.source_hashes) !== hashJson(cohortPilot.source_hashes_after) || !Array.isArray(cohortPilot.cleanup) || cohortPilot.cleanup.length !== 11 || !cohortPilot.cleanup.every((entry) => object(entry)?.status === "fulfilled")) throw new Error("fed37 cohort lacks complete passed rows, source freeze or cleanup");
  const cohortSourceHashes = object(cohortPilot.source_hashes);
  if (!cohortSourceHashes) throw new Error("fed37 cohort lacks source hashes");
  const cohortSourceDrift: readonly Readonly<{ path: string; retained: string; current: string }>[] = (await Promise.all(Object.entries(cohortSourceHashes).map(async ([path, expected]) => {
    const current = sha256Bytes(await readFile(resolve(path)));
    return current === expected ? undefined : { path, retained: expected, current };
  }))).filter((entry): entry is Readonly<{ path: string; retained: string; current: string }> => entry !== undefined);
  const cohortAdmissions = await Promise.all(cohortPilot.rows.map(recheckCohortPilotRow));
  const launchedPhase = Array.isArray(cohortPilot.phases) ? cohortPilot.phases.map(object).find((phase) => phase?.label === "launched") : undefined;
  const launchedParticipants = object(launchedPhase?.value);
  const launchedRows = Array.isArray(launchedParticipants?.participants) ? launchedParticipants.participants : [];
  const launchedById = new Map(launchedRows.map((entry) => { const participant = phaseIdentity(entry); return [participant.participantId, participant] as const; }));
  for (const { attempt } of cohortAdmissions) {
    const launchedSender = launchedById.get(attempt.kind === "communication" ? attempt.sender.participantId : "");
    const launchedReceiver = launchedById.get(attempt.kind === "communication" ? attempt.receiver.participantId : "");
    if (attempt.kind !== "communication" || !launchedSender || !launchedReceiver || JSON.stringify(launchedSender) !== JSON.stringify(attempt.sender) || JSON.stringify(launchedReceiver) !== JSON.stringify(attempt.receiver)) throw new Error(`fed37 row ${attempt.attemptId} lease identity is not bound to the launched phase`);
  }
  const receipt = object(JSON.parse(await readFile(join(retained, "receipt.json"), "utf8")));
  const historyRows = JSON.parse(await readFile(join(retained, "history.json"), "utf8")) as unknown;
  const hashes = object(JSON.parse(await readFile(join(retained, "hashes.json"), "utf8")));
  const sourceHashes = object(hashes?.source_hashes);
  if (receipt?.cleanup !== true || receipt.history_complete !== true || !sourceHashes) throw new Error("retained generic execution lacks cleanup, history or source provenance");
  if (hashes?.history !== hashJson(historyRows) || hashes.rows_hash !== hashJson(receipt.rows)) throw new Error("retained generic evidence digest mismatch");
  for (const [path, expected] of Object.entries(sourceHashes)) {
    if (sha256(await readFile(path, "utf8")) !== expected) throw new Error(`retained generic dependency changed: ${path}`);
  }
  const historyText = await readFile(join(retained, "history.json"), "utf8");
  const oracleText = await readFile("tests/acceptance/oracle.ts", "utf8");
  const rows = Array.isArray(receipt?.rows) ? receipt.rows.map(receiptRow) : [];
  const histories = Array.isArray(historyRows) ? historyRows.map(historyActor) : [];
  const historyByAgent = new Map(histories.map((entry) => [entry.actor.agent, entry]));
  const historyHash = sha256(historyText);
  const sourceRevision = hashJson(sourceHashes ?? {});
  const oracleRevision = sha256(oracleText);
  const raw = (label: string) => ({ path: `${retained}/${label}`, sha256: historyHash, sourceRevision, oracleRevision });
  const attempts: AttemptRow[] = [];

  for (const [index, row] of rows.entries()) {
    const request = object(row.received);
    const returned = object(row.returned);
    const requestPayload = object(request?.payload);
    const returnedPayload = object(returned?.payload);
    const requestContent = string(requestPayload?.content, "request content");
    // The retained generic fixture specifies this transform independently of the observed reply.
    const expectedReplyContent = `reply:${requestContent}`;
    string(returnedPayload?.content, "reply content");
    const senderHistory = historyByAgent.get(row.sender.agent);
    const receiverHistory = historyByAgent.get(row.receiver.agent);
    if (!senderHistory || !receiverHistory) throw new Error(`generic row ${index} has no complete participant history`);
    const traces = [
      ...extractGenericTraces(senderHistory.records, row.sender, raw("history.json")),
      ...extractGenericTraces(receiverHistory.records, row.receiver, raw("history.json")),
    ];
    const collected = collectGenericExchange({
      sender: row.sender, receiver: row.receiver, nonce: requestContent, requestContent, expectedReplyContent,
      sent: row.sent, received: row.received, replied: row.replied, returned: row.returned, traces,
      histories: { sender: senderHistory, receiver: receiverHistory },
    });
    const verdict = checkExchangeEvidence(collected.evidence);
    if (verdict.outcome !== row.verdict.outcome || verdict.outcome !== "meets") throw new Error(`retained generic row ${index} failed unchanged oracle`);
    attempts.push({
      kind: "communication", attemptId: `retained-generic-${index}`, pairId: `${row.sender.route}->${row.receiver.route}`,
      sender: row.sender, receiver: row.receiver, status: "completed", outcome: verdict.outcome,
      execution: { status: "completed", detail: "retained generic9 receipt re-evaluated through exact collector and oracle" }, raw: [raw("history.json")],
    });
  }

  const additionalAttempts: AttemptRow[] = [];
  const failedAdditionalAttempts: AttemptRow[] = [];
  const additionalProvenance: Array<Readonly<Record<string, unknown>>> = [];
  const admitRetained = async (label: string, phaseFile: string): Promise<void> => {
    const value = Array.isArray(pilot.rows) ? pilot.rows.map(object).find((row) => row?.id === label) : undefined;
    if (!value || value.status !== "passed") throw new Error(`retained ${label} pilot row is not passed`);
    const exchange = independentExchange(value, label);
    const raw = await Promise.all((Array.isArray(value.raw) ? value.raw : []).map(verifyRawEvidence));
    if (raw.length !== 2) throw new Error(`retained ${label} pilot row lacks both raw histories`);
    const verdict = checkExchangeEvidence(exchange.evidence);
    if (verdict.outcome !== "meets") throw new Error(`retained ${label} failed current oracle: ${verdict.reasons.join("; ")}`);
    const attemptId = `retained-pilot-${label}-8993b003`;
    additionalAttempts.push({ kind: "communication", attemptId, pairId: exchange.pairId, sender: exchange.sender, receiver: exchange.receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: "retained pilot exchange rechecked with independent fixture arithmetic and current oracle" }, raw });
    additionalProvenance.push({ label, artifact: pilotReceiptPath, originalPhase: phaseFile, attemptId, nonce: string(value.nonce, "qualification nonce"), raw, lease: value.lease });
  };
  await admitRetained("same-codex", pilotPhase("same-codex"));
  await admitRetained("mixed-generic-to-codex", pilotPhase("mixed-generic-to-codex"));
  await admitRetained("mixed-codex-to-generic", pilotPhase("mixed-codex-to-generic"));

  const crossReference = object(references?.codex_to_pi);
  const cross = await verifiedArtifact(crossAdjudicationPath, string(crossReference?.sha256, "codex-to-pi artifact hash"));
  const crossExchange = independentExchange(cross, "codex-to-pi");
  const crossRaw = await Promise.all((Array.isArray(cross.raw) ? cross.raw : []).map(verifyRawEvidence));
  if (crossRaw.length !== 2 || cross.verdict === undefined || object(cross.originalReceipt)?.execution === undefined) throw new Error("codex-to-pi adjudication lacks raw/original failure provenance");
  const crossVerdict = checkExchangeEvidence(crossExchange.evidence);
  if (crossVerdict.outcome !== "meets") throw new Error(`retained codex-to-pi failed current oracle: ${crossVerdict.reasons.join("; ")}`);
  const crossSuccessId = "retained-pilot-codex-to-pi-adjudicated-9b91104a";
  const crossFailedId = "retained-pilot-codex-to-pi-original-failed-9b91104a";
  additionalAttempts.push({ kind: "communication", attemptId: crossSuccessId, pairId: crossExchange.pairId, sender: crossExchange.sender, receiver: crossExchange.receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: "retained programmatic adjudication rechecked with current oracle" }, raw: crossRaw });
  failedAdditionalAttempts.push({ kind: "communication", attemptId: crossFailedId, pairId: crossExchange.pairId, sender: crossExchange.sender, receiver: crossExchange.receiver, status: "failed", outcome: "uncertain", execution: { status: "failed", detail: string(object(cross.originalReceipt)?.execution && object(object(cross.originalReceipt)?.execution)?.detail, "original failure detail") }, raw: crossRaw, detail: "original failed attempt retained; adjudicated evidence is admitted separately" });
  additionalProvenance.push({ label: "codex-to-pi", artifact: crossAdjudicationPath, attemptId: crossSuccessId, sourceArtifactHash: string(crossReference?.sha256, "codex-to-pi artifact hash"), originalFailure: cross.originalReceipt, raw: crossRaw });

  const pi = object(JSON.parse(await readFile(piAdjudicationPath, "utf8")));
  if (!pi) throw new Error("Pi same-route adjudication is malformed");
  const piExchange = independentExchange(pi, "same-pi");
  const piRaw = await Promise.all((Array.isArray(pi.retained_raw_histories) ? pi.retained_raw_histories : []).map(verifyRawEvidence));
  const piVerdict = checkExchangeEvidence(piExchange.evidence);
  if (piRaw.length !== 2 || piVerdict.outcome !== "meets") throw new Error(`retained same-pi failed current oracle: ${piVerdict.reasons.join("; ")}`);
  const piSuccessId = "retained-pilot-same-pi-claim-ack-adjudicated-a3a60457";
  const piFailedId = "retained-pilot-same-pi-original-failed-1dea7d7c";
  additionalAttempts.push({ kind: "communication", attemptId: piSuccessId, pairId: piExchange.pairId, sender: piExchange.sender, receiver: piExchange.receiver, status: "completed", outcome: "meets", execution: { status: "completed", detail: "retained Pi claim/ack adjudication rechecked with current oracle" }, raw: piRaw });
  const piOriginalRef = object(pi.original_receipt);
  const piOriginalReceipt = await verifiedArtifact(string(piOriginalRef?.path, "original Pi receipt path"), string(piOriginalRef?.sha256, "original Pi receipt hash"));
  const originalPi = Array.isArray(piOriginalReceipt.rows) ? piOriginalReceipt.rows.map(object).find((row) => row?.id === "same-pi") : undefined;
  const originalPiObject = object(originalPi);
  const originalPiRawValues: readonly unknown[] = Array.isArray(originalPiObject?.raw) ? originalPiObject.raw : [];
  const originalPiRaw = await Promise.all(originalPiRawValues.map(verifyRawEvidence));
  failedAdditionalAttempts.push({ kind: "communication", attemptId: piFailedId, pairId: piExchange.pairId, sender: piExchange.sender, receiver: piExchange.receiver, status: "failed", outcome: "uncertain", execution: { status: "failed", detail: string(object(originalPi)?.error, "original Pi failure") }, raw: originalPiRaw, detail: "original failed attempt retained; corrected claim/ack evidence is admitted separately" });
  additionalProvenance.push({ label: "same-pi", artifact: piAdjudicationPath, attemptId: piSuccessId, sourceArtifactHash: sha256(await readFile(piAdjudicationPath, "utf8")), originalFailure: originalPi, raw: piRaw });
  const cohortAttempts = cohortAdmissions.map(({ attempt }) => attempt);
  const historicalPositivePairs = new Set([...attempts, ...additionalAttempts].filter((attempt) => attempt.kind === "communication" && attempt.status === "completed").map((attempt) => attempt.kind === "communication" ? attempt.pairId : ""));
  const corroboratingCohortAttemptIds = new Set(cohortAttempts.filter((attempt) => attempt.kind === "communication" && historicalPositivePairs.has(attempt.pairId)).map((attempt) => attempt.attemptId));
  attempts.push(...failedAdditionalAttempts, ...additionalAttempts, ...cohortAttempts);

  const genericRoutes = new Set(frozenRoutes.filter((route) => !route.modelBacked).map((route) => route.id));
  for (const obligation of frozenDimensionObligations) {
    if (!genericRoutes.has(obligation.route)) continue;
    attempts.push({
      kind: obligation.kind === "registration" ? "registration" : obligation.kind,
      attemptId: `retained-generic-not-applicable-${obligation.id}`, rowId: obligation.id, route: obligation.route,
      status: "unrun", outcome: "not_applicable", execution: { status: "not_run", detail: "generic route has no model automatic or initiative contract" },
      detail: "explicit nonrequired generic dimension",
    });
  }

  const routes: readonly RouteSpec[] = frozenRoutes.map((route) => {
    const blocked = route.id === "claude-cli" || route.id === "claude-native-child" || route.id === "gemini-cli";
    const evidenced = new Set(["generic-stdio", "generic-http", "generic-stateless", "codex-appserver", "pi-rpc-cli"]);
    return { ...route, availability: blocked
      ? { kind: "blocked_prerequisite", detail: route.id === "gemini-cli" ? "historical tier blocker; no fresh claim" : "fresh authentication unavailable" }
      : evidenced.has(route.id)
        ? { kind: "available" }
      : { kind: "setup_gap", detail: "no fresh reusable adapter/preflight evidence" } };
  });
  const admittedAttemptIds = [...attempts.filter((attempt) => attempt.kind === "communication" && attempt.status === "completed" && !corroboratingCohortAttemptIds.has(attempt.attemptId)).map((attempt) => attempt.attemptId), ...attempts.filter((attempt) => attempt.kind !== "communication").map((attempt) => attempt.attemptId)];
  const report = buildQualificationReport({ routes, obligations: frozenPairMatrix, attempts, expectedDimensionObligations: frozenDimensionObligations, admittedAttemptIds });
  if (report.total.outcome === "meets") throw new Error("checkpoint cannot claim a full pass while required rows are unresolved");
  const statusCounts = Object.fromEntries(Object.entries(report.counts)) as Record<string, number>;
  const outcomeCounts = Object.fromEntries(Object.entries(report.outcomes)) as Record<string, number>;
  const output = {
    run, kind: "offline-fresh-qualification-checkpoint", generatedAt: new Date().toISOString(),
    contract: { routeCount: frozenRoutes.length, pairCount: frozenPairMatrix.length, dimensionObligationCount: frozenDimensionObligations.length, requiredDimensionObligations: frozenDimensionObligations.filter((row) => row.required).length },
    provenance: { retainedRun: "52c5a060-62d3-4675-af88-d8194b7ac381", sourceHashes, sourceRevision, oracleRevision, historyHash },
    preflight: routes.map(({ id, availability }) => ({ id, availability })),
    genericReevaluation: { receiptRows: rows.length, exactOracleMeets: rows.length },
    additionalCommunication: { admitted: additionalAttempts, failedOriginals: failedAdditionalAttempts, provenance: additionalProvenance, uniqueRoutePairs: [...new Set(additionalAttempts.map((attempt) => attempt.kind === "communication" ? attempt.pairId : ""))] },
    cohortPilot: { receipt: cohortPilotReceiptPath, sourceHashes: cohortSourceHashes, sourceDrift: cohortSourceDrift, sourceDriftPolicy: "retained before/after hashes are authoritative for this historical run; current drift is reported and does not rewrite its provenance", cleanup: cohortPilot.cleanup, attempts: cohortAttempts, admittedAttemptIds: cohortAttempts.filter((attempt) => !corroboratingCohortAttemptIds.has(attempt.attemptId)).map((attempt) => attempt.attemptId), corroboratingAttemptIds: [...corroboratingCohortAttemptIds], promptLifecycle: cohortAdmissions.map(({ attempt, promptLifecycle }) => ({ attemptId: attempt.attemptId, promptLifecycle })) },
    originalPilot: { path: pilotReceiptPath, execution: pilot.execution, failedRows: Array.isArray(pilot.rows) ? pilot.rows.filter((row) => object(row)?.status === "failed") : [], unfinishedRows: pilot.unfinished_rows },
    routeEvidence: { index: evidenceIndexPath, indexHash: sha256(await readFile(evidenceIndexPath, "utf8")), evidencedRoutes: ["generic-stdio", "generic-http", "generic-stateless", "codex-appserver", "pi-rpc-cli"] },
    report,
    counts: { status: statusCounts, outcome: outcomeCounts, total: report.total.outcome },
  };
  const dir = join(artifactRoot, run);
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "checkpoint.json"), `${JSON.stringify(output, null, 2)}\n`, "utf8");
  const markdown = [
    `# Offline qualification checkpoint ${run}`, "", `- Contract: ${frozenRoutes.length} routes, ${frozenPairMatrix.length} ordered pairs, ${frozenDimensionObligations.length} dimension rows (${frozenDimensionObligations.filter((row) => row.required).length} required).`,
    `- Retained generic re-evaluation: ${rows.length}/${rows.length} exact exchanges meet unchanged collector/oracle checks.`, `- Row status counts: ${JSON.stringify(statusCounts)}.`, `- Outcome counts: ${JSON.stringify(outcomeCounts)}.`, `- Overall verdict: **${report.total.outcome}**; required rows remain unresolved.`,
    `- Provenance: source revision ${sourceRevision}, oracle ${oracleRevision}, history ${historyHash}.`, "- No native/model/Redis calls were made.",
  ].join("\n");
  await writeFile(join(dir, "checkpoint.md"), `${markdown}\n`, "utf8");
  expect(output.contract.pairCount).toBe(625);
  expect(output.genericReevaluation).toEqual({ receiptRows: 9, exactOracleMeets: 9 });
  const pairIds = new Set<string>(frozenPairMatrix.map((pair) => pair.pairId));
  const communicationRows = report.rows.filter((row) => pairIds.has(row.id));
  expect(Object.fromEntries(["completed", "unrun", "setup_gap", "blocked"].map((status) => [status, communicationRows.filter((row) => row.status === status).length]))).toEqual({ completed: 17, unrun: 8, setup_gap: 459, blocked: 141 });
  expect(output.additionalCommunication.admitted).toHaveLength(5);
  expect(output.additionalCommunication.failedOriginals).toHaveLength(2);
  expect(output.cohortPilot.attempts).toHaveLength(8);
  expect(output.cohortPilot.admittedAttemptIds).toHaveLength(3);
  expect(output.cohortPilot.corroboratingAttemptIds).toHaveLength(5);
  expect(report.total.outcome).toBe("uncertain");
};

describe.skipIf(!enabled)("offline fresh qualification checkpoint", () => {
  it("materializes the frozen ledger without claiming unavailable behavior", async () => { await runCheckpoint(); }, 30_000);
});

describe.skipIf(!discoveryEnabled)("offline discovery-qualified qualification checkpoint", () => {
  it("admits only the explicit current generic/cohort receipts with bidirectional discovery", async () => { await runDiscoveryCheckpoint(); }, 30_000);
});
