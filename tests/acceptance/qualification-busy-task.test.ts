import { createHash, randomInt, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { Redis } from "ioredis";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { createGenericAdapters } from "./qualification-generic.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import { extractNativeTraces, extractGenericTraces } from "./qualification-evidence.js";
import { collectIdleClaimExchange } from "./qualification-idle-claim.js";
import { checkExchangeEvidence } from "./oracle.js";
import { sanitizeEvidence } from "./public-evidence.js";
import { assertSetupTurnCompleted } from "./qualification-idle-task.js";
import { assertBusyDeliveryObservation, type BusyControllerEvent, type BusyPromptAcceptance } from "./qualification-busy-task.js";
import type { GenericParticipant, RawEvidenceRef } from "./qualification-types.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_BUSY_TASK === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-busy-task");
const sourceLabels = [
  "tests/acceptance/qualification-busy-task.test.ts", "tests/acceptance/qualification-idle-claim.ts",
  "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-generic.ts",
  "tests/acceptance/qualification-evidence.ts", "tests/acceptance/qualification-discovery.ts",
  "tests/acceptance/qualification-idle-task.ts", "tests/acceptance/oracle.ts", "tests/acceptance/owned-redis.ts",
  "bin/gptqueue-session", "dist/mcp-server/index.js", "package.json", "package-lock.json",
] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });
type Json = Record<string, unknown>;
type Cleanup = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;
const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const bytesHash = (value: Uint8Array): string => createHash("sha256").update(value).digest("hex");
const fileHash = async (path: string): Promise<string> => bytesHash(await readFile(path));
const redact = (value: unknown): unknown => sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true });
const wait = (ms: number): Promise<void> => new Promise(resolveDelay => setTimeout(resolveDelay, ms));
const ids = (history: unknown): readonly string[] => {
  const turns = object(history)?.turns;
  if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.map(turn => { const id = object(turn)?.id; if (typeof id !== "string" || id.length === 0) throw new Error("native turn has no exact ID"); return id; });
};
const traceRows = (rows: readonly [string, string[]][]): readonly Json[] => rows.map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[i * 2]!, fields[i * 2 + 1]!]))) as Json[];
const callsInTurn = (history: unknown, turnId: string): readonly string[] => {
  const turn = turnById(history, turnId);
  return Array.isArray(turn?.items) ? (turn.items as unknown[]).flatMap(item => object(item)?.type === "mcpToolCall" && typeof object(item)?.id === "string" ? [String(object(item)!.id)] : []) : [];
};
const sourceHashes = async (): Promise<Json> => {
  const trees = await Promise.all(["src", "dist", "tests/acceptance"].map(async prefix =>
    (await readdir(join(repo, prefix), { recursive: true })).filter(path => /\.(?:ts|js|json)$/u.test(path)).map(path => `${prefix}/${path}`)));
  const labels = [...new Set([...sourceLabels, ...trees.flat()])].sort();
  const files: Record<string, string> = Object.fromEntries(labels.map(file => [file, join(repo, file)]));
  files.node_executable = process.execPath; files.codex_executable = process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex";
  return Object.fromEntries(await Promise.all(Object.entries(files).map(async ([label, path]) => [label, await fileHash(path)])));
};
const evidenceRef = (path: string, sha256: string, source: Json, oracleSha: string): RawEvidenceRef => ({
  path, sha256, sourceRevision: bytesHash(Buffer.from(JSON.stringify(source))), oracleRevision: oracleSha,
});
const turnById = (history: unknown, id: string): Json | undefined => {
  const turns = object(history)?.turns;
  return Array.isArray(turns) ? turns.map(object).find(turn => turn?.id === id) : undefined;
};
const turnIsActive = (turn: Json | undefined): boolean => ["inprogress", "in_progress", "running", "started"].includes(String(turn?.status ?? "").toLowerCase());
const waitIdle = async (model: CodexParticipant, signal: AbortSignal): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) { const status = await model.status(signal); if (status.kind === "idle" && status.runtimeId === model.identity.hostRuntimeId) return; if (status.kind === "terminated" || status.kind === "unknown") throw new Error(`model not observable: ${JSON.stringify(status)}`); await wait(250); }
  throw new Error("model did not become idle");
};

describe("Codex appserver busy-task qualification", () => {
  it("keeps the native trial explicitly gated", () => { if (!enabled) expect(process.env.GPTQUEUE_QUALIFICATION_BUSY_TASK).not.toBe("1"); });

  it.skipIf(!enabled)("delivers a generic task during an actual native in-progress turn", async () => {
    const runId = randomUUID(), artifactDir = join(artifactRoot, runId), phasesDir = join(artifactDir, "phases");
    await mkdir(phasesDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, runner_pid: process.pid, gate: "GPTQUEUE_QUALIFICATION_BUSY_TASK=1", route: "codex-appserver", model: "gpt-5.6-luna", passed: false, phase: "running", phases: [], cleanup: [], source_hashes_before: {}, source_hashes_after: {} };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: redact(value) }; (receipt.phases as Json[]).push(phase);
      await writeFile(join(phasesDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt.json"), `${JSON.stringify(redact(receipt), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined, observer: Redis | undefined, workspace: string | undefined;
    let codex: ReturnType<typeof createCodexAdapters> | undefined, generic: ReturnType<typeof createGenericAdapters> | undefined;
    let model: CodexParticipant | undefined, peer: GenericParticipant | undefined, failure: unknown;
    let busyPromise: Promise<unknown> | undefined;
    try {
      receipt.started_at = new Date().toISOString(); receipt.source_hashes_before = await sourceHashes();
      receipt.row_id = "codex-appserver:automatic:busy-deferral";
      receipt.constraints = "One owned-profile native busy-deferral observation; not initiative or installation-wide activation.";
      console.info(`[qualification-busy-task] receipt=${join(artifactDir, "receipt.json")} run_id=${runId} runner_pid=${process.pid}`);
      const source = receipt.source_hashes_before as Json, oracleSha = await fileHash(join(repo, "tests/acceptance/oracle.ts"));
      await persist("started", { runner_pid: process.pid, source_revision: bytesHash(Buffer.from(JSON.stringify(source))), oracle_sha256: oracleSha });
      redis = await startOwnedRedis(); observer = new Redis(redis.url); workspace = await mkdtemp(join(tmpdir(), "gptqueue-busy-task-"));
      receipt.redis = { port: Number(new URL(redis.url).port), database: 15, owned_process: true };
      codex = createCodexAdapters({ workspaceRoot: workspace, codexBin: process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex", model: "gpt-5.6-luna" }); generic = createGenericAdapters({ repo });
      const nativeAdapter = codex.adapters.find(({ spec }) => spec.id === "codex-appserver"), peerAdapter = generic.adapters.find(({ spec }) => spec.id === "generic-stdio");
      if (!nativeAdapter || !peerAdapter) throw new Error("required adapters unavailable");
      const signal = AbortSignal.timeout(420_000);
      if ((await nativeAdapter.preflight(signal)).kind !== "available" || (await peerAdapter.preflight(signal)).kind !== "available") throw new Error("required adapter unavailable");
      peer = await peerAdapter.launch({ role: "sender", pairId: runId, nonce: `peer-${runId}`, redisUrl: redis.url }, signal) as GenericParticipant;
      model = await nativeAdapter.launch({ role: "receiver", pairId: runId, nonce: `model-${runId}`, redisUrl: redis.url }, signal) as CodexParticipant;
      receipt.identities = { peer: peer.identity, model: model.identity }; receipt.provenance = model.provenance; receipt.executable_paths = { node: process.execPath, codex: process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex" }; await persist("launched", receipt.identities);
      receipt.row_id = "codex-appserver:automatic:busy-deferral"; receipt.console_locator = `receipt=${join(artifactDir, "receipt.json")} runner_pid=${process.pid}`;
      const controllerEvents: Array<BusyControllerEvent & Json> = [{ kind: "controller_prompt", at: Date.now(), prompt: "Call get_runtime_status and list_agents, then reply READY and end this turn." }]; receipt.controller_ledger = controllerEvents;
      await peer.call("list_agents", {}, signal);
      const setupResult = await model.prompt("Call get_runtime_status and list_agents, then reply READY and end this turn.", signal);
      const baselineModel = await model.history(signal), baselinePeer = await peer.history(signal), baselineTurns = ids(baselineModel);
      const baselineModelPath = join(artifactDir, "baseline-model-history.json"), baselinePeerPath = join(artifactDir, "baseline-peer-history.json");
      await writeFile(baselineModelPath, `${JSON.stringify(redact(baselineModel))}\n`, { mode: 0o600 }); await writeFile(baselinePeerPath, `${JSON.stringify(redact(baselinePeer))}\n`, { mode: 0o600 });
      assertSetupTurnCompleted(baselineModel, setupResult);
      const nativeRef = evidenceRef(baselineModelPath, await fileHash(baselineModelPath), source, oracleSha), peerRef = evidenceRef(baselinePeerPath, await fileHash(baselinePeerPath), source, oracleSha);
      const nativeBaseline = extractNativeTraces(JSON.parse(await readFile(baselineModelPath, "utf8")), model.identity, nativeRef), genericBaseline = extractGenericTraces(JSON.parse(await readFile(baselinePeerPath, "utf8")), peer.identity, peerRef);
      const binding = nativeBaseline.find(trace => trace.name === "get_runtime_status" && trace.successful && trace.runtimeBound && trace.output?.activation_ready === true && object(trace.output?.runtime)?.runtime_id === model!.identity.hostRuntimeId && typeof object(trace.output?.runtime)?.working_directory === "string" && createHash("sha256").update(resolve(String(object(trace.output?.runtime)?.working_directory))).digest("hex") === model!.identity.cwdHash);
      if (!binding) throw new Error("baseline lacks activation-ready runtime/cwd binding");
      const discovery = { model_to_peer: collectPeerDiscovery(model.identity, peer.identity, nativeBaseline), peer_to_model: collectPeerDiscovery(peer.identity, model.identity, genericBaseline) };
      await waitIdle(model, signal); receipt.baseline = { binding, discovery, turn_ids: baselineTurns, idle: true }; await persist("baseline-ready", receipt.baseline);

      const workProduct = 12345 * 67890, workPrompt = "Compute 12345*67890 independently. During this calculation turn, do not poll or coordinate with peers. When finished, reply exactly BUSY-DONE:<decimal product> and end this turn.";
      controllerEvents.push({ kind: "controller_prompt", at: Date.now(), prompt: workPrompt }, { kind: "busy_prompt_started", at: Date.now() });
      busyPromise = model.prompt(workPrompt, signal); void busyPromise.catch(error => { receipt.busy_prompt_rejection = String(error); });
      let busyTurnId = "", busyBefore: unknown, busyObservedAt = 0;
      const busyDeadline = Date.now() + 30_000;
      while (Date.now() < busyDeadline) {
        const history = await model.history(signal), active = (object(history)?.turns as unknown[] | undefined)?.map(object).find(turn => !baselineTurns.includes(String(turn?.id)) && turnIsActive(turn));
        if (active?.id) { busyTurnId = String(active.id); busyBefore = history; busyObservedAt = Date.now(); break; }
        await wait(100);
      }
      if (!busyTurnId) throw new Error("no actual native in-progress turn observed before task boundary");
      const nonce = `busy-${randomUUID()}`, left = randomInt(1000, 9999), right = randomInt(1000, 9999), expectedReplyContent = `answer ${nonce}: ${left * right}`;
      const requestContent = `Task ${nonce}: calculate ${left} * ${right}. Independently compute the answer and send a correlated result with content "answer ${nonce}: <decimal product>".`;
      busyBefore = await model.history(signal);
      const busyBeforeBytes = Buffer.from(`${JSON.stringify(redact(busyBefore))}\n`), busyBeforePath = join(artifactDir, "busy-before-history.json"); await writeFile(busyBeforePath, busyBeforeBytes, { mode: 0o600 });
      if (!turnIsActive(turnById(busyBefore, busyTurnId))) throw new Error("native busy turn completed before the peer-send bracket began");
      const beforeObservedAt = Date.now(), beforeBusyIds = ids(busyBefore); const boundary = beforeObservedAt; controllerEvents.push({ kind: "stimulus_boundary", at: boundary, busy_turn_id: busyTurnId, busy_observed_at: beforeObservedAt }, { kind: "generic_send_started", at: boundary });
      const sent = await peer.call("send_message", { to: model.identity.agent, type: "task", content: requestContent, idempotency_key: `${nonce}:task` }, signal); const peerSentAt = Date.now();
      const sentResult = object(sent);
      if (sentResult?.status !== "sent" || typeof sentResult.message_id !== "string" || sentResult.message_id.length === 0) throw new Error("generic task send failed");
      const sentMessageId: string = sentResult.message_id;
      controllerEvents.push({ kind: "generic_send", at: peerSentAt, message_id: sentMessageId });
      const afterSend = await model.history(signal), afterObservedAt = Date.now(), afterStatus = await model.status(signal), afterBusyTurn = turnById(afterSend, busyTurnId);
      if (!turnIsActive(afterBusyTurn) || afterStatus.kind !== "busy") throw new Error("task was not bracketed inside the same live native busy turn");
      const busyAfterBytes = Buffer.from(`${JSON.stringify(redact(afterSend))}\n`), busyAfterPath = join(artifactDir, "busy-after-send-history.json"); await writeFile(busyAfterPath, busyAfterBytes, { mode: 0o600 });
      receipt.plan = { nonce, left, right, requestContent, expectedReplyContent, independent_expected_product: left * right, busy_turn_id: busyTurnId, boundary, peer_sent_at: peerSentAt, busy_before_observed_at: beforeObservedAt, busy_after_observed_at: afterObservedAt, busy_before_history: evidenceRef(busyBeforePath, bytesHash(busyBeforeBytes), source, oracleSha), busy_after_history: evidenceRef(busyAfterPath, bytesHash(busyAfterBytes), source, oracleSha), busy_before_turn_ids: beforeBusyIds, busy_after_status: afterStatus }; await persist("busy-bracket", receipt.plan);
      const promptResult = await busyPromise; const acceptedId = String(object(promptResult)?.turn ? object(object(promptResult)?.turn)?.id : "");
      receipt.busy_prompt_result = promptResult; receipt.busy_work = { left: 12345, right: 67890, expected_final: `BUSY-DONE:${workProduct}` };
      if (acceptedId !== busyTurnId) throw new Error(`accepted original prompt turn ${acceptedId} did not match observed busy turn ${busyTurnId}`);
      const finalDeadline = Date.now() + 180_000; let proof: Json | undefined, lastError = ""; let received = false;
      while (Date.now() < finalDeadline) {
        if (!received) { const response = await peer.call("receive_message", { timeout: 5 }, AbortSignal.any([signal, AbortSignal.timeout(10_000)])); received = object(response)?.status === "message"; await persist("generic-receive", response); }
        const history = await model.history(signal), peerHistory = await peer.history(signal), path = join(artifactDir, `native-history-${Date.now()}.json`), peerPath = join(artifactDir, `peer-history-${Date.now()}.json`);
        const nativeBytes = Buffer.from(`${JSON.stringify(redact(history))}\n`), peerBytes = Buffer.from(`${JSON.stringify(redact(peerHistory))}\n`); await writeFile(path, nativeBytes, { mode: 0o600 }); await writeFile(peerPath, peerBytes, { mode: 0o600 });
        const rows = traceRows(await observer!.xrange(`gptq:inbox-trace:${model.identity.agent}`, "-", "+")); await persist("observation", { model_history: path, peer_history: peerPath, raw_trace_rows: rows });
        const modelEvidence = evidenceRef(path, bytesHash(nativeBytes), source, oracleSha), peerEvidence = evidenceRef(peerPath, bytesHash(peerBytes), source, oracleSha);
        try {
          const traces = extractNativeTraces(JSON.parse(nativeBytes.toString()), model.identity, modelEvidence), genericTraces = extractGenericTraces(JSON.parse(peerBytes.toString()), peer.identity, peerEvidence, true);
          const requested = rows.find(row => row.stage === "activation_requested" && row.message_id === sentMessageId && row.runtime_id === model!.identity.hostRuntimeId && typeof row.operation_id === "string");
          const started = requested && rows.find(row => row.stage === "turn_started" && row.runtime_id === model!.identity.hostRuntimeId && row.operation_id === requested.operation_id && typeof row.turn_id === "string");
          const continuation = started ? String(started.turn_id) : "";
          const continuationTurn = continuation ? turnById(history, continuation) : undefined;
          const user = continuationTurn && Array.isArray(continuationTurn.items) && requested && (continuationTurn.items as unknown[]).map(object).find(item => (item?.type === "userMessage" || item?.type === "UserMessage") && (item.clientId === requested.operation_id || item.client_id === requested.operation_id));
          if (!requested || !started || !user || !continuation) throw new Error("activation_requested/turn_started/native user item did not join exact subsequent turn");
          const exchange = collectIdleClaimExchange({ peer: peer.identity, model: model.identity, genericTraces, nativeTraces: traces, postStimulusCallIds: callsInTurn(history, continuation), nonce, requestContent, expectedReplyContent });
          if (exchange.request?.id !== sentMessageId) throw new Error("exchange request ID does not match the exact sent message ID");
          const verdict = checkExchangeEvidence(exchange); if (verdict.outcome !== "meets") throw new Error(JSON.stringify(verdict));
          const finalObservedAt = Date.now();
          const busyObservation = { actor: model.identity, baselineTurnIds: baselineTurns, before: { observedAt: beforeObservedAt, runtimeId: model.identity.hostRuntimeId, history: JSON.parse(busyBeforeBytes.toString()) }, after: { observedAt: afterObservedAt, runtimeId: model.identity.hostRuntimeId, history: JSON.parse(busyAfterBytes.toString()) }, afterStatus, sendStartedAt: boundary, sendCompletedAt: peerSentAt, sentMessageId, controllerLedger: controllerEvents, acceptedPrompt: promptResult as BusyPromptAcceptance, final: { observedAt: finalObservedAt, runtimeId: model.identity.hostRuntimeId, history: JSON.parse(nativeBytes.toString()) }, laterActivationTurnId: continuation, expectedBusyFinalAnswer: `BUSY-DONE:${workProduct}` };
          const busyId = assertBusyDeliveryObservation({ ...busyObservation, sentMessageId });
          proof = { exchange, verdict, busy_turn_id: busyId, accepted_original_prompt_turn_id: acceptedId, continuation_turn_id: continuation, final_observed_at: finalObservedAt, activation: { requested, started, user }, model_history: modelEvidence, peer_history: peerEvidence, raw_trace_rows: rows, controller_events: controllerEvents }; break;
        } catch (error) { lastError = String(error); receipt.last_collector_error = lastError; await wait(500); }
      }
      if (!proof) throw new Error(`busy task proof absent: ${lastError}`);
      receipt.proof = proof; await waitIdle(model, signal); receipt.lifecycle = { status: "idle" }; receipt.passed = true; receipt.phase = "completed"; await persist("proof", proof);
    } catch (error) { failure = error; receipt.error = String(error); receipt.phase = "failed"; await persist("failure", String(error)); }
    finally {
      if (busyPromise) { await busyPromise.catch(error => { failure ??= error; receipt.busy_prompt_error = String(error); }); receipt.busy_prompt_settled = true; }
      if (model) await model.history(AbortSignal.timeout(10_000)).then(history => persist("failure-model-history", history)).catch(error => persist("failure-model-history-error", String(error)).catch(() => undefined));
      if (peer) await peer.history(AbortSignal.timeout(10_000)).then(history => persist("failure-peer-history", history)).catch(error => persist("failure-peer-history-error", String(error)).catch(() => undefined));
      const cleanup: Cleanup[] = [];
      for (const [name, action] of [["model", () => model?.close()], ["peer", () => peer?.close()], ["codex", () => codex?.close()], ["generic", () => generic?.close()], ["observer", () => observer?.quit()], ["redis", () => redis?.close()], ["workspace", () => workspace ? rm(workspace, { recursive: true, force: true }) : Promise.resolve()]] as const) { try { await action(); cleanup.push({ name, status: "fulfilled" }); } catch (error) { cleanup.push({ name, status: "rejected", error: String(error) }); failure ??= error; } }
      receipt.cleanup = cleanup; receipt.source_hashes_after = await sourceHashes().catch(() => ({})); if (JSON.stringify(receipt.source_hashes_before) !== JSON.stringify(receipt.source_hashes_after)) { failure ??= new Error("source hash drift during qualification"); } if (cleanup.some(item => item.status === "rejected")) { failure ??= new Error("qualification cleanup failed"); }
      if (failure) receipt.passed = false;
      receipt.execution = failure ? { status: "failed", detail: String(failure) } : { status: "completed" }; receipt.ended_at = new Date().toISOString(); await persist("cleanup", { cleanup, source_hashes_after: receipt.source_hashes_after });
    }
    if (failure) throw failure; expect(receipt.passed).toBe(true);
  }, 450_000);
});
