import { createHash, randomUUID, randomInt } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { Redis } from "ioredis";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { createGenericAdapters } from "./qualification-generic.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { extractNativeTraces, extractGenericTraces } from "./qualification-evidence.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import { assertIdleObservation, assertSetupTurnCompleted } from "./qualification-idle-task.js";
import { collectIdleReplyContinuation } from "./qualification-idle-reply.js";
import { sanitizeEvidence } from "./public-evidence.js";
import type { GenericCallRecord, GenericParticipant } from "./qualification-types.js";
import { CODEX_BIN } from "./local-tools.js";

const resultEnabled = process.env.GPTQUEUE_QUALIFICATION_IDLE_RESULT === "1";
const errorEnabled = process.env.GPTQUEUE_QUALIFICATION_IDLE_ERROR === "1";
const enabled = resultEnabled || errorEnabled;
const bothEnabled = resultEnabled && errorEnabled;
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-idle-reply-native");
const sourceLabels = ["tests/acceptance/qualification-idle-reply-native.test.ts", "tests/acceptance/qualification-idle-reply.ts", "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-generic.ts", "tests/acceptance/qualification-evidence.ts", "tests/acceptance/qualification-discovery.ts", "tests/acceptance/qualification-idle-task.ts", "tests/acceptance/owned-redis.ts", "bin/gptqueue-session", "dist/mcp-server/index.js", "package.json", "package-lock.json"] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });
type Json = Record<string, unknown>;
const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const hash = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const redact = (value: unknown): unknown => sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true });
const ids = (history: unknown): readonly string[] => {
  const turns = object(history)?.turns;
  if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.map(turn => { const id = object(turn)?.id; if (typeof id !== "string" || id.length === 0) throw new Error("native turn has no exact ID"); return id; });
};
const wait = (ms: number) => new Promise<void>(resolveDelay => setTimeout(resolveDelay, ms));
const waitIdle = async (model: CodexParticipant, signal: AbortSignal): Promise<Json> => {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) { const status = await model.status(signal); if (status.kind === "idle" && status.runtimeId === model.identity.hostRuntimeId) return status as Json; if (status.kind === "terminated" || status.kind === "unknown") throw new Error(`native model not observable: ${JSON.stringify(status)}`); await wait(250); }
  throw new Error("native model did not become idle");
};
const callsAfter = (history: unknown, baseline: readonly string[]): readonly string[] => {
  const turns = object(history)?.turns;
  return Array.isArray(turns) ? turns.flatMap(turn => baseline.includes(String(object(turn)?.id)) ? [] : (Array.isArray(object(turn)?.items) ? object(turn)!.items as unknown[] : [])).flatMap(item => object(item)?.type === "mcpToolCall" && typeof object(item)?.id === "string" ? [String(object(item)!.id)] : []) : [];
};
const traceRows = (rows: readonly [string, string[]][]): readonly Json[] => rows.map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[i * 2]!, fields[i * 2 + 1]!])));
const digestFile = async (path: string): Promise<string> => hash(await readFile(path));
const sourceHashes = async (): Promise<Json> => {
  const trees = await Promise.all(["src", "dist", "tests/acceptance"].map(async prefix => (await readdir(join(repo, prefix), { recursive: true })).filter(path => /\.(?:ts|js|json)$/u.test(path)).map(path => `${prefix}/${path}`)));
  const labels = [...new Set([...sourceLabels, ...trees.flat()])].sort();
  const files: Record<string, string> = Object.fromEntries(labels.map(file => [file, join(repo, file)]));
  files.node_executable = process.execPath; files.codex_executable = process.env.CODEX_BIN ?? CODEX_BIN;
  return Object.fromEntries(await Promise.all(Object.entries(files).map(async ([label, path]) => [label, await digestFile(path)])));
};

describe("native Codex idle correlated reply qualification", () => {
  it("requires exactly one native reply qualification gate", () => expect(Number(resultEnabled) + Number(errorEnabled)).toBeLessThanOrEqual(1));
  it.skipIf(enabled)("keeps the result and error trials opt-in", () => expect(process.env.GPTQUEUE_QUALIFICATION_IDLE_RESULT ?? process.env.GPTQUEUE_QUALIFICATION_IDLE_ERROR).not.toBe("1"));

  it.skipIf(!enabled || bothEnabled)(`continues the outstanding native task on a peer ${resultEnabled ? "result" : "error"}`, async () => {
    const replyType = resultEnabled ? "result" as const : "error" as const;
    const runId = randomUUID(), artifactDir = join(artifactRoot, runId), phasesDir = join(artifactDir, "phases");
    await mkdir(phasesDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, route: "codex-appserver", model: "gpt-5.6-luna", reply_type: replyType, database: 15, passed: false, phase: "running", phases: [] };
    const persist = async (label: string, value: unknown): Promise<void> => { const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true }) }; (receipt.phases as Json[]).push(phase); await writeFile(join(phasesDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 }); await writeFile(join(artifactDir, "receipt.json"), `${JSON.stringify(sanitizeEvidence(receipt), null, 2)}\n`, { mode: 0o600 }); };
    let redis: OwnedRedis | undefined, redisTrace: Redis | undefined, workspace: string | undefined;
    let codex: ReturnType<typeof createCodexAdapters> | undefined, generic: ReturnType<typeof createGenericAdapters> | undefined;
    let model: CodexParticipant | undefined, peer: GenericParticipant | undefined;
    let failure: unknown;
    try {
      receipt.runner_pid = process.pid; receipt.started_at = new Date().toISOString(); receipt.source_hashes_before = await sourceHashes(); const sourceRevision = hash(Buffer.from(JSON.stringify(receipt.source_hashes_before)));
      receipt.row_id = `codex-appserver:automatic:idle-${replyType}`; receipt.constraints = "automatic/private-MCP-only; not initiative or installation-wide activation"; console.info(`[qualification-idle-reply] receipt=${join(artifactDir, "receipt.json")} run_id=${runId} runner_pid=${process.pid}`); await persist("started", { pid: process.pid });
      redis = await startOwnedRedis(); redisTrace = new Redis(redis.url); workspace = await mkdtemp(join(tmpdir(), "gptqueue-idle-reply-native-")); receipt.redis = { port: Number(new URL(redis.url).port), database: 15, owned_process: true };
      codex = createCodexAdapters({ workspaceRoot: workspace, codexBin: process.env.CODEX_BIN ?? CODEX_BIN, model: "gpt-5.6-luna" }); generic = createGenericAdapters({ repo });
      const nativeAdapter = codex.adapters.find(({ spec }) => spec.id === "codex-appserver"), genericAdapter = generic.adapters.find(({ spec }) => spec.id === "generic-stdio");
      if (!nativeAdapter || !genericAdapter) throw new Error("required adapters unavailable");
      const signal = AbortSignal.timeout(420_000);
      if ((await nativeAdapter.preflight(signal)).kind !== "available" || (await genericAdapter.preflight(signal)).kind !== "available") throw new Error("required adapter unavailable");
      peer = await genericAdapter.launch({ role: "sender", pairId: runId, nonce: `peer-${runId}`, redisUrl: redis.url }, signal) as GenericParticipant;
      model = await nativeAdapter.launch({ role: "receiver", pairId: runId, nonce: `model-${runId}`, redisUrl: redis.url }, signal) as CodexParticipant;
      receipt.identities = { model: model.identity, peer: peer.identity }; receipt.model_provenance = model.provenance; receipt.executable_paths = { node: process.execPath, codex: process.env.CODEX_BIN ?? CODEX_BIN }; await persist("launched", receipt.identities);
      const controllerEvents: Array<{ kind: string; at: number; sequence: number; prompt?: string }> = []; let eventSequence = 0;
      const controllerPrompt = async (prompt: string): Promise<unknown> => { const event = { kind: "controller_prompt", at: Date.now(), sequence: ++eventSequence, prompt }; controllerEvents.push(event); await persist("controller-prompt", event); return model!.prompt(prompt, signal); };
      await peer.call("list_agents", {}, signal);
      const setup = await controllerPrompt("Use only gptqueue-shared. Call get_runtime_status and list_agents, then reply READY and end this turn.");
      const baselineModel = await model.history(signal), baselinePeer = await peer.history(signal), baselineTurns = ids(baselineModel);
      await persist("baseline", { model: baselineModel, peer: baselinePeer, setup });
      const nativeRef = { path: join(artifactDir, "baseline-model-history.json"), sha256: "", sourceRevision, oracleRevision: await digestFile(join(repo, "tests/acceptance/qualification-idle-reply.ts")) };
      const peerRef = { path: join(artifactDir, "baseline-peer-history.json"), sha256: "", sourceRevision, oracleRevision: await digestFile(join(repo, "tests/acceptance/qualification-idle-reply.ts")) };
      await writeFile(nativeRef.path, `${JSON.stringify(redact(baselineModel))}\n`, { mode: 0o600 }); await writeFile(peerRef.path, `${JSON.stringify(redact(baselinePeer))}\n`, { mode: 0o600 });
      const native = extractNativeTraces(JSON.parse(await readFile(nativeRef.path, "utf8")), model.identity, { ...nativeRef, sha256: await digestFile(nativeRef.path) });
      const genericBaseline = extractGenericTraces(JSON.parse(await readFile(peerRef.path, "utf8")), peer.identity, { ...peerRef, sha256: await digestFile(peerRef.path) });
      assertSetupTurnCompleted(baselineModel, setup); const binding = native.find(trace => trace.name === "get_runtime_status" && trace.successful && trace.runtimeBound && trace.output?.activation_ready === true && object(trace.output?.runtime)?.runtime_id === model!.identity.hostRuntimeId && typeof object(trace.output?.runtime)?.working_directory === "string" && createHash("sha256").update(resolve(String(object(trace.output?.runtime)?.working_directory))).digest("hex") === model!.identity.cwdHash); if (!binding) throw new Error("baseline lacks activation-ready native binding");
      const discovery = { model_to_peer: collectPeerDiscovery(model.identity, peer.identity, native), peer_to_model: collectPeerDiscovery(peer.identity, model.identity, genericBaseline) }; const baselineStatus = await waitIdle(model, signal);
      const nonce = `idle-reply-${randomUUID()}`;
      const plannedRequestContent = `${nonce}: choose the hidden data after this turn completes and reply correlated.`;
      const prompt = replyType === "result" ? `Use only gptqueue-shared. Send exactly one task to the generic peer discovered by list_agents. Tell it to choose a fresh hidden integer after this turn completes, then send the integer as a correlated result. When the result arrives, output exactly DOUBLE:<twice the integer>. Do not poll for the result.` : `Use only gptqueue-shared. Send exactly one task to the generic peer discovered by list_agents. Tell it to choose a fresh hidden error code after this turn completes, then send a correlated error. If the code starts TEMP_, output exactly RETRY:<exact code>; if it starts PERM_, output exactly STOP:<exact code>. Do not poll for the error.`;
      const promptText = `${prompt} Send this exact task content: "${plannedRequestContent}" End this turn immediately after sending; do not poll.`;
      const origin = await controllerPrompt(promptText), originStatus = await waitIdle(model, signal), afterOrigin = await model.history(signal);
      assertSetupTurnCompleted(afterOrigin, origin);
      const originBytes = Buffer.from(`${JSON.stringify(redact(afterOrigin))}\n`), originPath = join(artifactDir, "origin-model-history.json");
      await writeFile(originPath, originBytes, { mode: 0o600 });
      const originHistory = JSON.parse(await readFile(originPath, "utf8")), preStimulusTurns = ids(originHistory), boundary = Date.now();
      controllerEvents.push({ kind: "stimulus_boundary", at: boundary, sequence: ++eventSequence }); receipt.boundary = boundary;
      const taskCalls = callsAfter(originHistory, baselineTurns);
      await persist("origin-complete", { origin, originStatus, baselineStatus, prompt: promptText, boundary, controllerEvents, taskCalls, history: originHistory });
      const originatingTurnId = object(origin)?.turn && typeof object(object(origin)?.turn)?.id === "string" ? String(object(object(origin)?.turn)!.id) : "";
      const originTurns = object(originHistory)?.turns;
      const originTurn = Array.isArray(originTurns) ? originTurns.map(object).find(turn => turn?.id === originatingTurnId) : undefined;
      const originItems = new Set((Array.isArray(originTurn?.items) ? originTurn.items : []).map(object).filter(item => item?.type === "mcpToolCall").map(item => String(item!.id)));
      const taskCall = extractNativeTraces(originHistory, model.identity, { path: originPath, sha256: hash(originBytes), sourceRevision, oracleRevision: nativeRef.oracleRevision }).find(trace => originItems.has(trace.sourceId) && trace.name === "send_message" && trace.successful === true && trace.runtimeBound === true && trace.input?.type === "task" && trace.input?.to === peer!.identity.agent && trace.output?.status === "sent");
      if (!taskCall || typeof taskCall.output?.message_id !== "string") throw new Error("originating native task send was not observed");
      if (taskCall.input?.content !== plannedRequestContent) throw new Error("native originating task content did not match exact planned request");
      const received = await peer.call("receive_message", { timeout: 30 }, signal) as Json; const requestId = String(taskCall.output.message_id); if (received.status !== "message" || object(received.message)?.id !== requestId) throw new Error("generic peer did not receive exact originating task");
      const hiddenGeneratedAt = new Date().toISOString(); const hidden = replyType === "result" ? String(randomInt(1000, 9000)) : `${randomInt(0, 2) === 0 ? "TEMP" : "PERM"}_${randomInt(10000, 99999)}`; const expected = replyType === "result" ? `DOUBLE:${Number(hidden) * 2}` : `${hidden.startsWith("TEMP_") ? "RETRY" : "STOP"}:${hidden}`;
      const replyContent = replyType === "result" ? hidden : `service failed with ${hidden}`; const peerSent = await peer.call("send_message", { to: model.identity.agent, type: replyType, in_reply_to: requestId, content: replyContent, idempotency_key: `${nonce}:reply` }, signal); const peerEvent = { kind: "peer_send", at: Date.now(), sequence: ++eventSequence }; controllerEvents.push(peerEvent); const activationRows = traceRows(await redisTrace.xrange(`gptq:inbox-trace:${model.identity.agent}`, "-", "+")); await persist("peer-reply", { received, hidden, hidden_generated_at: hiddenGeneratedAt, peerSent, request_id: requestId, activation_rows: activationRows, controller_events: controllerEvents });
      if (object(peerSent)?.status !== "sent" || typeof object(peerSent)?.message_id !== "string") throw new Error("peer reply send failed");
      receipt.plan = { nonce, requestContent: plannedRequestContent, hidden, hiddenGeneratedAt, replyContent, expected };
      const deadline = Date.now() + 180_000; let proof: unknown; let lastError = "";
      while (Date.now() < deadline) {
        const history = await model.history(signal), path = join(artifactDir, `native-history-${Date.now()}.json`);
        await writeFile(path, `${JSON.stringify(redact(history))}\n`, { mode: 0o600 });
        const peerPath = join(artifactDir, `peer-history-${Date.now()}.json`);
        await writeFile(peerPath, `${JSON.stringify(redact(await peer.history(signal)))}\n`, { mode: 0o600 });
        const bytes = await readFile(path), peerBytes = await readFile(peerPath);
        const parsed = JSON.parse(bytes.toString()), finalPeer = JSON.parse(peerBytes.toString()) as GenericCallRecord[];
        const modelRef = { path, sha256: hash(bytes), sourceRevision, oracleRevision: nativeRef.oracleRevision };
        const finalPeerRef = { path: peerPath, sha256: hash(peerBytes), sourceRevision, oracleRevision: nativeRef.oracleRevision };
        const rows = traceRows(await redisTrace.xrange(`gptq:inbox-trace:${model.identity.agent}`, "-", "+"));
        await persist("observation", { modelRef, peerRef: finalPeerRef, activation_rows: rows });
        try {
          assertIdleObservation({ boundary, nativeTurnIds: ids(parsed), controllerEvents }, preStimulusTurns);
          const nativeTraces = extractNativeTraces(parsed, model.identity, modelRef), genericTraces = extractGenericTraces(finalPeer, peer.identity, finalPeerRef);
          const collected = collectIdleReplyContinuation({ replyType, model: model.identity, peer: peer.identity, baselineTurnIds: baselineTurns, originatingTurnId, nativeHistory: parsed, genericTraces, nativeTraces, requestContent: plannedRequestContent, replyContent, continuation: expected, nonce });
          const requested = rows.find(row => row.stage === "activation_requested" && row.message_id === collected.replyId && row.runtime_id === model!.identity.hostRuntimeId && typeof row.operation_id === "string" && row.operation_id.length > 0);
          const started = requested && rows.find(row => row.stage === "turn_started" && row.runtime_id === model!.identity.hostRuntimeId && row.operation_id === requested.operation_id && row.turn_id === collected.continuationTurnId);
          const turns = object(parsed)?.turns, turn = Array.isArray(turns) ? turns.map(object).find(value => value?.id === collected.continuationTurnId) : undefined;
          const user = Array.isArray(turn?.items) && requested ? turn.items.map(object).find(item => (item?.type === "userMessage" || item?.type === "UserMessage") && (item.clientId === requested.operation_id || item.client_id === requested.operation_id)) : undefined;
          if (!requested || !started || !user) throw new Error("activation trace/user item did not join exact continuation");
          proof = { collected, activation: { requested, started, user }, final_history_ref: modelRef, final_peer_history_ref: finalPeerRef, controller_events: controllerEvents };
          break;
        } catch (error) { lastError = String(error); receipt.last_collector_error = lastError; await wait(500); }
      }
      if (!proof) throw new Error(`idle reply continuation absent: ${lastError}`); receipt.proof = proof; receipt.baseline = { binding, discovery, baselineTurns, preStimulusTurns, originTurnId: originatingTurnId }; receipt.controller_events = controllerEvents; receipt.passed = true; receipt.phase = "completed"; await persist("proof", proof);
    } catch (error) { failure = error; receipt.error = String(error); receipt.phase = "failed"; await persist("failure", String(error)); }
    finally {
      const cleanup: Json[] = [];
      if (model) { try { await persist("failure-model-history", await model.history(AbortSignal.timeout(10_000))); } catch (error) { cleanup.push({ name: "failure-model-history", status: "rejected", error: String(error) }); failure ??= error; } }
      if (peer) { try { await persist("failure-peer-history", await peer.history(AbortSignal.timeout(10_000))); } catch (error) { cleanup.push({ name: "failure-peer-history", status: "rejected", error: String(error) }); failure ??= error; } }
      for (const [name, action] of [["model", () => model?.close()], ["peer", () => peer?.close()], ["codex", () => codex?.close()], ["generic", () => generic?.close()], ["redis-trace", async () => redisTrace?.quit()], ["redis", () => redis?.close()], ["workspace", async () => workspace && rm(workspace, { recursive: true, force: true })] ] as const) {
        try { await action(); cleanup.push({ name, status: "fulfilled" }); } catch (error) { cleanup.push({ name, status: "rejected", error: String(error) }); failure ??= new Error(`${name} cleanup failed: ${String(error)}`); }
      }
      receipt.cleanup = cleanup; if (cleanup.some(item => item.status === "rejected")) { receipt.passed = false; failure ??= new Error("native qualification cleanup failed"); } receipt.source_hashes_after = await sourceHashes(); if (JSON.stringify(receipt.source_hashes_before) !== JSON.stringify(receipt.source_hashes_after)) { failure ??= new Error("source hash drift during native qualification"); receipt.passed = false; }
      receipt.execution = failure ? { status: "failed", detail: String(failure) } : { status: "completed" };
      receipt.phase = failure ? "failed" : "completed";
      receipt.ended_at = new Date().toISOString(); await persist("cleanup", { cleanup, source_hashes_after: receipt.source_hashes_after });
    }
    if (failure) throw failure; expect(receipt.passed).toBe(true);
  }, 450_000);
});
