import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { createGenericAdapters } from "./qualification-generic.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import { extractNativeTraces, extractGenericTraces } from "./qualification-evidence.js";
import { checkExchangeEvidence } from "./oracle.js";
import { collectIdleClaimExchange } from "./qualification-idle-claim.js";
import type { GenericParticipant, ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";
import { assertIdleObservation, assertSetupTurnCompleted } from "./qualification-idle-task.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_IDLE_TASK === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-idle-task");
const sourceFiles = ["tests/acceptance/qualification-idle-task.test.ts", "tests/acceptance/qualification-idle-task.ts", "tests/acceptance/qualification-idle-claim.ts", "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-generic.ts", "tests/acceptance/qualification-evidence.ts", "tests/acceptance/qualification-discovery.ts", "tests/acceptance/oracle.ts", "tests/acceptance/owned-redis.ts", "bin/gptqueue-session", "dist/mcp-server/index.js"] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });
type Json = Record<string, unknown>;
type CleanupResult = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const hashes = async (): Promise<Json> => {
  const trees = await Promise.all(["src", "dist", "tests/acceptance"].map(async prefix => (await readdir(join(repo, prefix), { recursive: true })).filter(path => /\.(?:ts|js|json)$/u.test(path)).map(path => `${prefix}/${path}`)));
  const labels = [...new Set([...sourceFiles, ...trees.flat(), "package.json", "package-lock.json"])].sort();
  const files: Record<string, string> = Object.fromEntries(labels.map(file => [file, join(repo, file)]));
  files.node_executable = process.execPath;
  files.codex_executable = process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex";
  return Object.fromEntries(await Promise.all(Object.entries(files).map(async ([label, path]) => [label, await digest(path)])));
};
const ref = (path: string, sha256: string, sources: Json = {}): RawEvidenceRef => ({ path, sha256, sourceRevision: createHash("sha256").update(JSON.stringify(sources)).digest("hex"), oracleRevision: String(sources["tests/acceptance/oracle.ts"] ?? "unit-fixture") });
const ids = (history: unknown): readonly string[] => {
  const turns = (history as Json)?.turns;
  if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.map(turn => {
    const id = (turn as Json)?.id;
    if (typeof id !== "string" || id.length === 0) throw new Error("native turn has no exact ID");
    return id;
  });
};
const wait = (ms: number): Promise<void> => new Promise(resolveDelay => setTimeout(resolveDelay, ms));
const redact = (value: unknown): unknown => sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true });
const waitIdle = async (model: CodexParticipant, signal: AbortSignal): Promise<void> => {
  const end = Date.now() + 15_000;
  while (Date.now() < end) { const status = await model.status(signal); if (status.kind === "idle") return; if (status.kind === "terminated" || status.kind === "unknown") throw new Error(`model is not observable idle: ${JSON.stringify(status)}`); await wait(250); }
  throw new Error("model did not become idle within 15s");
};
const postTurnCallIds = (history: unknown, baseline: readonly string[]): readonly string[] => {
  const turns = (history as Json)?.turns;
  return Array.isArray(turns) ? turns.flatMap(turn => {
    const row = turn as Json;
    return baseline.includes(String(row.id)) || !Array.isArray(row.items) ? [] : row.items.flatMap(item => (item as Json)?.type === "mcpToolCall" && typeof (item as Json).id === "string" ? [String((item as Json).id)] : []);
  }) : [];
};

describe("idle-task qualification fixture", () => {
  it("does not confuse an interrupted setup turn with an earlier completed turn", () => {
    const history = { turns: [{ id: "old", status: "completed" }, { id: "setup", status: "interrupted" }] };
    expect(() => assertSetupTurnCompleted(history, { turn: { id: "setup" } })).toThrow(/setup.*interrupted/);
    expect(assertSetupTurnCompleted({ turns: [{ id: "setup", status: "completed" }] }, { turn: { id: "setup" } })).toBe("setup");
    expect(() => assertSetupTurnCompleted(history, { turn: { id: "missing" } })).toThrow(/missing/);
  });
  it.skipIf(enabled)("keeps the native trial explicitly gated", () => expect(process.env.GPTQUEUE_QUALIFICATION_IDLE_TASK).not.toBe("1"));
  it("rejects a task proof when the model has no post-stimulus native turn", () => {
    const model = { participantId: "codex-model", route: "codex-appserver" as const, hostRuntimeId: "model-runtime", agent: "model", cwdHash: "cwd", profileHash: "profile", epochHash: "epoch" } satisfies ParticipantIdentity;
    const peer = { ...model, participantId: "generic-peer", route: "generic-stdio" as const, hostRuntimeId: "peer-runtime", agent: "peer" } satisfies ParticipantIdentity;
    const content = `idle-${randomUUID()}: calculate 17+25`, expected = `answer ${content.split(":")[0]}: 42`, evidenceRef = ref("history.json", "history-sha");
    const native = extractNativeTraces({ turns: [{ id: "baseline", items: [{ type: "mcpToolCall", id: "base", tool: "get_runtime_status", result: { structuredContent: { status: "ok", agent: model.agent, runtime: { runtime_id: model.hostRuntimeId } } } }] }] }, model, evidenceRef);
    const generic = extractGenericTraces([{ sourceId: "send", name: "send_message", request: { to: model.agent, type: "task", content }, response: { isError: false, result: { status: "sent", message_id: "request" } } }], peer, evidenceRef);
    expect(() => collectIdleClaimExchange({ peer, model, genericTraces: generic, nativeTraces: native, postStimulusCallIds: [], nonce: content.split(":")[0]!, requestContent: content, expectedReplyContent: expected })).toThrow();
  });
  it("requires exact discovery from a runtime-bound native observation", () => {
    const model = { participantId: "codex-model", route: "codex-appserver" as const, hostRuntimeId: "model-runtime", agent: "model", cwdHash: "cwd", profileHash: "profile", epochHash: "epoch" } satisfies ParticipantIdentity;
    const peer = { ...model, participantId: "generic-peer", route: "generic-stdio" as const, hostRuntimeId: "peer-runtime", agent: "peer" } satisfies ParticipantIdentity;
    const trace = extractNativeTraces({ turns: [{ items: [{ type: "mcpToolCall", id: "runtime", tool: "get_runtime_status", result: { structuredContent: { status: "ok", agent: model.agent, runtime: { runtime_id: model.hostRuntimeId } } } }, { type: "mcpToolCall", id: "discover", tool: "list_agents", result: { structuredContent: { status: "ok", agent: model.agent, runtime: { runtime_id: model.hostRuntimeId }, agents: [{ name: peer.agent, online: true }] } } }] }] }, model, ref("history.json", "history-sha"));
    expect(collectPeerDiscovery(model, peer, trace).observedPeer).toMatchObject({ name: peer.agent, online: true });
    expect(trace.every(item => item.runtimeBound)).toBe(true);
  });
  it("does not treat a controller injected prompt after the boundary as proof", () => {
    expect(() => assertIdleObservation({ boundary: 10, nativeTurnIds: ["baseline", "task-turn"], controllerEvents: [{ kind: "controller_prompt", at: 11 }] }, ["baseline"])).toThrow(/controller prompt/);
    expect(() => assertIdleObservation({ boundary: 10, nativeTurnIds: ["baseline"], controllerEvents: [] }, ["baseline"])).toThrow(/no new native turn/);
    expect(() => assertIdleObservation({ boundary: 10, nativeTurnIds: ["baseline", "task-turn"], controllerEvents: [{ kind: "controller_prompt", at: 10 }] }, ["baseline"])).toThrow(/controller prompt/);
    expect(postTurnCallIds({ turns: [{ id: "baseline", items: [{ id: "old", type: "mcpToolCall" }] }, { id: "task-turn", items: [{ id: "new", type: "mcpToolCall" }] }] }, ["baseline"])).toEqual(["new"]);
  });

  it.skipIf(!enabled)("proves an automatically handled native idle task", async () => {
    const runId = randomUUID(), artifactDir = join(artifactRoot, runId), phasesDir = join(artifactDir, "phases");
    await mkdir(phasesDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, runner_pid: process.pid, passed: false, execution: { status: "running" }, lifecycle: { status: "running" }, started_at: new Date().toISOString(), command: ["/home/rahul/nodeenv2251-311/bin/node", "./node_modules/vitest/vitest.mjs", "run", "tests/acceptance/qualification-idle-task.test.ts"], phases: [], cleanup: [], source_hashes_before: {}, source_hashes_after: {} };
    const persist = async (label: string, value: unknown): Promise<void> => { const phase = { label, at: new Date().toISOString(), value: redact(value) }; (receipt.phases as Json[]).push(phase); await writeFile(join(phasesDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 }); await writeFile(join(artifactDir, "receipt.json"), `${JSON.stringify(redact(receipt), null, 2)}\n`, { mode: 0o600 }); };
    let redis: OwnedRedis | undefined, workspace: string | undefined, codex: ReturnType<typeof createCodexAdapters> | undefined, generic: ReturnType<typeof createGenericAdapters> | undefined, model: CodexParticipant | undefined, peer: GenericParticipant | undefined, failure: unknown;
    try {
      receipt.source_executables = { node_executable: process.execPath, codex_executable: process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex" };
      receipt.gate = "GPTQUEUE_QUALIFICATION_IDLE_TASK=1";
      await persist("receipt-initial", { runner_pid: process.pid }); receipt.source_hashes_before = await hashes(); await persist("source-hashes-before", receipt.source_hashes_before);
      redis = await startOwnedRedis(); workspace = await mkdtemp(join(tmpdir(), "gptqueue-idle-task-"));
      receipt.row_id = "codex-appserver:automatic:idle-task";
      receipt.constraints = "Owned private-MCP-only adapter; automatic handling, not initiative or installation-wide activation";
      receipt.redis = { port: Number(new URL(redis.url).port), database: 15, owned_process: true };
      codex = createCodexAdapters({ workspaceRoot: workspace, codexBin: process.env.CODEX_BIN ?? "/home/rahul/.local/bin/codex", model: "gpt-5.6-luna" }); generic = createGenericAdapters({ repo });
      const modelAdapter = codex.adapters.find(({ spec }) => spec.id === "codex-appserver"), peerAdapter = generic.adapters.find(({ spec }) => spec.id === "generic-stdio");
      if (!modelAdapter || !peerAdapter) throw new Error("required adapters unavailable");
      const signal = AbortSignal.timeout(420_000);
      if ((await modelAdapter.preflight(signal)).kind !== "available" || (await peerAdapter.preflight(signal)).kind !== "available") throw new Error("required adapter preflight unavailable");
      peer = await peerAdapter.launch({ role: "sender", pairId: runId, nonce: `peer-${runId}`, redisUrl: redis.url }, signal) as GenericParticipant; await persist("peer-launched", peer.identity);
      model = await modelAdapter.launch({ role: "receiver", pairId: runId, nonce: `model-${runId}`, redisUrl: redis.url }, signal) as CodexParticipant; await persist("model-launched", { identity: model.identity, provenance: model.provenance });
      receipt.identities = { peer: peer.identity, model: model.identity }; receipt.provenance = model.provenance;
      await peer.call("list_agents", {}, signal);
      const events: Array<{ kind: string; at: number }> = [{ kind: "controller_prompt", at: 1 }];
      receipt.controller_events = events;
      const setupPromptResult = await model.prompt("Call get_runtime_status and list_agents, then reply READY and end the turn.", signal);
      await persist("setup-prompt-returned", setupPromptResult);
      const baselineModel = await model.history(signal), baselineModelPath = join(artifactDir, "baseline-model-history.json"); await writeFile(baselineModelPath, `${JSON.stringify(redact(baselineModel), null, 2)}\n`, { mode: 0o600 });
      const baselinePeer = await peer.history(signal), baselinePeerPath = join(artifactDir, "baseline-peer-history.json"); await writeFile(baselinePeerPath, `${JSON.stringify(redact(baselinePeer), null, 2)}\n`, { mode: 0o600 });
      receipt.setup_turn_id = assertSetupTurnCompleted(baselineModel, setupPromptResult);
      const before = receipt.source_hashes_before as Json;
      const baselineNative = extractNativeTraces(JSON.parse(await readFile(baselineModelPath, "utf8")), model.identity, ref(baselineModelPath, await digest(baselineModelPath), before)), baselineGeneric = extractGenericTraces(JSON.parse(await readFile(baselinePeerPath, "utf8")), peer.identity, ref(baselinePeerPath, await digest(baselinePeerPath), before));
      const binding = baselineNative.find(trace => {
        const runtime = trace.output?.runtime as Json | undefined;
        return trace.name === "get_runtime_status" && trace.successful && trace.runtimeBound && trace.output?.activation_ready === true && runtime?.runtime_id === model!.identity.hostRuntimeId && typeof runtime.working_directory === "string" && createHash("sha256").update(resolve(runtime.working_directory)).digest("hex") === model!.identity.cwdHash;
      });
      if (!binding) throw new Error("native baseline lacks exact activation-ready binding");
      const discovery = { model_to_peer: collectPeerDiscovery(model.identity, peer.identity, baselineNative), peer_to_model: collectPeerDiscovery(peer.identity, model.identity, baselineGeneric) };
      const idle = await model.status(signal); expect(idle).toEqual({ kind: "idle", runtimeId: model.identity.hostRuntimeId });
      const baselineTurns = ids(baselineModel), nonce = `idle-${randomUUID()}`, left = 17, right = 25;
      const requestContent = `Task ${nonce}: calculate ${left} + ${right}. Send a correlated result with content "answer ${nonce}: <decimal answer>".`, expectedReplyContent = `answer ${nonce}: ${left + right}`;
      receipt.baseline = { turn_ids: baselineTurns, binding, discovery, idle }; receipt.plan = { nonce, left, right, requestContent, expectedReplyContent };
      const boundary = events.length + 1; events.push({ kind: "stimulus_boundary", at: boundary }); await persist("baseline-ready", receipt.baseline);
      const sent = await peer.call("send_message", { to: model.identity.agent, type: "task", content: requestContent, idempotency_key: `${nonce}:task` }, signal); await persist("stimulus", { boundary, nonce, requestContent, expectedReplyContent, sent });
      if ((sent as Json)?.status !== "sent" || typeof (sent as Json).message_id !== "string") throw new Error("generic peer task send failed");
      events.push({ kind: "generic_send", at: events.length + 1 });
      let returned = false, proven = false, observation = 0, lastCollectorError: unknown;
      const deadline = Date.now() + 180_000;
      while (Date.now() < deadline) {
        if (!returned) { const response = await peer.call("receive_message", { timeout: 5 }, AbortSignal.any([signal, AbortSignal.timeout(10_000)])); returned = (response as Json)?.status === "message"; await persist("generic-receive", response); }
        const finalModel = await model.history(AbortSignal.any([signal, AbortSignal.timeout(15_000)])), finalPeer = await peer.history(signal);
        const finalModelPath = join(artifactDir, `observation-${++observation}-model.json`), finalPeerPath = join(artifactDir, `observation-${observation}-peer.json`);
        await writeFile(finalModelPath, `${JSON.stringify(redact(finalModel), null, 2)}\n`, { mode: 0o600 }); await writeFile(finalPeerPath, `${JSON.stringify(redact(finalPeer), null, 2)}\n`, { mode: 0o600 });
        const modelRef = ref(finalModelPath, await digest(finalModelPath), before), peerRef = ref(finalPeerPath, await digest(finalPeerPath), before);
        const native = extractNativeTraces(JSON.parse(await readFile(finalModelPath, "utf8")), model.identity, modelRef), genericTraces = extractGenericTraces(JSON.parse(await readFile(finalPeerPath, "utf8")), peer.identity, peerRef, true), postIds = postTurnCallIds(finalModel, baselineTurns);
        if ((finalModel as Json)?.id !== model.identity.hostRuntimeId) throw new Error("history belongs to another native thread");
        try {
          assertIdleObservation({ boundary, nativeTurnIds: ids(finalModel), controllerEvents: events }, baselineTurns);
          const exchange = collectIdleClaimExchange({ peer: peer.identity, model: model.identity, genericTraces, nativeTraces: native, postStimulusCallIds: postIds, nonce, requestContent, expectedReplyContent });
          const verdict = checkExchangeEvidence(exchange); if (verdict.outcome !== "meets") throw new Error(JSON.stringify(verdict));
          receipt.proof = { boundary, baseline_turn_ids: baselineTurns, final_turn_ids: ids(finalModel), post_stimulus_call_ids: postIds, model_history: modelRef, peer_history: peerRef, exchange, verdict };
          await persist("proof", receipt.proof); proven = true; break;
        } catch (error) { lastCollectorError = String(error); receipt.last_collector_error = lastCollectorError; }
        await wait(500);
      }
      if (!proven) throw new Error(`idle task proof absent after 180s: ${String(lastCollectorError)}`);
      receipt.execution = { status: "completed" }; receipt.passed = true;
      try { await waitIdle(model, signal); receipt.lifecycle = { status: "idle", grace_ms: 15_000 }; }
      catch (error) { receipt.lifecycle = { status: "unresolved", detail: String(error), grace_ms: 15_000 }; }
      await persist("lifecycle", receipt.lifecycle);
    } catch (error) { failure = error; receipt.execution = { status: "failed", detail: String(error) }; await persist("failure", String(error)).catch(() => undefined); if (model) await model.history(AbortSignal.timeout(10_000)).then(history => persist("failure-model-history", history)).catch(historyError => persist("failure-model-history-error", String(historyError)).catch(() => undefined)); if (peer) await peer.history(AbortSignal.timeout(10_000)).then(history => persist("failure-peer-history", history)).catch(historyError => persist("failure-peer-history-error", String(historyError)).catch(() => undefined)); } finally {
      const cleanup: CleanupResult[] = []; for (const action of [{ name: "model.close", run: async () => model?.close() }, { name: "peer.close", run: async () => peer?.close() }, { name: "codex.close", run: async () => codex?.close() }, { name: "generic.close", run: async () => generic?.close() }, { name: "redis.close", run: async () => redis?.close() }, { name: "workspace.remove", run: async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); } }] as const) { try { await action.run(); cleanup.push({ name: action.name, status: "fulfilled" }); } catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); } }
      receipt.cleanup = cleanup; if (cleanup.some(item => item.status === "rejected")) { receipt.passed = false; receipt.lifecycle = { status: "cleanup_failed" }; failure ??= new Error("cleanup failed"); } receipt.source_hashes_after = await hashes().catch(() => ({})); if (JSON.stringify(receipt.source_hashes_before) !== JSON.stringify(receipt.source_hashes_after)) { receipt.passed = false; failure ??= new Error("source changed during qualification"); } receipt.ended_at = new Date().toISOString(); await persist("cleanup-after", cleanup).catch(() => undefined);
    }
    if (failure) throw failure; expect(receipt.passed).toBe(true);
  });
});
