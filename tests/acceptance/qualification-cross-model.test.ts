import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it, vi } from "vitest";
import { createCodexAdapters, type CodexParticipant } from "./qualification-codex.js";
import { createPiAdapters, type PiRpcParticipant } from "./qualification-pi.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import { CohortLeasePool, type CohortMember } from "./qualification-scheduler.js";
import { runModelPair, type TypedEvidenceWriter } from "./qualification-driver.js";
import { extractNativeTraces } from "./qualification-evidence.js";
import { checkExchangeEvidence } from "./oracle.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_CROSS_MODEL === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-cross-model");
const sourceFiles = [
  "tests/acceptance/codex-appserver-history.ts", "/home/rahul/.local/bin/codex", "tests/acceptance/qualification-cross-model.test.ts", "tests/acceptance/qualification-driver.ts", "tests/acceptance/qualification-evidence.ts", "tests/acceptance/qualification-types.ts", "tests/acceptance/qualification-scheduler.ts", "tests/acceptance/qualification-codex.ts", "tests/acceptance/qualification-pi.ts", "tests/acceptance/oracle.ts", "tests/acceptance/qualification-codex-appserver.test.ts", "tests/acceptance/qualification-pi-rpc.test.ts", "tests/acceptance/codex-support.ts", "src/experimental-wrapper/bridge.ts", "src/registered-shell/runtime.ts", "src/registered-shell/codex-socket.ts", "src/registered-shell/codex-history.ts", "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js",
] as const;
const digest = async (path: string): Promise<string> => createHash("sha256").update(await readFile(path)).digest("hex");
const sourceHashes = async (): Promise<Readonly<Record<string, string>>> => Object.fromEntries(await Promise.all(sourceFiles.map(async (file) => [file, await digest(resolve(repo, file))] as const)));
const safeName = (value: string): string => createHash("sha256").update(value).digest("hex").slice(0, 16);
const identity = (participant: CodexParticipant | PiRpcParticipant): Readonly<Record<string, unknown>> => ({ participant_id: participant.identity.participantId, route: participant.identity.route, host_runtime_id: participant.identity.hostRuntimeId, agent: participant.identity.agent, cwd_hash: participant.identity.cwdHash, profile_hash: participant.identity.profileHash, epoch_hash: participant.identity.epochHash, provenance: participant.provenance });
type Json = Record<string, unknown>;
type CleanupResult = Readonly<{ name: string; status: "fulfilled" | "rejected"; error?: string }>;

vi.setConfig({ testTimeout: 600_000, hookTimeout: 60_000 });

describe.skipIf(!enabled)("Codex appserver to Pi RPC qualification pair", () => {
  it("records assisted prompted communication separately from automatic idle activation", async () => {
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, route: "codex-appserver->pi-rpc-cli", execution: { status: "running" }, passed: false, started_at: new Date().toISOString(), models: { codex: "gpt-5.6-luna", pi: { provider: "openrouter", model: "z-ai/glm-5.3-flash" } }, source_hashes: {}, phases: [], cleanup: [], verdicts: { assisted_prompted_receiver_communication: { outcome: "uncertain", execution: { status: "not_run" } }, automatic_idle_activation: { outcome: "not_applicable", execution: { status: "unsupported" }, detail: "This gate deliberately prompts the receiver to run its claim loop; an idle activation gate is separate." } } };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true, redactSessionObjectIds: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true, redactSessionObjectIds: true }), null, 2)}\n`, { mode: 0o600 });
    };
    const before = await sourceHashes();
    receipt.source_hashes = before;
    await persist("source-hashes", before);
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let codexSet: ReturnType<typeof createCodexAdapters> | undefined;
    let piSet: ReturnType<typeof createPiAdapters> | undefined;
    let sender: CodexParticipant | undefined;
    let receiver: PiRpcParticipant | undefined;
    let failure: unknown;
    try {
      redis = await startOwnedRedis();
      receipt.redis = { database: new URL(redis.url).pathname.slice(1), host: new URL(redis.url).hostname, port: new URL(redis.url).port };
      workspace = await mkdtemp(join(tmpdir(), "gptqueue-qualification-cross-model-"));
      await mkdir(join(workspace, "codex"), { recursive: true });
      await mkdir(join(workspace, "pi"), { recursive: true });
      codexSet = createCodexAdapters({ workspaceRoot: join(workspace, "codex"), model: "gpt-5.6-luna" });
      piSet = createPiAdapters({ workspaceRoot: join(workspace, "pi"), provider: "openrouter", model: "z-ai/glm-5.3-flash" });
      const codexAdapter = codexSet.adapters.find(({ spec }) => spec.id === "codex-appserver");
      const piAdapter = piSet.adapters.find(({ spec }) => spec.id === "pi-rpc-cli");
      if (!codexAdapter || !piAdapter) throw new Error("cross-model adapters are unavailable");
      const gateSignal = AbortSignal.timeout(540_000);
      const availability = await Promise.all([codexAdapter.preflight(gateSignal), piAdapter.preflight(gateSignal)]);
      await persist("preflight", availability);
      expect(availability).toEqual([{ kind: "available" }, { kind: "available" }]);
      sender = await codexAdapter.launch({ role: "sender", pairId: "codex-pi-cross-model", nonce: runId, redisUrl: redis.url }, gateSignal) as CodexParticipant;
      receiver = await piAdapter.launch({ role: "receiver", pairId: "codex-pi-cross-model", nonce: runId, redisUrl: redis.url }, gateSignal) as PiRpcParticipant;
      await persist("launched", { sender: identity(sender), receiver: identity(receiver), shared_redis_database: new URL(redis.url).pathname.slice(1) });
      expect(sender.identity.route).toBe("codex-appserver");
      expect(receiver.identity.route).toBe("pi-rpc-cli");
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      await Promise.all([
        sender.prompt("Call get_runtime_status exactly once, then reply RUNTIME_STATUS_SENDER_READY.", gateSignal),
        receiver.prompt("Call get_runtime_status exactly once, then reply RUNTIME_STATUS_RECEIVER_READY.", gateSignal),
      ]);
      const [senderInitialHistory, receiverInitialHistory] = await Promise.all([sender.history(gateSignal), receiver.history(gateSignal)]);
      const runtimeRaw = { path: "runtime-initialization", sha256: "runtime-initialization", sourceRevision: before["tests/acceptance/qualification-codex.ts"] ?? "source", oracleRevision: before["tests/acceptance/oracle.ts"] ?? "oracle" };
      const senderRuntimeTraces = extractNativeTraces(senderInitialHistory, sender.identity, runtimeRaw);
      const receiverRuntimeTraces = extractNativeTraces(receiverInitialHistory, receiver.identity, runtimeRaw);
      expect(senderRuntimeTraces.some((trace) => trace.name === "get_runtime_status" && trace.runtimeBound === true)).toBe(true);
      expect(receiverRuntimeTraces.some((trace) => trace.name === "get_runtime_status" && trace.runtimeBound === true)).toBe(true);
      await persist("runtime-initialized", { sender: { history: senderInitialHistory, runtime_status_calls: senderRuntimeTraces.filter((trace) => trace.name === "get_runtime_status").length }, receiver: { history: receiverInitialHistory, runtime_status_calls: receiverRuntimeTraces.filter((trace) => trace.name === "get_runtime_status").length } });
      const initial = await Promise.all([sender.status(gateSignal), receiver.status(gateSignal)]);
      await persist("initial-status", initial);
      expect(initial[0]?.kind).toBe("idle");
      expect(initial[1]?.kind).toBe("idle");
      let snapshot = 0;
      const writer: TypedEvidenceWriter = {
        writeHistory: async ({ actor, serialized }): Promise<Readonly<{ path: string; sha256: string; sourceRevision: string; oracleRevision: string }>> => {
          const path = join(artifactDir, `history-${String(++snapshot).padStart(5, "0")}-${safeName(actor.agent)}.json`);
          await writeFile(path, serialized, { mode: 0o600 });
          return { path, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: before["tests/acceptance/qualification-driver.ts"] ?? "source", oracleRevision: before["tests/acceptance/oracle.ts"] ?? "oracle" };
        },
      };
      const members: readonly CohortMember[] = [{ participant: sender, identity: sender.identity }, { participant: receiver, identity: receiver.identity }];
      const result = await runModelPair(new CohortLeasePool(members), { pair: { pairId: "codex-appserver->pi-rpc-cli", sender: "codex-appserver", receiver: "pi-rpc-cli", nonce: `cross-${runId}` }, left: 17, right: 25, evidenceWriter: writer }, gateSignal);
      const verdict = checkExchangeEvidence(result.exchange!);
      receipt.verdicts = { assisted_prompted_receiver_communication: verdict, automatic_idle_activation: { outcome: "not_applicable", execution: { status: "unsupported" }, detail: "Receiver was deliberately prompted to run a claim loop for this communication gate." } };
      await persist("exchange-verdict", { result, verdict });
      expect(verdict.outcome).toBe("meets");
      expect(result.exchange?.request_consumption?.acknowledged).toBe(true);
      expect(result.exchange?.reply_consumption?.acknowledged).toBe(true);
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
      const failureSignal = AbortSignal.timeout(10_000);
      const histories = await Promise.all([
        sender?.history(failureSignal).catch((historyError) => ({ history_error: String(historyError) })),
        receiver?.history(failureSignal).catch((historyError) => ({ history_error: String(historyError) })),
      ]);
      if (histories[0] !== undefined) await writeFile(join(artifactDir, "history-sender-failure-sanitized.json"), `${JSON.stringify(sanitizeEvidence(histories[0], { parseEmbeddedJson: true, redactSessionObjectIds: true }), null, 2)}\n`, { mode: 0o600 }).catch(() => undefined);
      if (histories[1] !== undefined) await writeFile(join(artifactDir, "history-receiver-failure-sanitized.json"), `${JSON.stringify(sanitizeEvidence(histories[1], { parseEmbeddedJson: true, redactSessionObjectIds: true }), null, 2)}\n`, { mode: 0o600 }).catch(() => undefined);
      await persist("failure-histories", histories).catch(() => undefined);
    } finally {
      await persist("cleanup-before", { sender: Boolean(sender), receiver: Boolean(receiver), codex_factory: Boolean(codexSet), pi_factory: Boolean(piSet), redis: Boolean(redis) }).catch(() => undefined);
      const cleanup: CleanupResult[] = [];
      const actions: readonly Readonly<{ name: string; run: () => Promise<void> }>[] = [
        { name: "sender.close", run: async () => { await sender?.close(); } },
        { name: "receiver.close", run: async () => { await receiver?.close(); } },
        { name: "codex-factory.close", run: async () => { await codexSet?.close(); } },
        { name: "pi-factory.close", run: async () => { await piSet?.close(); } },
        { name: "redis.close", run: async () => { await redis?.close(); } },
        { name: "workspace.remove", run: async () => { if (workspace) await rm(workspace, { recursive: true, force: true }); } },
      ];
      for (const action of actions) {
        try { await action.run(); cleanup.push({ name: action.name, status: "fulfilled" }); }
        catch (error) { cleanup.push({ name: action.name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      if (cleanup.some((item) => item.status === "rejected")) { receipt.passed = false; receipt.execution = { status: "failed", detail: "ordered cleanup failed" }; failure ??= new Error("ordered cleanup failed"); }
      const after = await sourceHashes();
      receipt.source_hashes_after = after;
      receipt.source_hashes_match = JSON.stringify(before) === JSON.stringify(after);
      if (!receipt.source_hashes_match) { receipt.passed = false; receipt.execution = { status: "failed", detail: "source hashes changed during cross-model gate" }; failure ??= new Error("source hashes changed during cross-model gate"); }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup-after", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
