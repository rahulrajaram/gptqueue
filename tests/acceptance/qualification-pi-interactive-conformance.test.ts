import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { createPiInteractiveAdapter, hasFreshPiInteractiveTurnEvidence } from "./qualification-pi-interactive.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import type { Participant } from "./qualification-types.js";
import { sanitizeEvidence } from "./public-evidence.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_PI_INTERACTIVE === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-pi-interactive");
const sourceFiles = [
  "tests/acceptance/qualification-pi-interactive.ts", "tests/acceptance/qualification-pi-interactive-conformance.test.ts", "tests/acceptance/qualification-types.ts", "tests/acceptance/owned-redis.ts", "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js",
] as const;
vi.setConfig({ testTimeout: 480_000, hookTimeout: 60_000 });
type Json = Record<string, unknown>;
const digest = async (file: string): Promise<string> => createHash("sha256").update(await readFile(join(repo, file))).digest("hex");
const identity = (participant: Participant): Json => ({ participant_id: participant.identity.participantId, route: participant.identity.route, host_runtime_id: participant.identity.hostRuntimeId, agent: participant.identity.agent, cwd_hash: participant.identity.cwdHash, profile_hash: participant.identity.profileHash, epoch_hash: participant.identity.epochHash });
const historyArray = (value: unknown): readonly unknown[] => Array.isArray(value) ? value : [];

describe.skipIf(!enabled)("Pi interactive qualification", () => {
  it("keeps two actual PTY participants isolated and records session histories", async () => {
    const provider = process.env.GPTQUEUE_PI_PROVIDER;
    const model = process.env.GPTQUEUE_PI_MODEL;
    if (!provider || !model) throw new Error("GPTQUEUE_PI_PROVIDER and GPTQUEUE_PI_MODEL are required");
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, route: "pi-interactive", passed: false, execution: { status: "running" }, provider, model, source_hashes: {}, phases: [], cleanup: [] };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let sender: Participant | undefined;
    let receiver: Participant | undefined;
    let failure: unknown;
    try {
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      await persist("source-hashes", receipt.source_hashes);
      redis = await startOwnedRedis();
      workspace = join(artifactDir, "workspace");
      const adapter = createPiInteractiveAdapter({ workspaceRoot: workspace, provider, model });
      const signal = AbortSignal.timeout(420_000);
      const availability = await adapter.preflight(signal);
      await persist("preflight", availability);
      expect(availability.kind).toBe("available");
      sender = await adapter.launch({ role: "sender", pairId: "pi-interactive-conformance", nonce: `${runId}-sender`, redisUrl: redis.url }, signal);
      receiver = await adapter.launch({ role: "receiver", pairId: "pi-interactive-conformance", nonce: `${runId}-receiver`, redisUrl: redis.url }, signal);
      receipt.identities = { sender: identity(sender), receiver: identity(receiver) };
      await persist("launched", receipt.identities);
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      expect((await Promise.all([sender.status(signal), receiver.status(signal)])).every(status => status.kind === "unknown")).toBe(true);
      if (sender.kind !== "model" || receiver.kind !== "model") throw new Error("Pi interactive route did not launch model participants");
      const before = await Promise.all([sender.history(signal), receiver.history(signal)]);
      await Promise.all([
        sender.prompt("Call get_runtime_status exactly once, then reply exactly PI_INTERACTIVE_SENDER_READY.", signal),
        receiver.prompt("Call get_runtime_status exactly once, then reply exactly PI_INTERACTIVE_RECEIVER_READY.", signal),
      ]);
      const histories = await Promise.all([sender.history(signal), receiver.history(signal)]);
      await persist("history", histories);
      await writeFile(join(artifactDir, "history-sanitized.json"), `${JSON.stringify(sanitizeEvidence(histories, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
      expect(hasFreshPiInteractiveTurnEvidence(historyArray(histories[0]), historyArray(before[0]).length, { marker: "PI_INTERACTIVE_SENDER_READY", agent: sender.identity.agent, runtimeId: sender.identity.hostRuntimeId })).toBe(true);
      expect(hasFreshPiInteractiveTurnEvidence(historyArray(histories[1]), historyArray(before[1]).length, { marker: "PI_INTERACTIVE_RECEIVER_READY", agent: receiver.identity.agent, runtimeId: receiver.identity.hostRuntimeId })).toBe(true);
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
      const failureSignal = AbortSignal.timeout(10_000);
      const failureHistories = await Promise.all([
        sender?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
        receiver?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
      ]);
      await persist("failure-history", failureHistories).catch(() => undefined);
      await persist("failure", { error: String(error) }).catch(() => undefined);
    } finally {
      const cleanup: Json[] = [];
      const actions: readonly [string, (() => Promise<unknown>) | undefined][] = [
        ["sender.close", sender ? () => sender!.close() : undefined],
        ["receiver.close", receiver ? () => receiver!.close() : undefined],
        ["redis.close", redis ? () => redis!.close() : undefined],
        ["workspace.remove", workspace ? async () => rm(workspace!, { recursive: true, force: true }) : undefined],
      ];
      for (const [name, action] of actions) {
        if (!action) { cleanup.push({ name, status: "skipped", reason: "not_started" }); continue; }
        try { await action(); cleanup.push({ name, status: "fulfilled" }); } catch (error) { cleanup.push({ name, status: "rejected", error: String(error) }); }
      }
      receipt.cleanup = cleanup;
      receipt.source_hashes_after = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      receipt.source_hashes_match = JSON.stringify(receipt.source_hashes) === JSON.stringify(receipt.source_hashes_after);
      if (receipt.source_hashes_match !== true || cleanup.some(item => item.status === "rejected")) { receipt.passed = false; failure ??= new Error("Pi interactive cleanup/source verification failed"); }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
