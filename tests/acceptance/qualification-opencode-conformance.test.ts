import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { hashText, openOwnedRedis, writeRepairReceipt } from "./opencode-repair-support.js";
import { childReadinessEvidence, parseMcpEnvelope } from "./opencode-qualification-oracles.js";
import { opencodeRouteAdapter } from "./qualification-opencode.js";
import { isModelParticipant } from "./qualification-types.js";

const enabled = process.env.GPTQUEUE_OPENCODE_CONFORMANCE === "1";
const overallTimeoutMs = 350_000;
const stageTimeoutMs = 150_000;
const routes = ["opencode-serve-attach", "opencode-native-task", "opencode-resume", "opencode-fork"] as const;

const stageSignal = (overall: AbortSignal): AbortSignal => AbortSignal.any([
  overall,
  AbortSignal.timeout(stageTimeoutMs),
]);

const valueHash = (value: unknown): string => hashText(JSON.stringify(value) ?? "undefined");

const processIsAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
};

const boundedClose = async (close: () => Promise<void>, overall: AbortSignal): Promise<Readonly<{ completed: boolean; error?: string }>> => {
  let cleanup = (): void => undefined;
  try {
    const deadline = new Promise<never>((_, reject) => {
      const abort = () => reject(overall.reason instanceof Error ? overall.reason : new Error("conformance cleanup timed out"));
      cleanup = () => overall.removeEventListener("abort", abort);
      if (overall.aborted) abort();
      else overall.addEventListener("abort", abort, { once: true });
    });
    await Promise.race([close(), deadline]);
    return { completed: true };
  } catch (error) {
    return { completed: false, error: String(error) };
  } finally {
    cleanup();
  }
};

const evidenceRecord = async (
  participant: Awaited<ReturnType<NonNullable<ReturnType<typeof opencodeRouteAdapter>>["launch"]>>,
  signal: AbortSignal,
): Promise<Readonly<Record<string, unknown>>> => {
  const candidate = participant as typeof participant & {
    evidence?: () => Promise<Readonly<Record<string, unknown>>>;
  };
  const evidence = candidate.evidence ? await candidate.evidence() : {
    identity: participant.identity,
    history: await participant.history(signal),
  };
  const histories = Object.entries(evidence)
    .filter(([key]) => key === "history" || key.endsWith("_history") || key === "parent" || key === "child" || key === "session")
    .map(([key, value]) => [key, value] as const)
    .filter(([, value]) => value !== undefined);
  const historyHashes = Object.fromEntries(histories.map(([key, value]) => [key, valueHash(value)]));
  const child = evidence.child;
  const childObject = child && typeof child === "object" ? child as Record<string, unknown> : undefined;
  const childHistory = childObject?.history;
  const childAgent = `gptqueue-opencode-${participant.identity.hostRuntimeId}`;
  return Object.freeze({
    ...evidence,
    identity_hashes: {
      participant_id: valueHash(participant.identity.participantId),
      host_runtime_id: valueHash(participant.identity.hostRuntimeId),
      cwd: participant.identity.cwdHash,
      profile: participant.identity.profileHash,
    },
    history_hashes: historyHashes,
    mcp_envelope_counts: Object.fromEntries(histories.map(([key, value]) => [key, parseMcpEnvelope(value).length])),
    ...(childHistory === undefined ? {} : {
      child_readiness: childReadinessEvidence(childHistory, childAgent, participant.identity.hostRuntimeId) !== undefined,
    }),
  });
};

describe.skipIf(!enabled)("OpenCode bounded native conformance", () => {
  it("qualifies only persistent OpenCode routes and exact fork/resume lineage", async () => {
    const overall = AbortSignal.timeout(overallTimeoutMs);
    const runID = `opencode-conformance-${new Date().toISOString().replace(/[:.]/gu, "-")}-${randomUUID()}`;
    const receipt: Record<string, unknown> = {
      run_id: runID,
      route_population: routes,
      redis: { owned_process: true, host: "127.0.0.1", database: 15 },
      started_at: new Date().toISOString(),
      routes: [],
      cleanup: [],
    };
    const sourcePaths = ["tests/acceptance/qualification-opencode.ts", "tests/acceptance/qualification-opencode-conformance.test.ts", "tests/acceptance/opencode-qualification-oracles.ts", "tests/acceptance/opencode-repair-support.ts", "tests/acceptance/owned-redis.ts", "dist/registered-shell/opencode-plugin.js", "dist/registered-shell/opencode-plugin-factory.js", "dist/registered-shell/opencode-backend.js", "dist/registered-shell/opencode-runtime.js"];
    const sourceHashes = async () => Object.fromEntries(await Promise.all(sourcePaths.map(async (path) => [path, hashText(await readFile(path, "utf8"))])));
    receipt.source_hashes_before = await sourceHashes();
    let owned: Awaited<ReturnType<typeof openOwnedRedis>> | undefined;
    const participants: Array<{ route: string; close: () => Promise<void>; closed: boolean; processIds: () => readonly number[]; failureHistory: () => readonly string[] }> = [];
    try {
      owned = await openOwnedRedis();
      if (!/^redis:\/\/127\.0\.0\.1(?::\d+)?\/15$/u.test(owned.url)) throw new Error("owned Redis endpoint was not local db15");
      for (const route of routes) {
        const routeReceipt: Record<string, unknown> = { route, stages: {} };
        (receipt.routes as unknown[]).push(routeReceipt);
        const adapter = opencodeRouteAdapter(route);
        expect(adapter).toBeDefined();
        const preflight = await adapter!.preflight(stageSignal(overall));
        routeReceipt.stages = { preflight };
        expect(preflight).toMatchObject({ kind: "available" });
        const participant = await adapter!.launch({
          role: "sender", pairId: `${runID}-${route}`, nonce: `${runID}-${route}`, redisUrl: owned.url,
        }, stageSignal(overall));
        if (!isModelParticipant(participant)) throw new Error(`${route} adapter returned a non-model participant`);
        const ownedParticipant = {
          route,
          close: participant.close,
          closed: false,
          processIds: () => (participant as typeof participant & { ownedProcessIds?: readonly number[] }).ownedProcessIds ?? [],
          failureHistory: () => (participant as typeof participant & { cleanupFailureHistory?: readonly string[] }).cleanupFailureHistory ?? [],
        };
        participants.push(ownedParticipant);
        try {
          const status = await participant.status(stageSignal(overall));
          const history = await participant.history(stageSignal(overall));
          const evidence = await evidenceRecord(participant, stageSignal(overall));
          const exactNonce = `${route} exact assistant nonce ${runID}`;
          const promptResult = ["opencode-resume", "opencode-fork"].includes(route)
            ? await participant.prompt(`Reply with this exact nonce and no other text: ${exactNonce}`, stageSignal(overall))
            : undefined;
          const afterPromptEvidence = promptResult === undefined ? evidence : await evidenceRecord(participant, stageSignal(overall));
          routeReceipt.stages = { preflight, status, history_hash: valueHash(history), evidence: afterPromptEvidence, prompt_result_hash: valueHash(promptResult) };
          expect(participant.identity.route).toBe(route);
          expect(participant.identity.hostRuntimeId).toBeTruthy();
          expect(status).toMatchObject({ runtimeId: participant.identity.hostRuntimeId });
          if (promptResult !== undefined) {
            const promptObject = promptResult && typeof promptResult === "object" ? promptResult as Record<string, unknown> : {};
            expect(promptObject.assistantText).toBe(exactNonce);
            expect((afterPromptEvidence as Record<string, unknown>).peer_reachable_while_cli_exited).toBe(true);
          }
        } finally {
          const cleanup = await boundedClose(participant.close, AbortSignal.timeout(20_000));
          const ownedProcessIds = ownedParticipant.processIds();
          const remaining = ownedProcessIds.filter(processIsAlive);
          const cleanupResult = { ...cleanup, owned_process_ids: ownedProcessIds, owned_processes_remaining: remaining, failure_history: ownedParticipant.failureHistory(), completed: cleanup.completed && remaining.length === 0 && ownedParticipant.failureHistory().length === 0 };
          ownedParticipant.closed = cleanupResult.completed;
          (receipt.cleanup as unknown[]).push({ route, ...cleanupResult });
        }
      }
    } catch (error) {
      receipt.error = String(error);
      const diagnostic = error && typeof error === "object" ? (error as { evidence?: unknown }).evidence : undefined;
      if (diagnostic !== undefined) {
        receipt.native_task_diagnostic = diagnostic;
        const diagnosticObject = diagnostic && typeof diagnostic === "object" ? diagnostic as Record<string, unknown> : undefined;
        const processId = diagnosticObject?.owned_process_id;
        if (typeof processId === "number") {
          const remaining = processIsAlive(processId);
          (receipt.cleanup as unknown[]).push({ route: "opencode-native-task", launch_failure_process_remaining: remaining, completed: !remaining });
        }
      }
      throw error;
    } finally {
      for (const participant of participants.filter((item) => !item.closed)) {
        const cleanup = await boundedClose(participant.close, AbortSignal.timeout(20_000));
        const ownedProcessIds = participant.processIds();
        const remaining = ownedProcessIds.filter(processIsAlive);
        const cleanupResult = { ...cleanup, owned_process_ids: ownedProcessIds, owned_processes_remaining: remaining, failure_history: participant.failureHistory(), completed: cleanup.completed && remaining.length === 0 && participant.failureHistory().length === 0 };
        (receipt.cleanup as unknown[]).push({ route: participant.route, late_cleanup: true, ...cleanupResult });
      }
      if (owned) await owned.close().catch((error) => { receipt.redis_cleanup_error = String(error); });
      receipt.source_hashes_after = await sourceHashes();
      const sourcesMatch = JSON.stringify(receipt.source_hashes_before) === JSON.stringify(receipt.source_hashes_after);
      const cleanupComplete = (receipt.cleanup as Array<{ completed: boolean }>).every((item) => item.completed) && receipt.redis_cleanup_error === undefined;
      receipt.passed = receipt.error === undefined && sourcesMatch && cleanupComplete && (receipt.routes as unknown[]).length === routes.length;
      receipt.finished_at = new Date().toISOString();
      receipt.receipt_sha256 = valueHash(receipt);
      writeRepairReceipt(runID, receipt);
      if (!sourcesMatch || !cleanupComplete) throw new Error("OpenCode conformance source verification or cleanup failed");
    }
  }, overallTimeoutMs + 90_000);
});
