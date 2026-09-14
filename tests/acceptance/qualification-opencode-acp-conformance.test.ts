import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { hashText, openOwnedRedis, writeRepairReceipt } from "./opencode-repair-support.js";
import { opencodeRouteAdapter } from "./qualification-opencode.js";
import { isModelParticipant } from "./qualification-types.js";

const enabled = process.env.GPTQUEUE_OPENCODE_ACP_CONFORMANCE === "1";
const overallTimeoutMs = 350_000;
const stageTimeoutMs = 150_000;

const stageSignal = (overall: AbortSignal): AbortSignal => AbortSignal.any([
  overall,
  AbortSignal.timeout(stageTimeoutMs),
]);

const valueHash = (value: unknown): string => hashText(JSON.stringify(value) ?? "undefined");
const processIsAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

const closeWithDeadline = async (close: () => Promise<void>): Promise<Readonly<{ completed: boolean; error?: string }>> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      close(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("ACP cleanup timeout")), 90_000); }),
    ]);
    return { completed: true };
  } catch (error) {
    return { completed: false, error: String(error) };
  } finally {
    if (timer) clearTimeout(timer);
  }
};

describe.skipIf(!enabled)("OpenCode ACP bounded native conformance", () => {
  it("launches, controls, observes, and cleans one repaired ACP participant", async () => {
    const overall = AbortSignal.timeout(overallTimeoutMs);
    const runID = `opencode-acp-conformance-${new Date().toISOString().replace(/[:.]/gu, "-")}-${randomUUID()}`;
    const receipt: Record<string, unknown> = {
      run_id: runID,
      route: "opencode-acp",
      redis: { owned_process: true, host: "127.0.0.1", database: 15 },
      started_at: new Date().toISOString(),
      cleanup: [],
    };
    const sourcePaths = [
      "tests/acceptance/qualification-opencode.ts",
      "tests/acceptance/qualification-opencode-acp-conformance.test.ts",
      "tests/acceptance/opencode-repair-support.ts",
      "tests/acceptance/owned-redis.ts",
      "dist/registered-shell/opencode-plugin.js",
      "dist/registered-shell/opencode-plugin-factory.js",
      "dist/registered-shell/opencode-backend.js",
      "dist/registered-shell/opencode-runtime.js",
    ];
    const sourceHashes = async () => Object.fromEntries(await Promise.all(sourcePaths.map(async (path) => [path, hashText(await readFile(path, "utf8"))])));
    receipt.source_hashes_before = await sourceHashes();
    let owned: Awaited<ReturnType<typeof openOwnedRedis>> | undefined;
    let participant: Awaited<ReturnType<NonNullable<ReturnType<typeof opencodeRouteAdapter>>["launch"]>> | undefined;
    let processIds: readonly number[] = [];
    try {
      owned = await openOwnedRedis();
      if (!/^redis:\/\/127\.0\.0\.1(?::\d+)?\/15$/u.test(owned.url)) throw new Error("owned Redis endpoint was not local db15");
      const adapter = opencodeRouteAdapter("opencode-acp");
      expect(adapter).toBeDefined();
      const preflight = await adapter!.preflight(stageSignal(overall));
      receipt.preflight = preflight;
      expect(preflight).toMatchObject({ kind: "available" });
      participant = await adapter!.launch({
        role: "sender", pairId: runID, nonce: runID, redisUrl: owned.url,
      }, stageSignal(overall));
      if (!isModelParticipant(participant)) throw new Error("ACP adapter returned a non-model participant");
      processIds = (participant as typeof participant & { ownedProcessIds?: readonly number[] }).ownedProcessIds ?? [];
      const firstStatus = await participant.status(stageSignal(overall));
      const firstHistory = await participant.history(stageSignal(overall));
      const promptNonce = `ACP qualification control nonce ${runID}`;
      const prompt = await participant.prompt(`Reply with this exact nonce and no other text: ${promptNonce}`, stageSignal(overall));
      const secondStatus = await participant.status(stageSignal(overall));
      const secondHistory = await participant.history(stageSignal(overall));
      const firstHistoryRows = Array.isArray(firstHistory) ? firstHistory : [];
      const secondHistoryRows = Array.isArray(secondHistory) ? secondHistory : [];
      const evidence = (participant as typeof participant & { evidence?: () => Promise<unknown> }).evidence
        ? await (participant as typeof participant & { evidence: () => Promise<unknown> }).evidence()
        : { history: secondHistory };
      receipt.identity = participant.identity;
      receipt.first_status = firstStatus;
      receipt.second_status = secondStatus;
      receipt.prompt_result_hash = valueHash(prompt);
      receipt.first_history_sha256 = valueHash(firstHistory);
      receipt.second_history_sha256 = valueHash(secondHistory);
      receipt.evidence = evidence;
      const promptResult = prompt && typeof prompt === "object" ? prompt as Record<string, unknown> : {};
      expect(participant.identity.route).toBe("opencode-acp");
      expect(participant.identity.hostRuntimeId).toBeTruthy();
      expect(firstStatus.kind).toBe("unknown");
      expect(secondStatus.kind).toBe("unknown");
      expect(promptResult.sessionID).toBe(participant.identity.hostRuntimeId);
      expect(promptResult.stopReason).toBe("end_turn");
      expect(promptResult.assistantText).toContain(promptNonce);
      expect(secondHistoryRows.length).toBeGreaterThanOrEqual(firstHistoryRows.length);
      receipt.idle_status_limitation = secondStatus.kind === "unknown" ? secondStatus.detail : "unexpectedly observed";
    } catch (error) {
      receipt.error = String(error);
      const diagnostic = error && typeof error === "object" ? (error as { evidence?: unknown }).evidence : undefined;
      if (diagnostic !== undefined) {
        receipt.launch_diagnostic = diagnostic;
        const diagnosticObject = diagnostic && typeof diagnostic === "object" ? diagnostic as Record<string, unknown> : undefined;
        const processId = diagnosticObject?.process_id;
        if (typeof processId === "number") {
          const remaining = processIsAlive(processId);
          (receipt.cleanup as unknown[]).push({ launch_failure_process_remaining: remaining, completed: !remaining });
        }
      }
      throw error;
    } finally {
      if (participant) {
        const cleanup = await closeWithDeadline(participant.close);
        const remaining = processIds.filter(processIsAlive);
        (receipt.cleanup as unknown[]).push({ ...cleanup, owned_processes_remaining: remaining, completed: cleanup.completed && remaining.length === 0 });
      }
      if (owned) await owned.close().catch((error) => { receipt.redis_cleanup_error = String(error); });
      receipt.source_hashes_after = await sourceHashes();
      const sourcesMatch = JSON.stringify(receipt.source_hashes_before) === JSON.stringify(receipt.source_hashes_after);
      const cleanupComplete = (receipt.cleanup as Array<{ completed: boolean }>).every((item) => item.completed) && receipt.redis_cleanup_error === undefined;
      receipt.passed = receipt.error === undefined && sourcesMatch && cleanupComplete;
      receipt.finished_at = new Date().toISOString();
      receipt.receipt_sha256 = valueHash(receipt);
      writeRepairReceipt(runID, receipt);
      if (!sourcesMatch || !cleanupComplete) throw new Error("ACP conformance source verification or cleanup failed");
    }
  }, overallTimeoutMs + 90_000);
});
