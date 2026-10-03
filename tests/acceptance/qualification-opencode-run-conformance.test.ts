import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";
import { hashText, writeRepairReceipt } from "./opencode-repair-support.js";
import { isModelParticipant } from "./qualification-types.js";
import { opencodeRouteAdapter, forkSessionLineage } from "./qualification-opencode.js";
import { modelsPath, opencodeBin } from "./opencode-support.js";
import { startOwnedRedis } from "./owned-redis.js";
import { homePath } from "./local-tools.js";

const enabled = process.env.GPTQUEUE_OPENCODE_RUN_ROUTES_CONFORMANCE === "1";
const overallTimeoutMs = 350_000;
const stageTimeoutMs = 150_000;
const routeInventory = ["opencode-run", "opencode-resume", "opencode-fork"] as const;
const selectedRoutes = process.env.GPTQUEUE_OPENCODE_RUN_ROUTES?.split(",").map((route) => route.trim()).filter(Boolean);
const routes: readonly (typeof routeInventory[number])[] = !selectedRoutes?.length ? routeInventory : selectedRoutes.map((route) => {
  if (!routeInventory.includes(route as typeof routeInventory[number])) throw new Error(`Unknown OpenCode conformance route: ${route}`);
  return route as typeof routeInventory[number];
});

const stageSignal = (overall: AbortSignal): AbortSignal => AbortSignal.any([
  overall,
  AbortSignal.timeout(stageTimeoutMs),
]);

const valueHash = (value: unknown): string => hashText(JSON.stringify(value) ?? "undefined");
const execFileAsync = promisify(execFile);

const dependencyPaths = [
  opencodeBin,
  modelsPath,
  homePath(".opencode/package.json"),
  homePath(".opencode/package-lock.json"),
  "package.json",
  "package-lock.json",
] as const;

const dependencySnapshot = async (): Promise<Readonly<Record<string, unknown>>> => {
  const files = Object.fromEntries(await Promise.all(dependencyPaths.map(async (path) => [path, hashText(await readFile(path))])));
  const versionResult = await execFileAsync(opencodeBin, ["--version"], { encoding: "utf8", timeout: 10_000 });
  return Object.freeze({
    executable: Object.freeze({ path: opencodeBin, sha256: files[opencodeBin], version: String(versionResult.stdout).trim() }),
    model_catalog: Object.freeze({ path: modelsPath, sha256: files[modelsPath] }),
    package_files: Object.freeze(Object.fromEntries(dependencyPaths.slice(2).map((path) => [path, files[path]]))),
  });
};

const processIsAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

const closeWithDeadline = async (close: () => Promise<void>): Promise<Readonly<{ completed: boolean; error?: string }>> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      close(),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("fork/resume cleanup timeout")), 90_000); }),
    ]);
    return { completed: true };
  } catch (error) {
    return { completed: false, error: String(error) };
  } finally {
    if (timer) clearTimeout(timer);
  }
};

describe.skipIf(!enabled)("OpenCode run/fork/resume conformance", () => {
  it("proves native lineage, exact assistant turns, and server reachability after CLI exit", async () => {
    const overall = AbortSignal.timeout(overallTimeoutMs);
    const runID = `opencode-persistent-fork-resume-${new Date().toISOString().replace(/[:.]/gu, "-")}-${randomUUID()}`;
    const receipt: Record<string, unknown> = {
      run_id: runID,
      routes,
      route_contract: "native_cli_lineage_with_owned_runtime_control",
      pair_admission: "none",
      redis: { owned_process: true, host: "127.0.0.1", database: 15 },
      started_at: new Date().toISOString(),
      routes_evidence: [],
      cleanup: [],
      failure_history: [],
    };
    const sourcePaths = [
      "tests/acceptance/qualification-opencode.ts",
      "tests/acceptance/qualification-opencode-run-conformance.test.ts",
      "tests/acceptance/opencode-repair-support.ts",
      "tests/acceptance/owned-redis.ts",
      "dist/registered-shell/opencode-plugin.js",
      "dist/registered-shell/opencode-plugin-factory.js",
      "dist/registered-shell/opencode-backend.js",
      "dist/registered-shell/opencode-runtime.js",
    ];
    const sourceHashes = async () => Object.fromEntries(await Promise.all(sourcePaths.map(async (path) => [path, hashText(await readFile(path, "utf8"))])));
    receipt.source_hashes_before = await sourceHashes();
    receipt.dependency_snapshot_before = await dependencySnapshot();
    let owned: Awaited<ReturnType<typeof startOwnedRedis>> | undefined;
    const participants: Array<Readonly<{
      route: string;
      close: () => Promise<void>;
      processIds: () => readonly number[];
      failureHistory: () => readonly string[];
      closed: { value: boolean };
    }>> = [];
    try {
      owned = await startOwnedRedis();
      if (!/^redis:\/\/127\.0\.0\.1(?::\d+)?\/15$/u.test(owned.url)) throw new Error("owned Redis endpoint was not local db15");
      for (const route of routes) {
        const adapter = opencodeRouteAdapter(route);
        expect(adapter).toBeDefined();
        const preflight = await adapter!.preflight(stageSignal(overall));
        expect(preflight).toMatchObject({ kind: "available" });
        const participant = await adapter!.launch({
          role: "sender", pairId: `${runID}-${route}`, nonce: `${runID}-${route}`, redisUrl: owned.url,
        }, stageSignal(overall));
        if (!isModelParticipant(participant)) throw new Error(`${route} adapter returned a non-model participant`);
        const candidate = participant as typeof participant & {
          evidence?: () => Promise<Readonly<Record<string, unknown>>>;
          ownedProcessIds?: readonly number[];
          cleanupFailureHistory?: readonly string[];
        };
        const state = { value: false };
        const ownedParticipant = {
          route,
          close: participant.close,
          processIds: () => candidate.ownedProcessIds ?? [],
          failureHistory: () => candidate.cleanupFailureHistory ?? [],
          closed: state,
        };
        participants.push(ownedParticipant);
        // Compact opaque nonce: long sentence-like nonces were observed to be
        // truncated by the model (echoing only the runID tail). Exactness of the
        // echoed assistant text is unchanged; only the nonce shape is test-owned.
        const exactNonce = `${route}-${createHash("sha256").update(`${runID}:${route}`).digest("hex").slice(0, 24)}`;
        let routeEvidenceRecorded = false;
        try {
          const beforeStatus = await participant.status(stageSignal(overall));
          const beforeHistory = await participant.history(stageSignal(overall));
          const promptResult = await participant.prompt(`Reply with this exact nonce and no other text: ${exactNonce}`, stageSignal(overall));
          const afterStatus = await participant.status(stageSignal(overall));
          const evidence = candidate.evidence ? await candidate.evidence() : { history: await participant.history(stageSignal(overall)) };
          const promptObject = promptResult && typeof promptResult === "object" ? promptResult as Record<string, unknown> : {};
          const nativeSession = evidence.native_session && typeof evidence.native_session === "object" ? evidence.native_session as Record<string, unknown> : {};
          const nativeTools = Array.isArray(evidence.native_tools) ? evidence.native_tools : [];
          const bindingValue = route === "opencode-run" ? evidence.registry_binding : evidence.binding_after_initial_cli_exit;
          const binding = bindingValue && typeof bindingValue === "object" ? bindingValue as Record<string, unknown> : {};
          const parentBinding = evidence.parent_binding_after_initial_cli_exit && typeof evidence.parent_binding_after_initial_cli_exit === "object"
            ? evidence.parent_binding_after_initial_cli_exit as Record<string, unknown> : {};
          (receipt.routes_evidence as unknown[]).push({
            route,
            identity: participant.identity,
            preflight,
            before_status: beforeStatus,
            before_history_sha256: valueHash(beforeHistory),
            after_status: afterStatus,
            prompt_result_hash: valueHash(promptResult),
            evidence,
            exact_lineage: {
              route,
              native_session_id: participant.identity.hostRuntimeId,
              runtime_epoch: evidence.runtime_epoch,
              initial_cli_args: evidence.initial_cli_args,
              server_process_id: evidence.server_process_id,
            },
            exact_tools_and_status: {
              native_tool_count: nativeTools.length,
              native_tools_include_runtime_status: nativeTools.includes("gptqueue_get_runtime_status"),
              runtime_status_probe_observed: evidence.runtime_status_probe_observed,
              session_status_while_cli_exited: nativeSession.status_while_cli_exited,
            },
            peer_reachability_while_cli_exited: evidence.peer_reachable_while_cli_exited,
            binding_after_cli_exit: { registration_present: binding.registrationPresent, binding_present: binding.bindingPresent },
            parent_binding_after_cli_exit: { registration_present: parentBinding.registrationPresent, binding_present: parentBinding.bindingPresent },
          });
          routeEvidenceRecorded = true;
          if (route === "opencode-run") {
            const payload = promptObject.payload && typeof promptObject.payload === "object" ? promptObject.payload as Record<string, unknown> : {};
            expect(promptObject.from).toBe(participant.identity.agent);
            expect(promptObject.to).toBe(evidence.control_identity);
            expect(promptObject.type).toBe("result");
            expect(payload.content).toBe(exactNonce);
            expect(typeof payload.in_reply_to).toBe("string");
            expect(evidence.control_traffic_excluded).toBe(true);
            expect(binding.bindingPresent).toBe(true);
            expect(evidence.native_runtime_observation).toBeDefined();
            const runtimeObservation = evidence.native_runtime_observation as Record<string, unknown>;
            if (runtimeObservation.epoch !== undefined) expect(runtimeObservation.epoch).toEqual(expect.any(String));
            const runtimeBinding = binding.binding && typeof binding.binding === "object" ? binding.binding as Record<string, unknown> : {};
            expect(runtimeBinding.epoch).toEqual(expect.any(String));
            expect(evidence.runtime_epoch).toContain(runtimeBinding.epoch as string);
            expect(evidence.native_cli_args).toEqual(expect.arrayContaining(["run", "--format", "json"]));
            expect(evidence.process_exit).toMatchObject({ state: "running" });
            expect(evidence.control_exchanges).toEqual(expect.arrayContaining([
              expect.objectContaining({ reply: expect.objectContaining({ payload: expect.objectContaining({ content: exactNonce }) }) }),
            ]));
          } else {
            expect(promptObject.assistantText).toBe(exactNonce);
            expect(promptObject.sessionID).toBe(participant.identity.hostRuntimeId);
            expect(evidence.peer_reachable_while_cli_exited).toBe(true);
            expect(evidence.runtime_status_probe_observed).toBe(true);
            expect(nativeSession.status_while_cli_exited).toBeDefined();
            expect(binding.bindingPresent).toBe(true);
            expect(parentBinding.bindingPresent).toBe(true);
            const runtimeBinding = binding.binding && typeof binding.binding === "object" ? binding.binding as Record<string, unknown> : {};
            expect(runtimeBinding.epoch).toEqual(expect.any(String));
            expect(evidence.runtime_epoch).toContain(runtimeBinding.epoch as string);
          }
          if (route !== "opencode-run") expect(nativeTools).toContain("gptqueue_get_runtime_status");
          if (route === "opencode-fork") {
            const sessionRecord = nativeSession.record && typeof nativeSession.record === "object" ? nativeSession.record as Record<string, unknown> : {};
            const seedRecord = evidence.seed && typeof evidence.seed === "object" ? (evidence.seed as Record<string, unknown>).record : undefined;
            expect(forkSessionLineage(seedRecord, sessionRecord)).toBe(true);
          }
        } catch (error) {
          if (!routeEvidenceRecorded) {
            let failureEvidence: unknown;
            let failureEvidenceError: string | undefined;
            if (candidate.evidence) {
              try { failureEvidence = await candidate.evidence(); }
              catch (evidenceError) { failureEvidenceError = String(evidenceError); }
            }
            (receipt.routes_evidence as unknown[]).push({
              route,
              identity: participant.identity,
              preflight,
              failure: String(error),
              failure_phase: "prompt_or_validation",
              ...(failureEvidence === undefined ? {} : { evidence: failureEvidence }),
              ...(failureEvidenceError === undefined ? {} : { evidence_error: failureEvidenceError }),
            });
          }
          throw error;
        } finally {
          const cleanup = await closeWithDeadline(participant.close);
          const ownedProcessIds = ownedParticipant.processIds();
          const remaining = ownedProcessIds.filter(processIsAlive);
          const failureHistory = ownedParticipant.failureHistory();
          let finalEvidence: unknown;
          let finalEvidenceError: string | undefined;
          if (route === "opencode-run" && candidate.evidence) {
            try { finalEvidence = await candidate.evidence(); }
            catch (error) { finalEvidenceError = String(error); }
          }
          state.value = cleanup.completed && remaining.length === 0 && failureHistory.length === 0;
          (receipt.cleanup as unknown[]).push({ route, ...cleanup, owned_process_ids: ownedProcessIds, owned_processes_remaining: remaining, failure_history: failureHistory, ...(finalEvidence === undefined ? {} : { final_evidence: finalEvidence }), ...(finalEvidenceError === undefined ? {} : { final_evidence_error: finalEvidenceError }), completed: state.value });
          (receipt.failure_history as unknown[]).push(...failureHistory);
        }
      }
    } catch (error) {
      receipt.error = String(error);
      const failureEvidence = error && typeof error === "object" && "evidence" in error
        ? (error as { evidence?: unknown }).evidence : undefined;
      if (failureEvidence !== undefined) receipt.failure_evidence = failureEvidence;
      throw error;
    } finally {
      for (const participant of participants.filter((item) => !item.closed.value)) {
        const cleanup = await closeWithDeadline(participant.close);
        const ownedProcessIds = participant.processIds();
        const remaining = ownedProcessIds.filter(processIsAlive);
        const failureHistory = participant.failureHistory();
        (receipt.cleanup as unknown[]).push({ route: participant.route, late_cleanup: true, ...cleanup, owned_process_ids: ownedProcessIds, owned_processes_remaining: remaining, failure_history: failureHistory, completed: cleanup.completed && remaining.length === 0 && failureHistory.length === 0 });
        (receipt.failure_history as unknown[]).push(...failureHistory);
      }
      if (owned) await owned.close().catch((error) => { receipt.redis_cleanup_error = String(error); (receipt.failure_history as unknown[]).push(`redis: ${String(error)}`); });
      receipt.source_hashes_after = await sourceHashes();
      receipt.dependency_snapshot_after = await dependencySnapshot();
      const sourcesMatch = JSON.stringify(receipt.source_hashes_before) === JSON.stringify(receipt.source_hashes_after);
      const dependenciesMatch = JSON.stringify(receipt.dependency_snapshot_before) === JSON.stringify(receipt.dependency_snapshot_after);
      const cleanupComplete = (receipt.cleanup as Array<{ completed: boolean }>).every((item) => item.completed) && receipt.redis_cleanup_error === undefined;
      receipt.passed = receipt.error === undefined && sourcesMatch && dependenciesMatch && cleanupComplete && (receipt.routes_evidence as unknown[]).length === routes.length;
      receipt.finished_at = new Date().toISOString();
      receipt.receipt_sha256 = valueHash(receipt);
      writeRepairReceipt(runID, receipt);
      if (!sourcesMatch || !dependenciesMatch || !cleanupComplete) throw new Error("OpenCode fork/resume source or dependency verification or cleanup failed");
    }
  }, overallTimeoutMs + 90_000);
});
