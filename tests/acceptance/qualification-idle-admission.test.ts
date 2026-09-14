import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { adjudicateIdleEvidence, admitIdleTaskCheckpoint, trustedPath, trustedSourceLabel } from "./qualification-idle-admission.js";
import type { RawEvidenceRef } from "./qualification-types.js";
import { frozenDimensionObligations, frozenPairMatrix, frozenRoutes } from "./qualification-routes.js";

type Json = Record<string, unknown>;
const object = (value: unknown): Json => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
const enabled = process.env.GPTQUEUE_IDLE_TASK_CHECKPOINT === "1";
const configured = (name: "GPTQUEUE_IDLE_TASK_RECEIPT" | "GPTQUEUE_IDLE_TASK_BASE"): string => {
  const value = process.env[name]; if (!value) throw new Error(`${name} is required when GPTQUEUE_IDLE_TASK_CHECKPOINT=1`); return resolve(value);
};
const ref = (value: unknown): RawEvidenceRef => {
  const row = object(value);
  for (const key of ["path", "sha256", "sourceRevision", "oracleRevision"]) {
    if (typeof row[key] !== "string" || !row[key]) throw new Error(`missing raw reference ${key}`);
  }
  return row as RawEvidenceRef;
};
const sha256 = (value: Uint8Array): string => createHash("sha256").update(value).digest("hex");
const trustFor = (receiptPath: string) => ({ repoRoot: resolve("."), evidenceRoot: dirname(resolve(receiptPath)), approvedExecutables: { node: process.execPath, codex: process.env.GPTQUEUE_IDLE_TASK_CODEX_EXECUTABLE ?? "/home/rahul/.local/bin/codex" } });
const fixture = async () => {
  const receiptPath = configured("GPTQUEUE_IDLE_TASK_RECEIPT");
  const receipt = JSON.parse(await readFile(receiptPath, "utf8")) as Json;
  const baseline = object(receipt.baseline), proof = object(receipt.proof);
  const baselineModelRef = ref(object(baseline.binding).rawHistoryRef), baselinePeerRef = ref(object(object(baseline.discovery).peer_to_model).rawHistoryRef), modelRef = ref(proof.model_history), peerRef = ref(proof.peer_history);
  const read = async (item: RawEvidenceRef): Promise<unknown> => JSON.parse(await readFile(resolve(item.path), "utf8"));
  return { receipt, baselineModel: await read(baselineModelRef), baselinePeer: await read(baselinePeerRef), finalModel: await read(modelRef), finalPeer: await read(peerRef), modelRef, peerRef, baselineModelRef, baselinePeerRef };
};

describe.skipIf(!enabled)("offline native idle-task admission (retained evidence gate)", () => {
  it("re-adjudicates the retained receipt through current collectors and oracle", async () => {
    const result = await adjudicateIdleEvidence(await fixture());
    expect(result.attempt.rowId).toBe("codex-appserver:automatic:idle-task");
    expect(result.attempt.outcome).toBe("meets");
  });

  it("rejects a wrong obligation row", async () => {
    const input = await fixture(); input.receipt.row_id = "codex-appserver:initiative:1";
    await expect(adjudicateIdleEvidence(input)).rejects.toThrow(/row_id/);
  });

  it("rejects a boundary that turns the controller setup prompt into post-stimulus activity", async () => {
    const input = await fixture(); object(input.receipt.proof).boundary = 0;
    await expect(adjudicateIdleEvidence(input)).rejects.toThrow(/boundary|stimulus/iu);
  });

  it("rejects a final history with the native acknowledgement removed", async () => {
    const input = await fixture();
    const finalModel = object(input.finalModel);
    input.finalModel = { ...finalModel, turns: Array.isArray(finalModel.turns) ? finalModel.turns.map(turn => { const row = object(turn); return { ...row, items: Array.isArray(row.items) ? row.items.filter(item => object(item).tool !== "acknowledge_tasks") : row.items }; }) : finalModel.turns };
    await expect(adjudicateIdleEvidence(input)).rejects.toThrow(/acknowledgement|acknowledge/iu);
  });

  it("rejects a controller prompt after the actual stimulus", async () => {
    const input = await fixture();
    const events = input.receipt.controller_events as unknown[];
    events.push({ kind: "controller_prompt", at: 4 });
    await expect(adjudicateIdleEvidence(input)).rejects.toThrow(/controller prompt/);
  });

  it("rejects a source mismatch before writing a checkpoint", async () => {
    const input = await fixture(), directory = await mkdtemp(join(tmpdir(), "gptqueue-idle-admission-"));
    try {
      for (const key of ["source_hashes_before", "source_hashes_after"]) object(input.receipt[key])["tests/acceptance/oracle.ts"] = "0".repeat(64);
      const path = join(directory, "altered-receipt.json");
      await writeFile(path, JSON.stringify(input.receipt), { mode: 0o600 });
      await expect(admitIdleTaskCheckpoint(configured("GPTQUEUE_IDLE_TASK_BASE"), path, join(directory, "output"), trustFor(path))).rejects.toThrow(/idle source changed/);
    } finally { await rm(directory, { recursive: true, force: true }); }
  });

  it("rejects outside, traversal, symlink-escape, and unapproved executable paths", async () => {
    const directory = await mkdtemp(join(tmpdir(), "gptqueue-idle-trust-")), root = join(directory, "root"), outside = join(directory, "outside.txt"), inside = join(root, "inside.txt"), escaped = join(root, "escaped.txt");
    try {
      await writeFile(outside, "outside"); await mkdir(root, { recursive: true }); await writeFile(inside, "inside"); await symlink(outside, escaped);
      await expect(trustedPath(outside, root, "outside")).rejects.toThrow(/escapes/);
      await expect(trustedPath(escaped, root, "symlink")).rejects.toThrow(/escapes/);
      await expect(trustedSourceLabel("../outside.txt", root)).rejects.toThrow(/normalized/);
      await expect(trustedSourceLabel("/etc/passwd", root)).rejects.toThrow(/normalized/);
      const receiptPath = configured("GPTQUEUE_IDLE_TASK_RECEIPT");
      await expect(admitIdleTaskCheckpoint(configured("GPTQUEUE_IDLE_TASK_BASE"), receiptPath, join(directory, "output"), { ...trustFor(receiptPath), approvedExecutables: { node: process.execPath, codex: "/bin/sh" } })).rejects.toThrow(/codex_executable/);
    } finally { await rm(directory, { recursive: true, force: true }); }
  });

  it.each([false, true])("rejects an escaped raw history through the full loader (symlink=%s)", async useSymlink => {
    const input = await fixture(), directory = await mkdtemp(join(tmpdir(), "gptqueue-idle-raw-"));
    const evidenceRoot = join(directory, "evidence");
    try {
      await mkdir(evidenceRoot);
      const baseline = object(input.receipt.baseline), proof = object(input.receipt.proof);
      const refs = [object(baseline.binding).rawHistoryRef, object(object(baseline.discovery).peer_to_model).rawHistoryRef, proof.model_history, proof.peer_history].map(object);
      for (const [index, reference] of refs.entries()) {
        const bytes = await readFile(String(reference.path)), path = join(evidenceRoot, `history-${index}.json`);
        await writeFile(path, bytes, { mode: 0o600 });
        reference.path = path;
      }
      const modelRef = object(proof.model_history), outside = join(directory, "outside.json");
      await writeFile(outside, await readFile(String(modelRef.path)), { mode: 0o600 });
      if (useSymlink) {
        const link = join(evidenceRoot, "escaped.json");
        await symlink(outside, link);
        modelRef.path = link;
      } else modelRef.path = outside;
      const path = join(evidenceRoot, "receipt.json");
      await writeFile(path, JSON.stringify(input.receipt), { mode: 0o600 });
      // Bytes and hashes still match: rejection must come from path ownership.
      await expect(admitIdleTaskCheckpoint(configured("GPTQUEUE_IDLE_TASK_BASE"), path, join(directory, "output"), trustFor(path))).rejects.toThrow(/raw evidence escapes trusted root/);
    } finally { await rm(directory, { recursive: true, force: true }); }
  });

  it("writes only an immutable derived checkpoint when explicit paths are supplied", async () => {
    const basePath = configured("GPTQUEUE_IDLE_TASK_BASE");
    const receiptPath = configured("GPTQUEUE_IDLE_TASK_RECEIPT");
    const outputRoot = resolve(process.env.GPTQUEUE_IDLE_TASK_OUTPUT ?? `${resolve(".gptqueue/repair-qualification/20260912/checkpoints")}/idle-task-admissions`);
    const baseBefore = await readFile(basePath), receiptBefore = await readFile(receiptPath);
    const output = await admitIdleTaskCheckpoint(basePath, receiptPath, outputRoot, trustFor(receiptPath));
    const canonicalBasePath = await realpath(basePath), canonicalReceiptPath = await realpath(receiptPath);
    const receiptRunId = String(object(JSON.parse(receiptBefore.toString("utf8"))).run_id);
    const outputRelative = relative(outputRoot, output);
    expect(outputRelative.length).toBeGreaterThan(0);
    expect(outputRelative.startsWith(".."), outputRelative).toBe(false);
    expect(outputRelative.startsWith("/"), outputRelative).toBe(false);
    expect(output).not.toBe(basePath);
    expect(output).not.toBe(receiptPath);

    const emitted = JSON.parse(await readFile(output, "utf8")) as Json;
    const markdownPath = resolve(output, "../checkpoint.md"), markdown = await readFile(markdownPath, "utf8");
    const outputStat = await stat(output), markdownStat = await stat(markdownPath);
    expect(outputStat.mode & 0o777).toBe(0o600);
    expect(markdownStat.mode & 0o777).toBe(0o600);
    expect(emitted.kind).toBe("offline-idle-task-admission-checkpoint");
    expect(markdown).toContain("codex-appserver:automatic:idle-task");

    const contract = object(emitted.contract);
    expect(contract).toEqual({ routeCount: frozenRoutes.length, pairCount: frozenPairMatrix.length, dimensionObligationCount: frozenDimensionObligations.length, requiredDimensionObligations: frozenDimensionObligations.filter(row => row.required).length });
    const admission = object(emitted.idleTaskAdmission), attempt = object(admission.attempt);
    expect(attempt).toMatchObject({ kind: "automatic_tasks", rowId: "codex-appserver:automatic:idle-task", route: "codex-appserver", status: "completed", outcome: "meets" });
    expect(admission.admittedAttemptIds).toEqual([attempt.attemptId]);
    expect(emitted.baseCheckpoint).toMatchObject({ path: canonicalBasePath, sha256: sha256(baseBefore), preserved: true });
    expect(admission.receipt).toBe(canonicalReceiptPath);
    expect(admission.receiptSha256).toBe(sha256(receiptBefore));
    expect(object(admission.provenance).receiptRunId).toBe(receiptRunId);
    expect(attempt.attemptId).toBe(`native-idle-task-${receiptRunId}`);
    expect(object(admission.trust)).toMatchObject({ repoRoot: await realpath(resolve(".")), evidenceRoot: await realpath(dirname(receiptPath)), approvedExecutables: { node: await realpath(process.execPath), codex: await realpath(process.env.GPTQUEUE_IDLE_TASK_CODEX_EXECUTABLE ?? "/home/rahul/.local/bin/codex") } });

    const base = JSON.parse(baseBefore.toString("utf8")) as Json;
    const baseReport = object(base.report), emittedReport = object(emitted.report);
    const baseRows = Array.isArray(baseReport.rows) ? baseReport.rows.map(object) : [], emittedRows = Array.isArray(emittedReport.rows) ? emittedReport.rows.map(object) : [];
    expect(emittedRows).toHaveLength(875);
    expect(emittedRows.filter(row => row.id !== "codex-appserver:automatic:idle-task")).toEqual(baseRows.filter(row => row.id !== "codex-appserver:automatic:idle-task"));
    expect(emittedRows.filter(row => frozenPairMatrix.some(pair => pair.pairId === row.id))).toEqual(baseRows.filter(row => frozenPairMatrix.some(pair => pair.pairId === row.id)));
    expect(emittedRows.find(row => row.id === "codex-appserver:automatic:idle-task")).toMatchObject({ id: "codex-appserver:automatic:idle-task", required: true, status: "completed", outcome: "meets", attemptId: attempt.attemptId });
    expect(emittedReport.counts).toEqual({ completed: 26, failed: 0, blocked: 141, setup_gap: 459, unrun: 249 });
    expect(object(emitted.counts)).toMatchObject({ status: emittedReport.counts, total: "uncertain" });
    expect(object(emittedReport.total).outcome).toBe("uncertain");
    expect(await readFile(basePath)).toEqual(baseBefore);
    expect(await readFile(receiptPath)).toEqual(receiptBefore);
  });
});
