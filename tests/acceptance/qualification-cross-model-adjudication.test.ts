import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { requireRetained } from "./retained-evidence.js";

type Adjudication = Readonly<{
  original_receipt: Readonly<{ path: string; sha256: string; immutable: boolean; execution_status: string; execution_detail: string }>;
  source_freeze: Readonly<{ before: Record<string, string>; after: Record<string, string>; before_at: string; after_at: string; unchanged: boolean; admitted_from_receipt: boolean }>;
  cleanup: readonly Readonly<{ status: string }>[];
  retained_histories: readonly Readonly<{ path: string; sha256: string; sanitized: boolean }>[];
  recovered_exchange: Readonly<{ request_message_id: string; reply_message_id: string; request_claim_id: string; reply_claim_id: string; recovered_after_failed_attempt: Readonly<{ successful: boolean; excluded_from_binding: boolean }> }>;
  oracle_decision: Readonly<{ verdict: string; native_execution_claim: boolean; original_timeout_preserved: boolean }>;
  admission: Readonly<{ source_hashes_match: boolean; cleanup_all_fulfilled: boolean; raw_histories_present: boolean; native_execution_claim: boolean }>;
}>;

const digest = (path: string): string => createHash("sha256").update(readFileSync(path)).digest("hex");

describe("offline cross-model adjudication", () => {
  it("admits only the recovered exact chain while preserving the timed-out receipt", (ctx) => {
    const root = join(process.cwd(), ".gptqueue/repair-qualification/20260912/qualification-cross-model/8a903b8f-0638-46d6-b44a-269dfccad35a");
    const artifactPath = join(root, "offline-adjudication.json");
    requireRetained(ctx, artifactPath);
    const artifact = JSON.parse(readFileSync(artifactPath, "utf8")) as Adjudication;
    const receiptPath = join(process.cwd(), artifact.original_receipt.path);
    expect(artifact.original_receipt.immutable).toBe(true);
    expect(artifact.original_receipt.execution_status).toBe("failed");
    expect(artifact.original_receipt.execution_detail).toContain("180000ms deadline");
    expect(digest(receiptPath)).toBe(artifact.original_receipt.sha256);
    expect(artifact.source_freeze.unchanged).toBe(true);
    expect(artifact.source_freeze.admitted_from_receipt).toBe(true);
    expect(artifact.source_freeze.before).toEqual(artifact.source_freeze.after);
    expect(artifact.source_freeze.before_at).toBe("2026-09-12T21:50:46.157Z");
    expect(artifact.source_freeze.after_at).toBe("2026-09-12T21:54:21.308Z");
    expect(artifact.cleanup.every((entry) => entry.status === "fulfilled")).toBe(true);
    expect(artifact.retained_histories.every((history) => history.sanitized && existsSync(history.path) && digest(history.path) === history.sha256)).toBe(true);
    expect(artifact.recovered_exchange.request_message_id).toBe("786a39a3-2b42-4d5e-8c49-d8dcc0baf679");
    expect(artifact.recovered_exchange.reply_message_id).toBe("c32adcbd-9856-4b16-b0a6-16026f35c1ee");
    expect(artifact.recovered_exchange.request_claim_id).toBe("36dc0960-3667-48ff-8ee8-bb0b63d2e5cb");
    expect(artifact.recovered_exchange.reply_claim_id).toBe("59b7d82d-fb2f-4888-948b-b2fe3d6b846b");
    expect(artifact.recovered_exchange.recovered_after_failed_attempt).toMatchObject({ successful: false, excluded_from_binding: true });
    expect(artifact.oracle_decision).toMatchObject({ verdict: "recovered_exact_exchange", native_execution_claim: false, original_timeout_preserved: true });
    expect(artifact.admission).toEqual({ source_hashes_match: true, cleanup_all_fulfilled: true, raw_histories_present: true, native_execution_claim: false });
  });
});
