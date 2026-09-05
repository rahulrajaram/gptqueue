import { describe, expect, it } from "vitest";
import { activationOperationId, inboxPrompt, validateRuntimeBinding, runtimeBindingSchema } from "../src/registered-shell/runtime.js";

const binding = (overrides: Record<string, unknown> = {}) => ({
  client: "codex", runtime_id: "runtime-1", epoch: "epoch-1", working_directory: "/workspace/project", ...overrides,
});

describe("runtime binding contract", () => {
  it("accepts the exact host identity and rejects client or directory mismatches", () => {
    expect(validateRuntimeBinding(binding(), { client: "codex", working_directory: "/workspace/project" }).ok).toBe(true);
    expect(validateRuntimeBinding(binding({ client: "pi" }), { client: "codex", working_directory: "/workspace/project" })).toEqual({ ok: false, code: "runtime_binding_mismatch" });
    expect(validateRuntimeBinding(binding({ working_directory: "/workspace/other" }), { client: "codex", working_directory: "/workspace/project" })).toEqual({ ok: false, code: "runtime_binding_mismatch" });
  });

  it("rejects malformed and extra binding fields", () => {
    expect(runtimeBindingSchema.safeParse(binding({ working_directory: "relative" })).success).toBe(false);
    expect(runtimeBindingSchema.safeParse(binding({ extra: "secret" })).success).toBe(false);
    expect(runtimeBindingSchema.safeParse(binding({ runtime_id: "" })).success).toBe(false);
  });

  it("makes operation identity order-independent but attempt-sensitive", () => {
    const parsed = runtimeBindingSchema.parse(binding());
    expect(activationOperationId("agent", parsed, ["b", "a"], 1)).toBe(activationOperationId("agent", parsed, ["a", "b"], 1));
    expect(activationOperationId("agent", parsed, ["a", "b"], 2)).not.toBe(activationOperationId("agent", parsed, ["a", "b"], 1));
    expect(inboxPrompt("agent", "operation")).toContain("claim_tasks");
    expect(inboxPrompt("agent", "operation")).toContain("Do not acknowledge unfinished work");
  });
});
