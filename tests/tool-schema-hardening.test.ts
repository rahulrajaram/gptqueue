import { describe, expect, it } from "vitest";
import { receiveMessageSchema } from "../src/mcp-server/tools/receive-message.js";
import { sendMessageSchema } from "../src/mcp-server/tools/send-message.js";
import { stableToolError } from "../src/mcp-server/tool-result.js";
import { GPTQUEUE_INSTRUCTIONS } from "../src/transports/setup-tools.js";

describe("MCP contract hardening", () => {
  it("bounds blocking receive timeouts", () => {
    expect(receiveMessageSchema.safeParse({ timeout: 0 }).success).toBe(true);
    expect(receiveMessageSchema.safeParse({ timeout: 60 }).success).toBe(true);
    expect(receiveMessageSchema.safeParse({ timeout: -1 }).success).toBe(false);
    expect(receiveMessageSchema.safeParse({ timeout: 61 }).success).toBe(false);
    expect(receiveMessageSchema.safeParse({ timeout: 1.5 }).success).toBe(false);
  });

  it("bounds retry idempotency keys", () => {
    const base = { to: "agent", content: "hello" };
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "retry-1" }).success).toBe(true);
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "" }).success).toBe(false);
    expect(sendMessageSchema.safeParse({ ...base, idempotency_key: "x".repeat(129) }).success).toBe(false);
  });

  it("returns stable unavailable errors as structured content", () => {
    const result = stableToolError(new Error("Session abc not found in Redis."));
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      status: "error",
      error: { code: "SESSION_UNAVAILABLE", retryable: false },
    });
  });

  it("places coordination-plane policy in server instructions", () => {
    expect(GPTQUEUE_INSTRUCTIONS).toContain("instead of native");
    expect(GPTQUEUE_INSTRUCTIONS).toContain("idempotency_key");
  });
});
