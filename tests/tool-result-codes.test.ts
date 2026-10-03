import { describe, expect, it } from "vitest";
import { stableToolError } from "../src/mcp-server/tool-result.js";
import { GptQueueError } from "../src/mcp-server/errors.js";

const codeOf = (error: unknown) =>
  (stableToolError(error).structuredContent as { error: { code: string; retryable: boolean } }).error;

describe("stableToolError classification", () => {
  it("uses a GptQueueError's code regardless of its wording", () => {
    expect(codeOf(new GptQueueError("SESSION_UNAVAILABLE", "reworded: no live binding"))).toMatchObject({ code: "SESSION_UNAVAILABLE", retryable: false });
    expect(codeOf(new GptQueueError("AGENT_NOT_REGISTERED", "reworded: please sign in"))).toMatchObject({ code: "AGENT_NOT_REGISTERED" });
  });

  it("falls back to message patterns for dependency errors", () => {
    expect(codeOf(new Error("connect ECONNREFUSED 127.0.0.1:6379"))).toMatchObject({ code: "REDIS_UNAVAILABLE", retryable: true });
    expect(codeOf(new Error("something else"))).toMatchObject({ code: "GPTQUEUE_ERROR" });
  });
});
