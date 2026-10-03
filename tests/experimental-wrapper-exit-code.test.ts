import { describe, expect, it } from "vitest";
import { exitCodeFor, finalExitCode } from "../src/experimental-wrapper/index.js";

describe("experimental wrapper exit-code precedence", () => {
  const sigint = exitCodeFor({ code: null, signal: "SIGINT" });

  it("passes the child's code through when cleanup succeeded and nothing interrupted", () => {
    expect(finalExitCode(0, null, false)).toBe(0);
    expect(finalExitCode(3, null, false)).toBe(3);
  });

  it("reports the interrupt even when the child exited cleanly", () => {
    expect(finalExitCode(0, "SIGINT", false)).toBe(sigint);
    expect(finalExitCode(7, "SIGINT", false)).toBe(7);
  });

  it("lets a cleanup failure override a clean exit", () => {
    expect(finalExitCode(0, null, true)).toBe(1);
    expect(finalExitCode(0, "SIGINT", true)).toBe(sigint);
  });
});
