import { describe, expect, it } from "vitest";
import { classifyPiNativeChildStatus, createPiNativeChildAdapter, decodePiNativeChildControl, encodePiNativeChildControl, piNativeChildQualificationRoute } from "./qualification-pi-child.js";

describe("Pi native-child qualification adapter", () => {
  it("declares a distinct native Agent route", () => {
    const adapter = createPiNativeChildAdapter({ installedRoot: "/does/not/launch" });
    expect(adapter.spec.id).toBe(piNativeChildQualificationRoute);
    expect(adapter.spec.host).toBe("pi");
    expect(adapter.spec.modelBacked).toBe(true);
  });

  it("preflights the installed SDK and native Agent extension", async () => {
    await expect(createPiNativeChildAdapter({ installedRoot: "/does/not/launch" }).preflight(new AbortController().signal)).resolves.toEqual({
      kind: "setup_gap",
      detail: "Pi SDK, installed pi-subagents Agent tool, or built GPTQueue extension is unavailable",
    });
  });

  it("keeps controller work distinguishable from peer traffic", () => {
    const encoded = encodePiNativeChildControl("send the requested peer task and report its result");
    expect(decodePiNativeChildControl(encoded)).toEqual({ kind: "controller_prompt", prompt: "send the requested peer task and report its result" });
    expect(decodePiNativeChildControl("ordinary peer task")).toBeUndefined();
  });

  it("never infers idle from an active native child", () => {
    expect(classifyPiNativeChildStatus({ status: "running" }, false, "child-session").kind).toBe("unknown");
    expect(classifyPiNativeChildStatus({ status: "idle" }, false, "child-session")).toEqual({ kind: "idle", runtimeId: "child-session" });
    expect(classifyPiNativeChildStatus({ status: "running" }, true, "child-session")).toEqual({ kind: "terminated", runtimeId: "child-session" });
  });

});
