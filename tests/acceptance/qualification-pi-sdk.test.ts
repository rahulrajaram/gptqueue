import { describe, expect, it } from "vitest";
import { createPiSdkAdapter, piSdkQualificationRoute } from "./qualification-pi-sdk.js";

describe("Pi SDK qualification adapter", () => {
  it("keeps the SDK route distinct from RPC", () => {
    const adapter = createPiSdkAdapter({ installedRoot: "/does/not/launch" });
    expect(adapter.spec.id).toBe(piSdkQualificationRoute);
    expect(adapter.spec.host).toBe("pi");
    expect(adapter.spec.modelBacked).toBe(true);
  });

  it("reports an unavailable installed SDK through preflight without launching", async () => {
    const adapter = createPiSdkAdapter({ installedRoot: "/does/not/launch" });
    await expect(adapter.preflight(new AbortController().signal)).resolves.toEqual({
      kind: "setup_gap",
      detail: "Installed Pi SDK, GPTQueue source extension, or existing Pi auth is unavailable",
    });
  });
});

