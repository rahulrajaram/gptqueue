import { describe, expect, it } from "vitest";
import { createPiNativeChildAdapter, enterPiNativeChildProfileScope } from "./qualification-pi-child.js";

describe("Pi native child owned profile scope", () => {
  it("keeps the owned profile through delayed work and restores an unset environment", async () => {
    const prior = process.env.PI_CODING_AGENT_DIR;
    delete process.env.PI_CODING_AGENT_DIR;
    let release: (() => void) | undefined;
    try {
      release = enterPiNativeChildProfileScope("/tmp/pi-owned-profile");
      const observed = await new Promise<string | undefined>(resolve => setTimeout(() => resolve(process.env.PI_CODING_AGENT_DIR), 0));
      expect(observed).toBe("/tmp/pi-owned-profile");
      release(); release = undefined;
      expect(process.env.PI_CODING_AGENT_DIR).toBeUndefined();
    } finally {
      release?.();
      if (prior === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = prior;
    }
  });

  it("rejects overlapping scopes and restores an existing environment", () => {
    const prior = process.env.PI_CODING_AGENT_DIR;
    process.env.PI_CODING_AGENT_DIR = "/tmp/original-profile";
    let release: (() => void) | undefined;
    try {
      release = enterPiNativeChildProfileScope("/tmp/pi-owned-profile");
      expect(() => enterPiNativeChildProfileScope("/tmp/other-profile")).toThrow(/already active/);
      release(); release = undefined;
      expect(process.env.PI_CODING_AGENT_DIR).toBe("/tmp/original-profile");
    } finally {
      release?.();
      if (prior === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = prior;
    }
  });

  it("does not let a stale release clear a newer scope", () => {
    const first = enterPiNativeChildProfileScope("/tmp/first-profile");
    first();
    const second = enterPiNativeChildProfileScope("/tmp/second-profile");
    try {
      expect(() => first()).toThrow(/already released/);
      expect(process.env.PI_CODING_AGENT_DIR).toBe("/tmp/second-profile");
    } finally { second(); }
  });

  it("rejects a spoofed matching environment without an active owned scope", async () => {
    const prior = process.env.PI_CODING_AGENT_DIR;
    process.env.PI_CODING_AGENT_DIR = "/tmp/spoof";
    try {
      const adapter = createPiNativeChildAdapter({
        installedRoot: "/does/not/load", provider: "unused", model: "unused",
        profileRoot: "/tmp/spoof", modelRuntime: {},
      });
      await expect(adapter.launch({ role: "sender", pairId: "pair", nonce: "nonce", redisUrl: "redis://127.0.0.1:6379/15" }, new AbortController().signal))
        .rejects.toThrow(/active owned profile scope/);
    } finally {
      if (prior === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = prior;
    }
  });
});
