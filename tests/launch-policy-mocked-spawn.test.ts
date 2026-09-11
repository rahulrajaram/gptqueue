/**
 * F1 mocked-spawn refusal matrix: asserts that `dispatchLaunch` NEVER reaches
 * child_process.spawn for the attack shapes called out by the review —
 * interpreter inline-code (`node -e`, `python -c`), basename-colliding
 * absolute paths, leading-dash args beyond the template, v1 documents, and
 * template mismatches — and that an approved contract is spawned verbatim.
 *
 * child_process is mocked file-wide; real-spawn coverage lives in
 * launch-policy.test.ts / wake-e2e.test.ts.
 */
import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("child_process", () => ({ spawn: vi.fn() }));

import { spawn as mockedSpawn } from "child_process";
import { dispatchLaunch } from "../src/mcp-server/launcher.js";
import { evaluateLaunchPolicy } from "../src/core/launch-policy.js";
import { scaffoldLaunchAllowlist } from "./helpers/launch-allowlist.js";

const spawnMock = vi.mocked(mockedSpawn);

/** A fake ChildProcess good enough for dispatchLaunch's contract. */
function fakeChild(): {
  child: {
    pid: number;
    once: (event: string, cb: () => void) => void;
    unref: () => void;
  };
  emitSpawn: () => void;
} {
  const listeners: Record<string, () => void> = {};
  return {
    child: {
      pid: 4242,
      once: (event, cb) => {
        listeners[event] = cb;
      },
      unref: () => {},
    },
    emitSpawn: () => listeners["spawn"]?.(),
  };
}

const setup = (
  commands: Parameters<typeof scaffoldLaunchAllowlist>[0]
): ReturnType<typeof scaffoldLaunchAllowlist> => {
  const s = scaffoldLaunchAllowlist(commands);
  s.set();
  return s;
};

describe("dispatchLaunch mocked-spawn refusal matrix (F1)", () => {
  beforeEach(() => {
    spawnMock.mockReset();
  });

  it("never spawns `node -e <payload>` — even when exactly allowlisted", async () => {
    const s = setup([
      { command: "node", allowed_args: [["-e", "process.exit(1)"], ["server.js"]] },
    ]);
    try {
      const res = await dispatchLaunch({
        command: "node",
        args: ["-e", "process.exit(1)"],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.code).toBe("launch_failed");
      expect(res.error?.message).toMatch(/inline-code/);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns `python -c <payload>` — even when exactly allowlisted", async () => {
    const s = setup([
      { command: "python3", allowed_args: [["-c", "import os"], []] },
    ]);
    try {
      const res = await dispatchLaunch({
        command: "python3",
        args: ["-c", "import os"],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.message).toMatch(/inline-code/);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns a basename-colliding absolute path against a bare allowlist entry", async () => {
    const s = setup([{ command: "node", allowed_args: [[]] }]);
    try {
      const res = await dispatchLaunch({
        command: "/attacker/work/node",
        args: [],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.message).toMatch(/not permitted/);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns a bare-name request against an absolute allowlist entry", async () => {
    const s = setup([{ command: "/usr/bin/node", allowed_args: [["server.js"]] }]);
    try {
      const res = await dispatchLaunch({
        command: "node",
        args: ["server.js"],
      });
      expect(res.dispatched).toBe(false);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns when trailing leading-dash args extend an exact template", async () => {
    const s = setup([{ command: "node", allowed_args: [["--port", "1234"]] }]);
    try {
      const res = await dispatchLaunch({
        command: "node",
        args: ["--port", "1234", "-x"],
      });
      expect(res.dispatched).toBe(false);
      expect(res.error?.message).toMatch(/not permitted/);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns when the args are shorter than the template or a value differs", async () => {
    const s = setup([{ command: "node", allowed_args: [["--agent", "alice"]] }]);
    try {
      const shorter = await dispatchLaunch({
        command: "node",
        args: ["--agent"],
      });
      expect(shorter.dispatched).toBe(false);
      expect(spawnMock).not.toHaveBeenCalled();

      const wrong = await dispatchLaunch({
        command: "node",
        args: ["--agent", "bob"],
      });
      expect(wrong.dispatched).toBe(false);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("never spawns when the allowlist is a version-1 document (fail closed)", async () => {
    const s = setup([{ command: "node", allowed_args: [[]] }]);
    try {
      const { writeFileSync } = await import("fs");
      writeFileSync(
        s.path,
        JSON.stringify({
          version: 1,
          commands: [{ command: "node", allowed_args_prefixes: [[]] }],
        })
      );
      const res = await dispatchLaunch({ command: "node", args: [] });
      expect(res.dispatched).toBe(false);
      expect(res.error?.message).toMatch(/version-1/);
      expect(spawnMock).not.toHaveBeenCalled();
    } finally {
      s.cleanup();
    }
  });

  it("spawns an approved contract exactly once, verbatim (command + args, no shell)", async () => {
    const s = setup([
      { command: "/usr/bin/pi", allowed_args: [["--agent", "alice"]] },
    ]);
    try {
      expect(await evaluateLaunchPolicy({ command: "/usr/bin/pi", args: ["--agent", "alice"] })).toEqual({
        ok: true,
      });
      const fake = fakeChild();
      spawnMock.mockReturnValue(fake.child as never);
      const pending = dispatchLaunch({
        command: "/usr/bin/pi",
        args: ["--agent", "alice"],
        cwd: process.cwd(),
      });
      // dispatchLaunch awaits the (async) policy check before registering the
      // child listeners, so wait for the spawn call before emitting.
      await vi.waitFor(() => expect(spawnMock).toHaveBeenCalled());
      fake.emitSpawn();
      const res = await pending;
      expect(res).toEqual({ dispatched: true, pid: 4242 });
      expect(spawnMock).toHaveBeenCalledTimes(1);
      expect(spawnMock.mock.calls[0]?.[0]).toBe("/usr/bin/pi");
      expect(spawnMock.mock.calls[0]?.[1]).toEqual(["--agent", "alice"]);
      const opts = spawnMock.mock.calls[0]?.[2] as { shell?: boolean };
      expect(opts.shell).toBeUndefined();
    } finally {
      s.cleanup();
    }
  });
});
