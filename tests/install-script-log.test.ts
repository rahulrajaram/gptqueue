import { afterAll, describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  lstatSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

/**
 * GQR-1 regression: the HTTP server log must live in a per-user 0700 state
 * directory as a 0600 file, and a symlinked log path must be refused instead
 * of appended through. The real install.sh runs with shimmed npm/ss/curl/node
 * so no build, port bind or server start ever happens; the fake node prints a
 * marker that lands in the log via the script's append redirect.
 */
const SCRIPT = resolve(import.meta.dirname, "../scripts/install.sh");
const LEGACY_TMP_LOG = "/tmp/gptqueue-http.log";
const MARKER = "fake-server-line";

const root = mkdtempSync(join(tmpdir(), "gptq-install-log-"));
afterAll(() => rmSync(root, { recursive: true, force: true }));

const bin = join(root, "bin");
const home = join(root, "home");
mkdirSync(bin);
mkdirSync(home);
const shim = (name: string, body: string): void => {
  writeFileSync(join(bin, name), `#!/bin/sh\n${body}\n`, { mode: 0o755 });
  chmodSync(join(bin, name), 0o755);
};
shim("npm", "exit 0");
shim("ss", "exit 0");
shim("curl", "exit 0");
shim("node", `echo ${MARKER}; exit 0`);

const run = (env: Record<string, string> = {}) =>
  spawnSync("bash", [SCRIPT], {
    env: { PATH: `${bin}:${process.env.PATH ?? ""}`, HOME: home, ...env },
    encoding: "utf8",
  });

const mode = (path: string): number => lstatSync(path).mode & 0o777;

/** Poll (bounded) for the detached fake server's marker to reach the log. */
const waitForMarker = async (path: string): Promise<string> => {
  for (let i = 0; i < 80; i++) {
    try {
      const text = readFileSync(path, "utf8");
      if (text.includes(MARKER)) return text;
    } catch {
      // not created yet
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`marker never reached ${path}`);
};

const exists = (path: string): boolean => {
  try {
    lstatSync(path);
    return true;
  } catch {
    return false;
  }
};

describe("install.sh server log placement", () => {
  it("defaults the log to a 0700 per-user state dir as a 0600 file, not /tmp", async () => {
    const legacyBefore = exists(LEGACY_TMP_LOG);
    try {
      const res = run();
      expect(res.status).toBe(0);

      const stateDir = join(home, ".local", "state", "gptqueue");
      const log = join(stateDir, "http.log");
      expect(exists(stateDir)).toBe(true);
      expect(mode(stateDir)).toBe(0o700);
      expect(exists(log)).toBe(true);
      expect(mode(log)).toBe(0o600);
      expect(await waitForMarker(log)).toContain(MARKER);

      // The predictable world-readable /tmp default must be gone (when the
      // box had no stale copy of it before the run).
      if (!legacyBefore) {
        expect(exists(LEGACY_TMP_LOG)).toBe(false);
      }
    } finally {
      if (!legacyBefore && exists(LEGACY_TMP_LOG)) rmSync(LEGACY_TMP_LOG, { force: true });
    }
  });

  it("honors XDG_STATE_HOME as the state-dir base", async () => {
    const xdg = join(root, "xdg");
    const res = run({ XDG_STATE_HOME: xdg });
    expect(res.status).toBe(0);

    const stateDir = join(xdg, "gptqueue");
    const log = join(stateDir, "http.log");
    expect(mode(stateDir)).toBe(0o700);
    expect(mode(log)).toBe(0o600);
    expect(await waitForMarker(log)).toContain(MARKER);
  });

  it("refuses to append through a symlinked log path (GPTQUEUE_LOG override)", async () => {
    const canary = join(root, "canary.log");
    writeFileSync(canary, "canary-content\n");
    const link = join(root, "log-link");
    symlinkSync(canary, link);

    const res = run({ GPTQUEUE_LOG: link });
    expect(res.status).not.toBe(0);
    expect(res.stderr).toMatch(/symlink/i);
    // The canary was neither appended to nor clobbered.
    expect(readFileSync(canary, "utf8")).toBe("canary-content\n");
    expect(exists(join(root, "nohup.out"))).toBe(false);
  });

  it("keeps the GPTQUEUE_LOG override for a regular file (0600)", async () => {
    const custom = join(root, "custom-dir", "custom.log");
    const res = run({ GPTQUEUE_LOG: custom });
    expect(res.status).toBe(0);
    expect(exists(custom)).toBe(true);
    expect(mode(custom)).toBe(0o600);
    expect(await waitForMarker(custom)).toContain(MARKER);
  });
});
