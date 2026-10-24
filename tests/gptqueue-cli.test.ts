/**
 * Subprocess tests for the public bin/gptqueue dispatcher: help/usage
 * surface, usage exit codes, and one real end-to-end refusal path (a socket
 * no daemon listens on fails loudly with exit 4 before any mutation). Also
 * guards the release bin manifest: package.json / package-lock.json bin
 * entries and REQUIRED_ENTRY_POINTS must all include gptqueue, and the
 * dispatcher's dist imports must exist after the build.
 */
import { describe, expect, it, beforeAll } from "vitest";
import { execSync, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { testRedisUrl } from "./helpers/test-redis.js";
// @ts-expect-error untyped .mjs tooling module
import * as rr from "../scripts/lib/runtime-release.mjs";

const ROOT = process.cwd();
const BIN = join(ROOT, "bin", "gptqueue");

const run = (args: readonly string[]) => {
  const result = spawnSync(process.execPath, [BIN, ...args], { cwd: ROOT, encoding: "utf8", timeout: 30_000 });
  return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
};

describe("gptqueue dispatcher CLI", () => {
  // The dispatcher imports from dist/, so build once before the suite.
  beforeAll(() => {
    execSync("npm run build", { cwd: ROOT, stdio: "inherit" });
  }, 180_000);

  it("prints usage and exits 2 without a command or with an unknown command", () => {
    expect(run([])).toMatchObject({ status: 2 });
    expect(run(["frobnicate"]).stderr).toContain("retire-thread");
    expect(run(["frobnicate"]).stderr).toContain("usage:");
  });

  it("gptqueue --help prints usage and exits 0", () => {
    const result = run(["--help"]);
    expect(result.status).toBe(0);
    expect(result.stdout).toContain("usage: gptqueue <command>");
  });

  it("retire-thread --help documents the full contract and exits 0", () => {
    const result = run(["retire-thread", "--help"]);
    expect(result.status).toBe(0);
    for (const expected of ["--expect-no-mailbox", "--allow-pending-mail", "--redis-url", "--socket", "--verify-timeout-ms", "--dry-run", "Exit codes"]) {
      expect(result.stdout, expected).toContain(expected);
    }
  });

  it("retire-thread usage errors exit 2 with the usage line", () => {
    expect(run(["retire-thread"])).toMatchObject({ status: 2 });
    const missing = run(["retire-thread", "t1", "mb1"]);
    expect(missing.status).toBe(2);
    expect(missing.stderr).toContain("Required --redis-url");
    expect(missing.stderr).toContain("usage: gptqueue retire-thread");
    expect(run(["retire-thread", "t1", "mb1", "--expect-no-mailbox", "--redis-url", "redis://u"]).status).toBe(2);
  });

  it("a socket no daemon listens on fails loudly with exit 4 before any mutation", () => {
    const scratch = mkdtempSync(join(tmpdir(), "gptqueue-cli-"));
    try {
      const missingSocket = join(scratch, "no-daemon.sock");
      const result = run(["retire-thread", "t1", "mb1", "--redis-url", testRedisUrl(), "--socket", missingSocket, "--dry-run"]);
      expect(result.status).toBe(4);
      const problem = JSON.parse(result.stderr.trim().split("\n").pop()!);
      expect(problem).toMatchObject({ error: "daemon_socket_not_listed" });
      expect(problem.message).toBe("daemon_socket_not_listed: observation failed");
      // Nothing was emitted beyond the failed check: no archived event.
      expect(result.stdout).not.toContain("archived");
    } finally {
      rmSync(scratch, { recursive: true, force: true });
    }
  });
});

describe("gptqueue release bin manifest", () => {
  it("ships gptqueue as a node entry point in every manifest that must list it", () => {
    const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
    expect(pkg.bin.gptqueue).toBe("./bin/gptqueue");
    const lock = JSON.parse(readFileSync(join(ROOT, "package-lock.json"), "utf8"));
    expect(lock.packages[""].bin.gptqueue).toBe("bin/gptqueue");
    expect(rr.REQUIRED_ENTRY_POINTS).toContain("gptqueue");
    expect(rr.nodeEntryPoints(ROOT)).toContain("gptqueue");
    // (Full import-integrity incl. dependency isolation is enforced by the
    // release gate in tests/runtime-release.test.ts, which stages a real release.)
  });

  it("every relative dist import of bin/gptqueue resolves after the build", () => {
    const text = readFileSync(BIN, "utf8");
    const specs = [...text.matchAll(/["'](\.\.\/[^"']+\.js)["']/g)].map((match) => match[1]!);
    expect(specs.length).toBeGreaterThan(0);
    for (const spec of specs) {
      expect(existsSync(resolve(dirname(BIN), spec)), spec).toBe(true);
    }
  });
});
