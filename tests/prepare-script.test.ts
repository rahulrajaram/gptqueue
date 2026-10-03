import { describe, it, expect, afterAll } from "vitest";
import { spawnSync } from "child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, copyFileSync, chmodSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

// `prepare` must treat commithooks installation as optional: no python3 means
// skip with a notice, but a present python3 whose installer fails must still
// fail. The build step is stripped; only the hook-install segment runs, in a
// temp copy, with a fake python3 so no hooks are ever installed anywhere.
const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf-8"));
const BUILD_PREFIX = "npm run build && ";
const prepare: string = pkg.scripts.prepare;

const root = mkdtempSync(join(tmpdir(), "gptq-prepare-"));
afterAll(() => rmSync(root, { recursive: true, force: true }));
mkdirSync(join(root, "scripts"));
copyFileSync(new URL("../scripts/install-commithooks", import.meta.url), join(root, "scripts", "install-commithooks"));
chmodSync(join(root, "scripts", "install-commithooks"), 0o755);

function fakeBin(name: string, python3Exit: number | null): string {
  const dir = join(root, name);
  mkdirSync(dir);
  if (python3Exit !== null) {
    writeFileSync(join(dir, "python3"), `#!/bin/sh\necho fake-python3 "$@"\nexit ${python3Exit}\n`, { mode: 0o755 });
  }
  return dir;
}

const runHookSegment = (path: string) =>
  spawnSync("/bin/sh", ["-c", prepare.slice(BUILD_PREFIX.length)], {
    cwd: root,
    env: { PATH: path },
    encoding: "utf-8",
  });

describe("package.json prepare: optional commithooks install", () => {
  it("still builds first", () => {
    expect(prepare.startsWith(BUILD_PREFIX)).toBe(true);
  });

  it("skips with a notice when python3 is absent", () => {
    const res = runHookSegment(fakeBin("no-python", null));
    expect(res.status).toBe(0);
    expect(res.stdout + res.stderr).toMatch(/python3 not found.*skipping/i);
  });

  it("fails when python3 exists and the installer fails", () => {
    const res = runHookSegment(fakeBin("python-fails", 3));
    expect(res.stdout).toContain("fake-python3");
    expect(res.status).not.toBe(0);
  });

  it("runs the installer when python3 exists", () => {
    const res = runHookSegment(fakeBin("python-ok", 0));
    expect(res.stdout).toContain("fake-python3");
    expect(res.status).toBe(0);
  });
});
