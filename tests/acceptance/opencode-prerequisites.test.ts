import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// A missing BUILT GPTQueue plugin is a build failure, not an absent external
// runtime: when the OpenCode binary and model catalog exist it must fail, and
// only an absent OpenCode binary or catalog may skip. No live agent runs here.
describe("OpenCode qualification prerequisites", () => {
  const cwd = process.cwd();
  const roots: string[] = [];

  afterEach(() => {
    process.chdir(cwd);
    vi.unstubAllEnvs();
    vi.resetModules();
    for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
  });

  /** A fake checkout + HOME with OpenCode present (or not) and no built plugin. */
  const loadPreflight = async (opencodePresent: boolean) => {
    const root = mkdtempSync(join(tmpdir(), "gptq-oc-prereq-"));
    roots.push(root);
    const home = join(root, "home");
    mkdirSync(join(home, ".cache/opencode"), { recursive: true });
    const bin = join(root, "opencode");
    if (opencodePresent) {
      writeFileSync(bin, "#!/bin/sh\nexit 0\n", { mode: 0o755 });
      writeFileSync(join(home, ".cache/opencode/models.json"), "{}");
    }
    vi.stubEnv("HOME", home);
    vi.stubEnv("OPENCODE_BIN", bin);
    process.chdir(root); // repo = process.cwd(): no dist/ here.
    vi.resetModules();
    const { opencodeRouteAdapter } = await import("./qualification-opencode.js");
    return () => opencodeRouteAdapter("opencode-run")!.preflight(new AbortController().signal);
  };

  it("OpenCode present but the built plugin missing is a failure, not a skip", async () => {
    const preflight = await loadPreflight(true);
    await expect(preflight()).rejects.toThrow(/Built GPTQueue plugin unavailable/u);
  });

  it("OpenCode absent is a skippable blocked_prerequisite", async () => {
    const preflight = await loadPreflight(false);
    const result = await preflight();
    expect(result.kind).toBe("blocked_prerequisite");
  });
});

describe("opencodePrerequisites predicate", () => {
  const paths = { binary: "/bin/opencode", models: "/models.json", plugin: "/dist/plugin.js" };
  const only = (...present: string[]) => (path: string) => present.includes(path);

  it("OpenCode present and plugin missing is build_missing", async () => {
    const { opencodePrerequisites } = await import("./opencode-support.js");
    expect(opencodePrerequisites(only(paths.binary, paths.models), paths).kind).toBe("build_missing");
  });

  it("an absent binary or model catalog is unavailable regardless of the plugin", async () => {
    const { opencodePrerequisites } = await import("./opencode-support.js");
    expect(opencodePrerequisites(only(paths.models, paths.plugin), paths).kind).toBe("unavailable");
    expect(opencodePrerequisites(only(paths.binary, paths.plugin), paths).kind).toBe("unavailable");
    expect(opencodePrerequisites(only(), paths).kind).toBe("unavailable");
  });

  it("everything present is ready", async () => {
    const { opencodePrerequisites } = await import("./opencode-support.js");
    expect(opencodePrerequisites(only(paths.binary, paths.models, paths.plugin), paths).kind).toBe("ready");
  });
});
