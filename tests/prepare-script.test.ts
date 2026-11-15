import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";

// Since 748cd0a the `prepare` script only builds; installing the shared
// commithooks is an explicit, opt-in `hooks:install` step. It must never run
// as a side effect of `prepare` (and so never of `npm install`). These tests
// pin that contract from package.json itself — no child processes, no python3.
const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf-8"));

describe("package.json prepare and hooks:install scripts", () => {
  it("prepare runs the build", () => {
    expect(pkg.scripts.prepare).toBe("npm run build");
    expect(typeof pkg.scripts.build).toBe("string");
    expect(pkg.scripts.build.length).toBeGreaterThan(0);
  });

  it("prepare does not run the commithooks installer", () => {
    expect(pkg.scripts.prepare).not.toMatch(/install-commithooks|hooks:install|python3/i);
  });

  it("hooks:install names the commithooks installer script", () => {
    expect(pkg.scripts["hooks:install"]).toContain("scripts/install-commithooks");
  });
});
