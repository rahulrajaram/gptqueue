import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

/**
 * Reference-boundary guard (H6 + review M3).
 *
 * activation-model.ts and custody-model.resolveWorkOwnership both claim (via
 * their header docblocks) to be reference models that are NOT wired into
 * production delivery. This test keeps those claims honest:
 *   (a) the disclosure text must remain present in the docblocks, and
 *   (b) no src file outside the model may import activation-model, so a silent
 *       production rewiring cannot happen without first touching (and updating)
 *       this test.
 * The disclosure phrases are the single source of truth asserted here; if the
 * wording changes, update BOTH the docblock and these constants together.
 */
const SRC_DIR = fileURLToPath(new URL("../src/", import.meta.url));
const ACTIVATION_MODEL_PATH = join(SRC_DIR, "core", "activation-model.ts");
const CUSTODY_MODEL_PATH = join(SRC_DIR, "core", "custody-model.ts");

/** Phrase disclosed in activation-model.ts's header. */
const ACTIVATION_DISCLOSURE = "NOT wired into production delivery";
/** Phrase disclosed above resolveWorkOwnership in custody-model.ts. */
const CUSTODY_DISCLOSURE = "UNWIRED DECISION ORACLE";

/** Recursively collect every `.ts` file under a directory. */
function walkTypeScriptFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const s = statSync(full);
    if (s.isDirectory()) {
      out.push(...walkTypeScriptFiles(full));
    } else if (entry.endsWith(".ts")) {
      out.push(full);
    }
  }
  return out;
}

describe("reference-model boundary is honest (H6 / M3)", () => {
  it("activation-model discloses it is NOT wired into production delivery", () => {
    const source = readFileSync(ACTIVATION_MODEL_PATH, "utf-8");
    expect(source).toContain(ACTIVATION_DISCLOSURE);
    // The disclosure must also name where production delivery actually lives so
    // a future reader is not misled into thinking this is the governing model.
    expect(source).toContain("send-message.ts");
    expect(source).toContain("wake-lease");
  });

  it("no file under src/ outside activation-model imports it (no silent rewiring)", () => {
    const offenders: string[] = [];
    for (const file of walkTypeScriptFiles(SRC_DIR)) {
      if (file === ACTIVATION_MODEL_PATH) continue;
      const source = readFileSync(file, "utf-8");
      const importLines = source
        .split("\n")
        .filter((line) => line.trim().startsWith("import"));
      if (importLines.some((line) => /activation-model/.test(line))) {
        offenders.push(file);
      }
    }
    expect(offenders).toEqual([]);
  });

  it("custody-model discloses that resolveWorkOwnership is an unwired decision oracle", () => {
    const source = readFileSync(CUSTODY_MODEL_PATH, "utf-8");
    expect(source).toContain(CUSTODY_DISCLOSURE);
    expect(source).toMatch(/enforcement gap/i);
  });
});
