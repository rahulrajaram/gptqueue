/**
 * AT10 plug-and-play boundary: the library under src/agent-tracking imports
 * only node: builtins and its own files, so it can be split into its own
 * package. GPTQueue reaches it only through the plugin seam.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import * as library from "../../src/agent-tracking/index.js";

const ROOT = resolve(__dirname, "..", "..");
const LIB = join(ROOT, "src", "agent-tracking");
const SRC = join(ROOT, "src");

const tsFiles = (dir: string): string[] =>
  readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? tsFiles(path) : path.endsWith(".ts") ? [path] : [];
  });

const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(\s*|\brequire\s*\(\s*|^\s*import\s+)["']([^"']+)["']/gm;
const specifiers = (path: string): string[] =>
  [...readFileSync(path, "utf8").matchAll(SPECIFIER)].map((m) => m[1]!);

describe("AT10 library boundary", () => {
  it("imports nothing but node: builtins and files inside src/agent-tracking", () => {
    const violations = tsFiles(LIB).flatMap((file) =>
      specifiers(file)
        .filter((spec) => {
          if (spec.startsWith("node:")) return false;
          if (!spec.startsWith(".")) return true; // a package dependency
          const target = resolve(dirname(file), spec);
          return relative(LIB, target).startsWith("..");
        })
        .map((spec) => `${relative(ROOT, file)} -> ${spec}`)
    );
    expect(violations).toEqual([]);
  });

  it("is reached from the rest of GPTQueue only through its index, and only by the plugin seam", () => {
    const importers = tsFiles(SRC)
      .filter((file) => !file.startsWith(LIB))
      .flatMap((file) =>
        specifiers(file)
          .filter((spec) => spec.startsWith(".") && resolve(dirname(file), spec).startsWith(LIB))
          .map((spec) => ({ file: relative(ROOT, file), target: relative(ROOT, resolve(dirname(file), spec)) }))
      );
    expect(importers.every((i) => i.target === "src/agent-tracking/index.js")).toBe(true);
    expect([...new Set(importers.map((i) => i.file))].sort()).toEqual(["src/integrations/agent-tracking.ts"]);
  });

  it("contains no not-implemented placeholders once delivered", () => {
    const placeholders = tsFiles(LIB).filter((file) => readFileSync(file, "utf8").includes("notImplemented("));
    expect(placeholders.map((file) => relative(ROOT, file))).toEqual([]);
  });

  it("exports the host-facing API from its index", () => {
    for (const name of [
      "createSeatService", "createMemorySeatStore", "createRedisSeatStore", "createNodeProcessProbe",
      "startSeat", "admitSeatFile", "authorizeBind", "transitionTenure", "checkFence", "toAgentDescriptor",
    ]) {
      expect(typeof (library as Record<string, unknown>)[name]).toBe("function");
    }
  });
});
