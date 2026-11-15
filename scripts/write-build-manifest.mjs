#!/usr/bin/env node
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
// @ts-ignore ESM tooling module
import { writeBuildManifest } from "./lib/build-provenance.mjs";

const args = process.argv.slice(2);
const rootIndex = args.indexOf("--root");
if (args.length !== 0 && (args.length !== 2 || rootIndex !== 0)) {
  throw new Error("usage: write-build-manifest.mjs [--root DIR]");
}
const root = rootIndex === 0 ? resolve(args[1]) : resolve(fileURLToPath(new URL("..", import.meta.url)));
writeBuildManifest(root);
