/**
 * Machine-local tool locations for acceptance tests, derived portably instead
 * of hardcoding one developer's home directory. Each can be overridden:
 *   GPTQUEUE_NODE_PREFIX  node installation prefix (default: the running node's)
 *   CODEX_BIN, OPENCODE_BIN  individual binaries
 * Tests that need a tool that is absent skip or report a prerequisite gap.
 */
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

export const HOME = homedir();

/** Prefix of the node install whose global modules the tests use (bin/, lib/node_modules/). */
export const NODE_PREFIX = process.env.GPTQUEUE_NODE_PREFIX ?? dirname(dirname(process.execPath));

/** Repository root (this file lives in tests/acceptance/). */
export const REPO_ROOT = resolve(import.meta.dirname, "../..");

export const nodePrefixPath = (...parts: string[]): string => join(NODE_PREFIX, ...parts);
export const homePath = (...parts: string[]): string => join(HOME, ...parts);
export const repoPath = (...parts: string[]): string => join(REPO_ROOT, ...parts);

export const CODEX_BIN = process.env.CODEX_BIN ?? homePath(".local/bin/codex");
