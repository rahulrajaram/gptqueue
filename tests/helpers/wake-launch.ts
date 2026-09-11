/**
 * Stable launch fixtures for tests that need a REAL spawned runtime under the
 * v2 exact-argv launch policy. The policy rejects interpreter inline-code
 * flags (`node -e …`) unconditionally (finding F1), so tests spawn
 * `process.execPath` pointed at these fixed script files instead, and the
 * scaffolded allowlist templates the exact argv
 * (`[<script path>]`). Paths are resolved from the repo root (vitest cwd).
 */
import { resolve } from "path";

/** Absolute path of the sleepy runtime fixture (exits after ~20s). */
export const WAKE_SLEEPY_SCRIPT: string = resolve(
  "tests/fixtures/wake-sleepy.mjs"
);

/** Absolute path of the immediately-exiting runtime fixture. */
export const WAKE_EXIT_SCRIPT: string = resolve("tests/fixtures/wake-exit.mjs");
