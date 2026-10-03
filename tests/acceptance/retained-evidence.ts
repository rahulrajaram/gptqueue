import { existsSync } from "node:fs";

/**
 * Retained qualification receipts live under the gitignored `.gptqueue/`
 * tree, so they exist only on machines that produced them. Skip, rather than
 * fail, a test whose exact evidence files are absent; where they are present
 * the test asserts in full.
 */
export const requireRetained = (
  ctx: { skip(condition: boolean, note?: string): void },
  ...paths: readonly string[]
): void => {
  const missing = paths.find((path) => !existsSync(path));
  ctx.skip(missing !== undefined, `retained evidence absent: ${missing}`);
};
