/**
 * Test helper: scaffold an operator launch-allowlist file (version 2, exact
 * argv templates) in a temp directory and point GPTQUEUE_LAUNCH_ALLOWLIST at
 * it. Cleanup restores the env var and removes the temp dir so tests never
 * leave stale configuration behind and never depend on a checked-in
 * `.gptqueue/launch-allowlist.json`.
 */
import { mkdtempSync, writeFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

export interface AllowlistCommandInput {
  command: string;
  /** Exact argv templates; a request must equal one fully. */
  allowed_args?: readonly (readonly string[])[];
  comment?: string;
}

export interface LaunchAllowlistScaffold {
  /** Absolute path of the written allowlist file (rewritable for re-check tests). */
  path: string;
  /** Point GPTQUEUE_LAUNCH_ALLOWLIST at this file. */
  set: () => void;
  /** Restore the env var and remove the temp directory. */
  cleanup: () => void;
}

export function scaffoldLaunchAllowlist(
  commands: readonly AllowlistCommandInput[],
  opts: { version?: number; path?: string } = {}
): LaunchAllowlistScaffold {
  const dir = mkdtempSync(join(tmpdir(), "gptqueue-allowlist-"));
  const path = opts.path ?? join(dir, "launch-allowlist.json");
  const config = {
    version: opts.version ?? 2,
    commands: commands.map((c) => ({
      command: c.command,
      allowed_args: c.allowed_args ?? [[]],
      ...(c.comment ? { comment: c.comment } : {}),
    })),
  };
  writeFileSync(path, JSON.stringify(config, null, 2));
  return {
    path,
    set: () => {
      process.env.GPTQUEUE_LAUNCH_ALLOWLIST = path;
    },
    cleanup: () => {
      delete process.env.GPTQUEUE_LAUNCH_ALLOWLIST;
      try {
        rmSync(dir, { recursive: true, force: true });
      } catch {
        /* best-effort */
      }
    },
  };
}
