/**
 * Launch policy: the operator-allowlist gate that governs which runtimes a
 * durable actor may register to launch (and therefore which commands the wake
 * launcher may spawn).
 *
 * This is the PROVISIONAL defense-in-depth layer added by review finding H2:
 * previously `buildLaunch`/`dispatchLaunch` copied `launch_command`,
 * `launch_args`, and `launch_cwd` verbatim (shell:false blocked metacharacter
 * injection but NOT arbitrary-program execution). Under this model a caller
 * could register `/bin/sh -c <payload>` and trigger it via
 * `send_message -> maybeWake -> dispatchLaunch`.
 *
 * The ratified model is an operator-managed allowlist (provisional pending
 * per-actor operator grants):
 *
 *   1. `.gptqueue/launch-allowlist.json` at the server's working directory
 *      governs new `wake_if_offline` registrations (fail-closed: an absent or
 *      unparseable file refuses admission).
 *   2. Dangerous delegators (shells and `-c`-style arg flags) are rejected
 *      regardless of the allowlist.
 *   3. dispatchLaunch re-checks the allowlist fail-closed as defense in depth,
 *      so a stale actor-directory entry cannot spawn what the operator has
 *      since disallowed.
 *   4. `launch_cwd` is confined to the server workspace root via
 *      path.resolve + prefix check (no realpath-required symlink-escape
 *      handling — documented limitation).
 *
 * This module reads the filesystem for the allowlist only; it performs no
 * spawns and touches no Redis.
 */

import { readFile, stat } from "fs/promises";
import { basename, resolve, sep } from "path";

/** One command the operator has permitted to be launched. */
export interface LaunchAllowlistEntry {
  readonly command: string;
  /**
   * Each entry is an accepted args prefix. A request's args are acceptable
   * when they are prefix-compatible with at least one entry (requested[i]
   * must equal allowed[i] for the prefix length; args beyond the prefix are
   * free). `[]` accepts any args.
   */
  readonly allowed_args_prefixes: readonly (readonly string[])[];
  readonly comment?: string;
}

/** Document format for `.gptqueue/launch-allowlist.json`. */
export interface LaunchAllowlistConfig {
  readonly version: number;
  readonly commands: readonly LaunchAllowlistEntry[];
}

/** Result of attempting to load + parse the operator allowlist file. */
export type AllowlistLoad =
  | { readonly kind: "loaded"; readonly config: LaunchAllowlistConfig }
  | { readonly kind: "absent" }
  | { readonly kind: "unparseable"; readonly reason: string };

/**
 * Minimal shape of a launch contract the policy inspects. Structurally
 * compatible with `RuntimeLaunchContract` (core/actor-directory.ts) so both
 * admission and dispatch can reuse one evaluator without an import cycle.
 */
export interface LaunchContractLike {
  readonly command: string;
  readonly args: readonly string[];
  readonly cwd?: string;
}

/**
 * Outcome of evaluating a launch contract against the operator allowlist.
 * `ok: true` when the contract may be launched; otherwise a typed error code
 * drives both admission and dispatch decisions.
 */
export type LaunchPolicyDecision =
  | Readonly<{ ok: true }>
  | Readonly<{
      ok: false;
      error: Readonly<{
        code:
          | "launch_command_rejected"
          | "launch_not_allowlisted"
          | "launch_cwd_confined";
        message: string;
      }>;
    }>;

/**
 * Shells treated as dangerous delegators: spawning one lets the payload
 * choose an arbitrary program, so they are rejected unconditionally (whether
 * or not the operator allowlisted them).
 */
export const DANGEROUS_SHELL_BASENAMES: ReadonlySet<string> = new Set([
  "sh",
  "bash",
  "zsh",
  "dash",
  "fish",
  "ksh",
  "cmd",
  "powershell",
  "pwsh",
]);

/**
 * Flag-style args that turn a shell into an arbitrary-program delelgator
 * (`sh -c "<payload>"`). Subsumed by rejecting every shell basename above,
 * but kept explicit so the rejection reason is auditable.
 */
export const DANGEROUS_SHELL_ARGS: ReadonlySet<string> = new Set([
  "-c",
  "-lc",
  "-Command",
]);

/** Path of the operator allowlist file, overridable for tests. */
export const allowlistFilePath = (): string =>
  process.env.GPTQUEUE_LAUNCH_ALLOWLIST?.trim() ||
  ".gptqueue/launch-allowlist.json";

/**
 * Normalize a command to its basename. `path.basename` handles both shapes:
 * a PATH-style bare name (`"node"` -> `"node"`) and a path
 * (`"/usr/bin/node"` -> `"node"`). Matching therefore compares basenames so
 * an absolute request path aliases the same allowlisted name.
 */
export const normalizeCommand = (command: string): string =>
  basename(command);

/** Whether a requested command's basename matches an allowlisted entry's. */
export const commandMatches = (
  requestedCommand: string,
  allowlistedCommand: string
): boolean =>
  normalizeCommand(requestedCommand) === normalizeCommand(allowlistedCommand);

/**
 * Whether `requestedArgs` are prefix-compatible with `allowedPrefix`: every
 * requested arg[i] must equal allowedPrefix[i] for the prefix length; args
 * beyond the prefix are free.
 */
export const argsPrefixCompatible = (
  requestedArgs: readonly string[],
  allowedPrefix: readonly string[]
): boolean => {
  for (let i = 0; i < allowedPrefix.length; i += 1) {
    if (requestedArgs[i] !== allowedPrefix[i]) return false;
  }
  return true;
};

/** Whether a request command+args match at least one allowlist entry. */
export const launchMatchesConfig = (
  command: string,
  args: readonly string[],
  config: LaunchAllowlistConfig
): boolean =>
  config.commands.some(
    (entry) =>
      commandMatches(command, entry.command) &&
      entry.allowed_args_prefixes.some((prefix) =>
        argsPrefixCompatible(args, prefix)
      )
  );

/**
 * True when the command is a dangerous delegator (a shell) regardless of its
 * args, OR a shell carrying a `-c`-style flag. The first clause already
 * rejects every listed shell; the arg clause is retained for explicit,
 * auditable rejection messaging.
 */
export const isDangerousDelegator = (
  command: string,
  args: readonly string[]
): boolean => {
  const normalized = normalizeCommand(command).toLowerCase();
  if (DANGEROUS_SHELL_BASENAMES.has(normalized)) return true;
  return (
    DANGEROUS_SHELL_BASENAMES.has(normalized) &&
    args.some((arg) => DANGEROUS_SHELL_ARGS.has(arg))
  );
};

export type CwdConfinement =
  | Readonly<{ ok: true }>
  | Readonly<{ ok: false; message: string }>;

/**
 * Confine `launch_cwd` to the server workspace root. Uses path.resolve +
 * a prefix check (NOT realpath), so a symlinked path whose lexical prefix is
 * inside the root passes even if its real target escapes — documented
 * limitation; realpath-based symlink-escape protection is out of scope here.
 */
export const launchCwdIsConfined = async (
  cwd: string
): Promise<CwdConfinement> => {
  let isDirectory = false;
  try {
    isDirectory = (await stat(cwd)).isDirectory();
  } catch {
    // fall through to the "not an existing directory" rejection
  }
  if (!isDirectory) {
    return {
      ok: false,
      message: `launch_cwd '${cwd}' is not an existing directory`,
    };
  }
  const workspaceRoot = resolve(process.cwd());
  const resolved = resolve(cwd);
  if (resolved === workspaceRoot || resolved.startsWith(workspaceRoot + sep)) {
    return { ok: true };
  }
  return {
    ok: false,
    message: `launch_cwd '${cwd}' resolves to '${resolved}' which is outside the server workspace root '${workspaceRoot}'`,
  };
};

/**
 * Load and parse the operator allowlist file (fail-closed: absent or
 * unparseable yields a non-loaded result).
 */
export const loadLaunchAllowlist = async (): Promise<AllowlistLoad> => {
  const path = allowlistFilePath();
  let raw: string;
  try {
    raw = await readFile(path, "utf-8");
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return { kind: "absent" };
    return {
      kind: "unparseable",
      reason: `cannot read ${path}: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  return parseLaunchAllowlist(raw, path);
};

/** Parse + validate the allowlist JSON document. */
export const parseLaunchAllowlist = (
  raw: string,
  source: string
): AllowlistLoad => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    return {
      kind: "unparseable",
      reason: `cannot parse ${source}: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  if (typeof parsed !== "object" || parsed === null) {
    return { kind: "unparseable", reason: `${source}: allowlist must be a JSON object` };
  }
  const obj = parsed as Record<string, unknown>;
  if (obj.version !== 1) {
    return {
      kind: "unparseable",
      reason: `${source}: unsupported allowlist version ${JSON.stringify(obj.version)} (expected 1)`,
    };
  }
  if (!Array.isArray(obj.commands)) {
    return {
      kind: "unparseable",
      reason: `${source}: 'commands' must be an array`,
    };
  }
  const commands: LaunchAllowlistEntry[] = [];
  for (const entry of obj.commands) {
    if (typeof entry !== "object" || entry === null) {
      return { kind: "unparseable", reason: `${source}: each command entry must be an object` };
    }
    const e = entry as Record<string, unknown>;
    if (typeof e.command !== "string" || e.command.trim().length === 0) {
      return {
        kind: "unparseable",
        reason: `${source}: allowlisted 'command' must be a non-empty string`,
      };
    }
    const rawPrefixes = e.allowed_args_prefixes;
    if (!Array.isArray(rawPrefixes)) {
      return {
        kind: "unparseable",
        reason: `${source}: 'allowed_args_prefixes' must be an array for command '${e.command}'`,
      };
    }
    const prefixes: string[][] = [];
    for (const p of rawPrefixes) {
      if (
        !Array.isArray(p) ||
        !p.every((a) => typeof a === "string")
      ) {
        return {
          kind: "unparseable",
          reason: `${source}: each args prefix must be an array of strings for command '${e.command}'`,
        };
      }
      prefixes.push([...p]);
    }
    commands.push({
      command: e.command,
      allowed_args_prefixes: prefixes,
      ...(typeof e.comment === "string" ? { comment: e.comment } : {}),
    });
  }
  return {
    kind: "loaded",
    config: Object.freeze({
      version: 1,
      commands: Object.freeze(commands.map((c) => Object.freeze(c))),
    }),
  };
};

/**
 * Evaluate a launch contract against the full operator policy (cwd
 * confinement, dangerous-delegator rejection, allowlist membership). Used at
 * admission (new wake_if_offline registrations) and at dispatch (defense in
 * depth), so both paths share identical rules and ordering.
 */
export const evaluateLaunchPolicy = async (
  contract: LaunchContractLike
): Promise<LaunchPolicyDecision> => {
  if (contract.cwd !== undefined) {
    const confined = await launchCwdIsConfined(contract.cwd);
    if (!confined.ok) {
      return Object.freeze({
        ok: false,
        error: Object.freeze({
          code: "launch_cwd_confined",
          message: confined.message,
        }),
      });
    }
  }

  if (isDangerousDelegator(contract.command, [...contract.args])) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "launch_command_rejected",
        message: `command '${contract.command}' is a dangerous delegator (shell) and is rejected regardless of the launch allowlist`,
      }),
    });
  }

  const loaded = await loadLaunchAllowlist();
  if (loaded.kind !== "loaded") {
    const path = allowlistFilePath();
    const why =
      loaded.kind === "absent"
        ? "file is absent"
        : `file is unparseable: ${loaded.reason}`;
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "launch_not_allowlisted",
        message: `launch allowlist at ${path} is unavailable (${why}); refusing to launch (fail closed)`,
      }),
    });
  }

  if (
    !launchMatchesConfig(contract.command, [...contract.args], loaded.config)
  ) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "launch_not_allowlisted",
        message: `command '${contract.command}' (args ${JSON.stringify([...contract.args])}) is not permitted by the launch allowlist at ${allowlistFilePath()}`,
      }),
    });
  }

  return Object.freeze({ ok: true });
};