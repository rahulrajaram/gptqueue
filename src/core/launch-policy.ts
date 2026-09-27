/**
 * Launch policy: the operator-allowlist gate that governs which runtimes a
 * durable actor may register to launch (and therefore which commands the wake
 * launcher may spawn).
 *
 * This is the defense-in-depth layer hardened by review finding F1: the
 * original provisional model matched commands by BASENAME and accepted any
 * args SUFFIX after a configured prefix, so a caller could register
 * `node -e <payload>` or an attacker-controlled path sharing an allowlisted
 * basename (`/attacker/work/node`) and have it spawned under the server's OS
 * identity. The ratified model is exact argv identity:
 *
 *   1. The operator allowlist (default `~/.config/gptqueue/launch-allowlist.json`,
 *      outside any agent workspace; see `allowlistFilePath`)
 *      governs new `wake_if_offline` registrations (fail-closed: an absent or
 *      unparseable file refuses admission).
 *   2. Command identity is exact: a bare-name entry matches only the identical
 *      bare name, and an absolute-path entry matches only an absolute request
 *      that lexically `resolve()`s to the same path. Basename aliasing is
 *      rejected (`/attacker/node` does NOT match an allowlisted `node`).
 *   3. Args match only by EXACT template: a request's full argv must equal one
 *      of the entry's `allowed_args` templates element-for-element and in
 *      length. There is no suffix freedom, and `[]` accepts only "no args".
 *   4. Dangerous delegators (shells) and interpreter inline-code flags
 *      (`node -e …`, `python -c …`) are rejected regardless of the allowlist.
 *   5. dispatchLaunch re-checks the allowlist fail-closed as defense in depth,
 *      so a stale actor-directory entry cannot spawn what the operator has
 *      since disallowed.
 *   6. `launch_cwd` is confined to the server workspace root via
 *      path.resolve + prefix check (no realpath-required symlink-escape
 *      handling — documented limitation, shared with command identity).
 *
 * Version 1 documents (basename + unbounded `allowed_args_prefixes` semantics)
 * are rejected at parse time with a migration message; they cannot be soundly
 * auto-converted to exact templates.
 *
 * This module reads the filesystem for the allowlist only; it performs no
 * spawns and touches no Redis.
 */

import { lstat, readFile, stat } from "fs/promises";
import { homedir } from "os";
import { basename, isAbsolute, join, resolve, sep } from "path";

/** One command the operator has permitted to be launched. */
export interface LaunchAllowlistEntry {
  readonly command: string;
  /**
   * Exact argv templates. A request's args are acceptable only when they
   * equal one template completely (same length, every element equal).
   * `[]` accepts only a request with no args. There is deliberately no
   * prefix/suffix freedom: a suffix such as `node … -e <payload>` must
   * never silently extend an operator grant.
   */
  readonly allowed_args: readonly (readonly string[])[];
  readonly comment?: string;
}

/** Document format for the operator launch allowlist. */
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
const DANGEROUS_SHELL_BASENAMES: ReadonlySet<string> = new Set([
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
 * Interpreters whose inline-code flags execute an arbitrary string supplied
 * in argv (`node -e`, `python -c`, `perl -e`, ...). Combined with the
 * inline-code predicate below these are rejected regardless of the allowlist
 * (F1/D1): even an exact-template grant must not become an arbitrary-code
 * channel. Operators who need scripted behavior point the interpreter at a
 * fixed script FILE instead.
 */
const DANGEROUS_INTERPRETER_BASENAMES: ReadonlySet<string> = new Set([
  "node",
  "nodejs",
  "deno",
  "bun",
  "tsx",
  "ts-node",
  "python",
  "python2",
  "python3",
  "ruby",
  "perl",
  "php",
]);

/**
 * The awk family takes its PROGRAM as its first non-flag positional argv
 * element — every invocation executes an argv-supplied string, so no flag
 * predicate can make it safe. Rejected unconditionally, like shells (D1).
 */
const DANGEROUS_AWK_BASENAMES: ReadonlySet<string> = new Set([
  "awk",
  "gawk",
  "mawk",
]);

/** Detached inline-code flags that make an interpreter execute an argv string. */
const DANGEROUS_INTERPRETER_ARGS: ReadonlySet<string> = new Set([
  "-e",
  "--eval",
  "-c",
  "--command",
  "-p",
  "--print",
  "-E",
  "-r",
]);

/**
 * Whether one argv element is an inline-code flag in any spelling: detached
 * (`-e`), equals-glued (`--eval=<code>`), or value-glued short form
 * (`python -c<code>`, `perl -e<code>`, `node -p<code>`). D1: the predicate
 * previously matched only exact detached tokens, so `--eval=x` and `-cfoo`
 * bypassed the documented unconditional rejection.
 */
const isInlineCodeArg = (arg: string): boolean =>
  DANGEROUS_INTERPRETER_ARGS.has(arg) ||
  /^--(eval|print|command)=/.test(arg) ||
  /^-[ecEpr].+/.test(arg);

/**
 * Path of the operator allowlist file. It defaults to the user config
 * directory, outside any workspace: agents running as the same user can write
 * their workspace, and an allowlist they can edit is one they can
 * self-authorize with. GPTQUEUE_LAUNCH_ALLOWLIST overrides it.
 */
const allowlistFilePath = (): string =>
  process.env.GPTQUEUE_LAUNCH_ALLOWLIST?.trim() ||
  join(
    process.env.XDG_CONFIG_HOME?.trim() || join(homedir(), ".config"),
    "gptqueue",
    "launch-allowlist.json"
  );

/** Pre-0.1.0 default, inside the server's working directory; no longer read. */
const LEGACY_ALLOWLIST_PATH = ".gptqueue/launch-allowlist.json";

/**
 * Reduce a command to its basename. Used ONLY by the rejection predicates
 * (`isDangerousDelegator`, `isDangerousInterpreter`): for rejection, basename
 * matching is conservative — it over-rejects aliases, never under-rejects.
 * Admission (`commandMatches`) never aliases by basename.
 */
const normalizeCommand = (command: string): string =>
  basename(command);

/**
 * Whether a requested command satisfies an allowlisted entry's identity
 * (F1: exact identity, never basename aliasing):
 *   - bare-name entry matches only the byte-identical bare name;
 *   - absolute-path entry matches only an absolute request that lexically
 *     `resolve()`s to the same path;
 *   - a bare request NEVER matches an absolute entry and vice versa.
 */
export const commandMatches = (
  requestedCommand: string,
  allowlistedCommand: string
): boolean => {
  const requestedAbsolute = isAbsolute(requestedCommand);
  const allowlistedAbsolute = isAbsolute(allowlistedCommand);
  if (requestedAbsolute !== allowlistedAbsolute) return false;
  return requestedAbsolute
    ? resolve(requestedCommand) === resolve(allowlistedCommand)
    : requestedCommand === allowlistedCommand;
};

/**
 * Whether `requestedArgs` EQUAL `template` exactly: same length and every
 * element identical. This replaces the v1 prefix semantics — there is no
 * suffix freedom, so `["--port", "1234", "-e", "<payload>"]` can never extend
 * a `["--port", "1234"]` grant, and `[]` accepts only "no args".
 */
export const argsMatchTemplate = (
  requestedArgs: readonly string[],
  template: readonly string[]
): boolean =>
  requestedArgs.length === template.length &&
  template.every((arg, i) => requestedArgs[i] === arg);

/** Whether a request command+args match at least one allowlist entry. */
export const launchMatchesConfig = (
  command: string,
  args: readonly string[],
  config: LaunchAllowlistConfig
): boolean =>
  config.commands.some(
    (entry) =>
      commandMatches(command, entry.command) &&
      entry.allowed_args.some((template) =>
        argsMatchTemplate(args, template)
      )
  );

/**
 * True when the command is a dangerous delegator (a shell) regardless of its
 * args — the basename rejection is what carries the audit story; there is
 * deliberately no args clause (D9: the previous second disjunct re-tested
 * the same set membership and could never fire).
 */
const isDangerousDelegator = (command: string): boolean =>
  DANGEROUS_SHELL_BASENAMES.has(normalizeCommand(command).toLowerCase());

/**
 * True when an allowlisted interpreter is asked to execute an inline code
 * string in ANY spelling (`node -e …`, `python -c<code>`, `--eval=<code>`,
 * `node -p …`, the awk family's positional program). Rejected regardless of
 * the allowlist (F1/D1): an interpreter executing an argv-supplied string is
 * an arbitrary-code channel even under an exact-template grant, because the
 * executed string is only as trustworthy as whoever authored the registration.
 */
const isDangerousInterpreter = (
  command: string,
  args: readonly string[]
): boolean => {
  const normalized = normalizeCommand(command).toLowerCase();
  if (DANGEROUS_AWK_BASENAMES.has(normalized)) return true;
  if (!DANGEROUS_INTERPRETER_BASENAMES.has(normalized)) return false;
  return args.some(isInlineCodeArg);
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
const loadLaunchAllowlist = async (): Promise<AllowlistLoad> => {
  const path = allowlistFilePath();
  let raw: string;
  try {
    // lstat, not stat: a symlink could point the allowlist at a writable file.
    const info = await lstat(path);
    if (!info.isFile()) {
      return { kind: "unparseable", reason: `${path} is not a regular file (symlinks are refused)` };
    }
    if ((info.mode & 0o022) !== 0) {
      return { kind: "unparseable", reason: `${path} is group- or world-writable; restrict it (chmod go-w)` };
    }
    raw = await readFile(path, "utf-8");
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT") {
      if (!process.env.GPTQUEUE_LAUNCH_ALLOWLIST?.trim() && (await lstat(LEGACY_ALLOWLIST_PATH).then(() => true, () => false))) {
        return {
          kind: "unparseable",
          reason: `found ${LEGACY_ALLOWLIST_PATH} in the working directory, which is no longer read because agents can write it; move it to ${path} or set GPTQUEUE_LAUNCH_ALLOWLIST`,
        };
      }
      return { kind: "absent" };
    }
    return {
      kind: "unparseable",
      reason: `cannot read ${path}: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  return parseLaunchAllowlist(raw, path);
};

/** Parse + validate the allowlist JSON document (version 2 only). */
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
  if (obj.version !== 2) {
    const hint =
      obj.version === 1
        ? "version-1 prefix-based documents are no longer accepted (basename matching and unbounded args suffixes permitted arbitrary interpreter execution); migrate each 'allowed_args_prefixes' entry to an exact 'allowed_args' template — a full argv, or [] for no args"
        : "expected 2";
    return {
      kind: "unparseable",
      reason: `${source}: unsupported allowlist version ${JSON.stringify(obj.version)} (${hint})`,
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
    const rawTemplates = e.allowed_args;
    if (!Array.isArray(rawTemplates)) {
      return {
        kind: "unparseable",
        reason: `${source}: 'allowed_args' must be an array for command '${e.command}'`,
      };
    }
    const templates: string[][] = [];
    for (const t of rawTemplates) {
      if (
        !Array.isArray(t) ||
        !t.every((a) => typeof a === "string")
      ) {
        return {
          kind: "unparseable",
          reason: `${source}: each allowed_args template must be an array of strings for command '${e.command}'`,
        };
      }
      templates.push([...t]);
    }
    commands.push({
      command: e.command.trim(),
      allowed_args: templates,
      ...(typeof e.comment === "string" ? { comment: e.comment } : {}),
    });
  }
  return {
    kind: "loaded",
    config: Object.freeze({
      version: 2,
      commands: Object.freeze(commands.map((c) => Object.freeze(c))),
    }),
  };
};

/**
 * Evaluate a launch contract against the full operator policy (cwd
 * confinement, dangerous-delegator and dangerous-interpreter rejection,
 * exact allowlist membership). Used at admission (new wake_if_offline
 * registrations) and at dispatch (defense in depth), so both paths share
 * identical rules and ordering.
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

  if (isDangerousDelegator(contract.command)) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "launch_command_rejected",
        message: `command '${contract.command}' is a dangerous delegator (shell) and is rejected regardless of the launch allowlist`,
      }),
    });
  }

  if (isDangerousInterpreter(contract.command, [...contract.args])) {
    return Object.freeze({
      ok: false,
      error: Object.freeze({
        code: "launch_command_rejected",
        message: `command '${contract.command}' carries an inline-code flag (${[...contract.args].find(isInlineCodeArg)}) and is rejected regardless of the launch allowlist; point the interpreter at a fixed script file instead`,
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
