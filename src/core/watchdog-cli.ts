/**
 * `gptqueue-watchdog --once [--config PATH] [--dry-run] [--redis-url URL]`
 *
 * One watchdog pass (loop adapter S2). Prints one JSON object on stdout.
 * Exit codes: 0 success, 2 usage or config problem, 1 Redis unavailable.
 * Scheduling (a timer) is deliberately not part of this command.
 * Output and limits: docs/WATCHDOG.md. A lead's state comes from one turn
 * record per agent name; a shared name with concurrent sessions can show idle
 * while another session runs.
 */
import { homedir } from "node:os";
import { join } from "node:path";
import { runWatchdog } from "./watchdog-shell.js";

export type CliArgs = Readonly<{ configPath: string; dryRun: boolean; redisUrl: string }>;
export type CliParse = Readonly<{ ok: true; args: CliArgs }> | Readonly<{ ok: false; reason: string }>;

const configHome = (env: NodeJS.ProcessEnv): string => env.XDG_CONFIG_HOME || join(homedir(), ".config");
export const stateHome = (env: NodeJS.ProcessEnv): string => env.XDG_STATE_HOME || join(homedir(), ".local", "state");

/** Pure argument parsing; defaults from the environment. */
export const parseWatchdogArgs = (argv: readonly string[], env: NodeJS.ProcessEnv): CliParse => {
  let once = false;
  let dryRun = false;
  let configPath = join(configHome(env), "gptqueue", "watchdog.json");
  let redisUrl = env.REDIS_URL || "redis://127.0.0.1:6379";
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    switch (arg) {
      case "--once": once = true; break;
      case "--dry-run": dryRun = true; break;
      case "--config":
      case "--redis-url": {
        const value = argv[i + 1];
        if (value === undefined || value.startsWith("--")) return { ok: false, reason: `${arg} needs a value` };
        if (arg === "--config") configPath = value; else redisUrl = value;
        i += 1;
        break;
      }
      default: return { ok: false, reason: `unknown argument ${String(arg)}` };
    }
  }
  return once ? { ok: true, args: { configPath, dryRun, redisUrl } } : { ok: false, reason: "--once is required (only single passes are supported)" };
};

const main = async (): Promise<void> => {
  const parsed = parseWatchdogArgs(process.argv.slice(2), process.env);
  if (!parsed.ok) {
    console.error(`usage: gptqueue-watchdog --once [--config PATH] [--dry-run] [--redis-url URL]\nerror: ${parsed.reason}`);
    process.exitCode = 2;
    return;
  }
  const result = await runWatchdog({ ...parsed.args, stateHome: stateHome(process.env), configHome: configHome(process.env) });
  console.log(JSON.stringify(result.output));
  process.exitCode = result.exitCode;
};

await main();
