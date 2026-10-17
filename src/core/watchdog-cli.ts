/**
 * `gptqueue-watchdog --once [--config PATH] [--dry-run] [--json] [--redis-url URL]`
 * `gptqueue-watchdog --status [--redis-url URL]`
 *
 * --once runs one watchdog pass (loop adapter S2). A quiet pass prints one
 * journal line; otherwise, or with --json, the full JSON report (W5).
 * Exit codes: 0 success, 2 usage or config problem, 1 Redis unavailable or
 * pass deadline. --status judges the last substantive (non-skipped) real pass:
 * exit 0 when it is under 3 minutes old, exited 0 and had no failed actions;
 * else 1, as `degraded` (failed actions) or `unhealthy`, with a reason (W6, W3).
 * Scheduling (a timer) is deliberately not part of this command.
 * Output and limits: docs/WATCHDOG.md. A lead's state comes from one turn
 * record per agent name; a shared name with concurrent sessions can show idle
 * while another session runs.
 */
import { Redis } from "ioredis";
import { runWatchdog, WATCHDOG_LAST_PASS, WATCHDOG_LAST_SUBSTANTIVE_PASS } from "./watchdog-shell.js";
import { configHome, evaluateWatchdogStatus, formatWatchdogOutput, parseWatchdogArgs, stateHome } from "./watchdog-report.js";

export { parseWatchdogArgs, stateHome } from "./watchdog-report.js";

const USAGE = "usage: gptqueue-watchdog --once [--config PATH] [--dry-run] [--json] [--redis-url URL] | --status [--redis-url URL]";

const status = async (redisUrl: string): Promise<number> => {
  const redis = new Redis(redisUrl, { lazyConnect: true, maxRetriesPerRequest: 0, enableOfflineQueue: false, connectTimeout: 3000, commandTimeout: 3000, retryStrategy: () => null });
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    const [substantive, last] = await redis.mget(WATCHDOG_LAST_SUBSTANTIVE_PASS, WATCHDOG_LAST_PASS);
    const evaluated = evaluateWatchdogStatus(substantive ?? null, last ?? null, new Date());
    console.log(JSON.stringify(evaluated.status));
    return evaluated.exitCode;
  } catch (error) {
    console.log(JSON.stringify({ state: "unhealthy", healthy: false, reason: `redis unavailable: ${error instanceof Error ? error.message : String(error)}`, last_pass: null }));
    return 1;
  } finally {
    redis.disconnect();
  }
};

const main = async (): Promise<void> => {
  const parsed = parseWatchdogArgs(process.argv.slice(2), process.env);
  if (!parsed.ok) {
    console.error(`${USAGE}\nerror: ${parsed.reason}`);
    process.exitCode = 2;
    return;
  }
  if (parsed.args.mode === "status") {
    process.exitCode = await status(parsed.args.redisUrl);
    return;
  }
  const result = await runWatchdog({ ...parsed.args, stateHome: stateHome(process.env), configHome: configHome(process.env) });
  console.log(formatWatchdogOutput(result, parsed.args.json));
  process.exitCode = result.exitCode;
};

await main();
