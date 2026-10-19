/**
 * Stop/SessionStart/UserPromptSubmit/PreToolUse hook entry. Runs at the end of every Claude turn, so it
 * imports only the hook, probe and record modules (plus ioredis): no MCP
 * server, SDK, zod or peek code.
 */
import { readFileSync } from "node:fs";
import { CLI_EVENTS, hookBinDir, runClaudeHook } from "./hook.js";

let stdin = "";
try { stdin = readFileSync(0, "utf8"); } catch { /* no stdin */ }
const argv = process.argv.slice(2);
if (!CLI_EVENTS.some((event) => event === argv[0])) {
  process.stderr.write(`usage: gptqueue-claude-hook ${CLI_EVENTS.join("|")}\n  pre-tool-use is the opt-in AskUserQuestion guard (PreToolUse matcher "AskUserQuestion").\n  GPTQUEUE_WORKER=1 forces headless worker mode (every event a no-op); =0 opts a print-mode session in.\n`);
  process.exitCode = 2;
} else {
  const { stdout, exitCode } = await runClaudeHook(argv, stdin, hookBinDir(process.argv[1] ?? "."));
  if (stdout !== null) process.stdout.write(`${stdout}\n`);
  process.exitCode = exitCode;
}
