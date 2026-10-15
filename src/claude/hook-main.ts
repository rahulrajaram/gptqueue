/**
 * Stop/SessionStart/UserPromptSubmit hook entry. Runs at the end of every Claude turn, so it
 * imports only the hook, probe and record modules (plus ioredis): no MCP
 * server, SDK, zod or peek code.
 */
import { readFileSync } from "node:fs";
import { hookBinDir, runClaudeHook } from "./hook.js";

let stdin = "";
try { stdin = readFileSync(0, "utf8"); } catch { /* no stdin */ }
const argv = process.argv.slice(2);
if (argv[0] !== "session-start" && argv[0] !== "stop" && argv[0] !== "user-prompt-submit") {
  process.stderr.write("usage: gptqueue-claude-hook session-start|stop|user-prompt-submit\n");
  process.exitCode = 2;
} else {
  const { stdout, exitCode } = await runClaudeHook(argv, stdin, hookBinDir(process.argv[1] ?? "."));
  if (stdout !== null) process.stdout.write(`${stdout}\n`);
  process.exitCode = exitCode;
}
