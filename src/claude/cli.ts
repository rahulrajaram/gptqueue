/** Entry points behind the thin bin/ shims for the Claude Code integration. */
import { resolveAgentName } from "./agent-name.js";
import { runPeek } from "./peek.js";

export const claudeNameMain = (argv: readonly string[]): void => {
  process.stdout.write(`${resolveAgentName(argv[0])}\n`);
};

export const inboxPeekMain = async (argv: readonly string[]): Promise<void> => {
  process.exitCode = await runPeek(argv);
};
