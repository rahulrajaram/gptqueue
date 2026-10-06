#!/usr/bin/env node

import * as pty from "node-pty";
import { agentAttributionEnv } from "./attribution.js";
import { IdleDetector } from "./idle-detector.js";
import { RedisWatcher } from "./redis-watcher.js";
import { terminalPromptBytes } from "./submit.js";
import { DEFAULT_NUDGE_POLICY, NUDGE_IDLE, ScreenGuard, decideNudge, onArrival, onDrained, onNudged, profileFor } from "./screen-guard.js";

function parseArgs(argv: string[]): { agent: string; cmd: string; args: string[] } {
  let agent = "";
  let cmdIndex = -1;

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--agent" && i + 1 < argv.length) {
      agent = argv[++i] ?? "";
    } else if (arg === "--cmd" && i + 1 < argv.length) {
      cmdIndex = i + 1;
      break;
    }
  }

  if (!agent || cmdIndex === -1) {
    console.error("Usage: gptqueue-pty --agent <name> --cmd <command> [args...]");
    process.exit(1);
  }

  const cmd = argv[cmdIndex] ?? "";
  const args = argv.slice(cmdIndex + 1);
  return { agent, cmd, args };
}

const { agent, cmd, args } = parseArgs(process.argv);

console.log(`[gptqueue-pty] Starting agent "${agent}" with command: ${cmd} ${args.join(" ")}`);

// Spawn the wrapped CLI in a PTY
const ptyProcess = pty.spawn(cmd, args, {
  name: "xterm-color",
  cols: process.stdout.columns || 80,
  rows: process.stdout.rows || 24,
  cwd: process.cwd(),
  env: {
    ...process.env,
    GPTQ_AGENT_NAME: agent,
    ...agentAttributionEnv(agent),
  } as Record<string, string>,
});

const idleDetector = new IdleDetector(2000);
const watcher = new RedisWatcher(agent);

const guard = new ScreenGuard(profileFor(cmd));

// Forward PTY output to stdout
ptyProcess.onData((data: string) => {
  process.stdout.write(data);
  guard.onOutput(data);
  idleDetector.onOutput();
});

// Forward stdin to PTY
if (process.stdin.isTTY) {
  process.stdin.setRawMode(true);
}
process.stdin.resume();
process.stdin.on("data", (data: Buffer) => {
  guard.onUserInput(data.toString());
  ptyProcess.write(data.toString());
});

// Handle terminal resize
process.stdout.on("resize", () => {
  ptyProcess.resize(
    process.stdout.columns || 80,
    process.stdout.rows || 24
  );
});

// When mail is pending, inject only into a quiet, recognised empty input
// prompt (WK-9), retrying while unsure, and re-nudge a bounded number of times
// while the queue stays non-empty.
let nudge = NUDGE_IDLE;
let lastCount = 0;
let nudgeTimer: ReturnType<typeof setTimeout> | null = null;
const schedule = (ms: number) => {
  if (nudgeTimer) clearTimeout(nudgeTimer);
  nudgeTimer = setTimeout(() => void step(), ms);
};
let warnedNoKnownPrompt = false;
const step = async (): Promise<void> => {
  nudgeTimer = null;
  if (nudge.nudges > 0) {
    const depth = await watcher.depth().catch(() => null);
    if (depth === 0) { nudge = onDrained(); lastCount = 0; return; }
    if (depth !== null) lastCount = depth;
  }
  const action = decideNudge(nudge, idleDetector.idle, guard.verdict(), Date.now());
  switch (action.kind) {
    case "inject": {
      const prompt = `You have ${lastCount} pending message(s) in your GPTQueue inbox. Call claim_tasks to claim a batch (optional max_batch and ttl_seconds), process the tasks, then acknowledge them with acknowledge_tasks (claim_id).`;
      // Bracketed paste + CR submits the prompt in the wrapped TUI; a bare LF
      // does NOT submit (proven against Claude Code CLI 2.1.283, 2026-09-28).
      ptyProcess.write(terminalPromptBytes(prompt));
      guard.onInjected();
      nudge = onNudged(nudge, Date.now());
      schedule(DEFAULT_NUDGE_POLICY.renudgeMs);
      return;
    }
    case "wait":
      if (action.reason === "no_known_prompt" && !warnedNoKnownPrompt) {
        warnedNoKnownPrompt = true;
        process.stderr.write("[gptqueue] not injecting inbox prompts: this CLI has no known empty-prompt marker, so the screen can never be proven safe; mail stays queued for the agent to claim\n");
      }
      schedule(action.ms);
      return;
    case "give_up": nudge = NUDGE_IDLE; return; // new mail re-arms
    case "none": return;
    default: { const unreachable: never = action; throw new Error(String(unreachable)); }
  }
};

watcher.on("message", (count: number) => {
  // The polling fallback re-reports a non-empty queue; only growth is new mail.
  if (!nudge.pending || count > lastCount) nudge = onArrival(nudge);
  lastCount = count;
  if (!nudgeTimer) schedule(0);
});

// Stopping the watcher talks to Redis, which may be down; never let that
// block or skip process exit.
const stopWatcher = (): Promise<void> =>
  Promise.race([
    watcher.stop().catch(() => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, 2_000).unref()),
  ]);

// Handle PTY exit
ptyProcess.onExit(async ({ exitCode }) => {
  if (nudgeTimer) clearTimeout(nudgeTimer);
  idleDetector.destroy();
  await stopWatcher();
  process.exit(exitCode);
});

// Graceful shutdown: kill the wrapped child first, so a Redis failure while
// stopping the watcher can never leave it running.
async function shutdown() {
  if (nudgeTimer) clearTimeout(nudgeTimer);
  idleDetector.destroy();
  ptyProcess.kill();
  await stopWatcher();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start watching Redis for incoming messages
watcher.start().catch((err) => {
  console.error("[gptqueue-pty] Redis watcher error:", err);
});
