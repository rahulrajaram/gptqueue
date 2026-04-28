#!/usr/bin/env node

import * as path from "node:path";
import * as pty from "node-pty";
import { IdleDetector } from "./idle-detector.js";
import { RedisWatcher } from "./redis-watcher.js";

function parseArgs(argv: string[]): { agent: string; cmd: string; args: string[] } {
  let agent = "";
  let cmdIndex = -1;

  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--agent" && i + 1 < argv.length) {
      agent = argv[++i];
    } else if (argv[i] === "--cmd" && i + 1 < argv.length) {
      cmdIndex = i + 1;
      break;
    }
  }

  if (!agent || cmdIndex === -1) {
    console.error("Usage: gptqueue-pty --agent <name> --cmd <command> [args...]");
    process.exit(1);
  }

  const cmd = argv[cmdIndex];
  const args = argv.slice(cmdIndex + 1);
  return { agent, cmd, args };
}

const { agent, cmd, args } = parseArgs(process.argv);

console.log(`[gptqueue-pty] Starting agent "${agent}" with command: ${cmd} ${args.join(" ")}`);

function agentAttributionEnv(agentName: string): Record<string, string> {
  return {
    AGENT_ATTRIBUTION_CALLER:
      process.env.AGENT_ATTRIBUTION_CALLER || "gptqueue-pty",
    AGENT_ATTRIBUTION_PROJECT:
      process.env.AGENT_ATTRIBUTION_PROJECT || path.basename(process.cwd()),
    AGENT_ATTRIBUTION_SESSION: process.env.AGENT_ATTRIBUTION_SESSION || agentName,
  };
}

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

let pendingInjection = false;

// Forward PTY output to stdout
ptyProcess.onData((data: string) => {
  process.stdout.write(data);
  idleDetector.onOutput();
});

// Forward stdin to PTY
if (process.stdin.isTTY) {
  process.stdin.setRawMode(true);
}
process.stdin.resume();
process.stdin.on("data", (data: Buffer) => {
  ptyProcess.write(data.toString());
});

// Handle terminal resize
process.stdout.on("resize", () => {
  ptyProcess.resize(
    process.stdout.columns || 80,
    process.stdout.rows || 24
  );
});

// When Redis watcher detects messages, wait for idle then inject
watcher.on("message", (count: number) => {
  if (pendingInjection) return;
  pendingInjection = true;

  const inject = () => {
    const prompt = `\nYou have ${count} pending message(s) in your GPTQueue inbox. Call the receive_message tool to process them.\n`;
    ptyProcess.write(prompt);
    pendingInjection = false;
  };

  if (idleDetector.idle) {
    inject();
  } else {
    idleDetector.once("idle", inject);
  }
});

// Handle PTY exit
ptyProcess.onExit(async ({ exitCode }) => {
  idleDetector.destroy();
  await watcher.stop();
  process.exit(exitCode);
});

// Graceful shutdown
async function shutdown() {
  idleDetector.destroy();
  await watcher.stop();
  ptyProcess.kill();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start watching Redis for incoming messages
watcher.start().catch((err) => {
  console.error("[gptqueue-pty] Redis watcher error:", err);
});
