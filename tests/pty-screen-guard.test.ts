import { describe, expect, it } from "vitest";
import { CLAUDE_PROFILE, GENERIC_PROFILE, NUDGE_IDLE, ScreenGuard, classifyScreen, decideNudge, onArrival, onNudged, profileFor, stripAnsi } from "../src/pty-wrapper/screen-guard.js";

// Synthetic, recorded-shape terminal output (written for these tests; not captured sessions).
const ESC = "\u001b";
const IDLE_PROMPT = `${ESC}[2J${ESC}[H╭──────────────────────────╮\r\n│ ${ESC}[2m>${ESC}[22m                        │\r\n╰──────────────────────────╯\r\n  ${ESC}[2m? for shortcuts${ESC}[22m\r\n`;
const PERMISSION_DIALOG = `${ESC}[2K╭──────────────────────────╮\r\n│ Bash command             │\r\n│   rm -rf build           │\r\n│ Do you want to proceed?  │\r\n│ ${ESC}[36m❯ 1. Yes${ESC}[39m                 │\r\n│   2. No, and tell Claude │\r\n╰──────────────────────────╯\r\n  Esc to cancel\r\n`;
const WORKING = `${ESC}[2K✻ Thinking… (3s · esc to interrupt)\r\n`;

describe("PTY screen guard (WK-9)", () => {
  it("strips CSI/OSC sequences", () => {
    expect(stripAnsi(`${ESC}]0;title\u0007a${ESC}[31mb${ESC}[0m`)).toBe("ab");
  });

  it("allows injection only at a recognised empty Claude prompt", () => {
    const g = new ScreenGuard(CLAUDE_PROFILE);
    expect(g.verdict()).toEqual({ safe: false, reason: "no_prompt_marker" });
    g.onOutput(IDLE_PROMPT);
    expect(g.verdict()).toEqual({ safe: true });
  });

  it("refuses while a permission dialog is drawn after the prompt, and recovers after a redraw", () => {
    const g = new ScreenGuard(CLAUDE_PROFILE);
    g.onOutput(IDLE_PROMPT);
    g.onOutput(PERMISSION_DIALOG);
    expect(g.verdict()).toEqual({ safe: false, reason: "dialog" });
    g.onOutput(IDLE_PROMPT); // full clear + prompt
    expect(g.verdict()).toEqual({ safe: true });
  });

  it("refuses while Claude is working", () => {
    const g = new ScreenGuard(CLAUDE_PROFILE);
    g.onOutput(IDLE_PROMPT);
    g.onOutput(WORKING);
    expect(g.verdict().safe).toBe(false);
  });

  it("refuses while the user has half-typed input, until it is submitted or abandoned", () => {
    const g = new ScreenGuard(CLAUDE_PROFILE);
    g.onOutput(IDLE_PROMPT);
    g.onUserInput("fix the te");
    expect(g.verdict()).toEqual({ safe: false, reason: "user_typing" });
    g.onUserInput("st\r");
    expect(g.verdict()).toEqual({ safe: true });
    g.onUserInput("draft");
    g.onUserInput("\u0015"); // Ctrl-U clears the line
    expect(g.verdict().safe).toBe(true);
    g.onUserInput(`${ESC}[A`); // arrow key is not typing
    expect(g.verdict().safe).toBe(true);
  });

  it("generic profile vetoes only dialogs and typing", () => {
    expect(profileFor("/usr/bin/claude")).toBe(CLAUDE_PROFILE);
    expect(profileFor("codex")).toBe(GENERIC_PROFILE);
    expect(classifyScreen("$ ", GENERIC_PROFILE, false)).toEqual({ safe: true });
    expect(classifyScreen("Do you want to continue? (y/n)", GENERIC_PROFILE, false).safe).toBe(false);
  });
});

describe("bounded re-nudge", () => {
  const policy = { retryMs: 100, renudgeMs: 1000, maxNudges: 2 };
  const safe = { safe: true } as const;

  it("waits on unsafe screens, re-nudges after the interval, and gives up after the budget", () => {
    let s = onArrival(NUDGE_IDLE);
    expect(decideNudge(s, true, { safe: false, reason: "dialog" }, 0, policy)).toEqual({ kind: "wait", ms: 100, reason: "dialog" });
    expect(decideNudge(s, false, safe, 0, policy)).toMatchObject({ kind: "wait", reason: "output_active" });
    expect(decideNudge(s, true, safe, 0, policy)).toEqual({ kind: "inject" });
    s = onNudged(s, 0);
    expect(decideNudge(s, true, safe, 500, policy)).toEqual({ kind: "wait", ms: 500, reason: "renudge_interval" });
    expect(decideNudge(s, true, safe, 1000, policy)).toEqual({ kind: "inject" });
    s = onNudged(s, 1000);
    expect(decideNudge(s, true, safe, 5000, policy)).toEqual({ kind: "give_up" });
    expect(decideNudge(onArrival(s), true, safe, 5000, policy)).toEqual({ kind: "inject" });
    expect(decideNudge(NUDGE_IDLE, true, safe, 0, policy)).toEqual({ kind: "none" });
  });
});
