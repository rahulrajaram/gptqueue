import { describe, expect, it } from "vitest";
import {
  BRACKETED_PASTE_BEGIN,
  BRACKETED_PASTE_END,
  terminalPromptBytes,
} from "../src/pty-wrapper/submit.js";

// Regression test for the Claude Code TUI submission bug (2026-09-28): a
// prompt terminated by a bare LF is left in the editor instead of submitted.
// The reliable framing is one bracketed paste containing the entire text plus
// a single trailing CR.
describe("terminalPromptBytes", () => {
  it("wraps the whole prompt in one bracketed paste and submits with CR", () => {
    const prompt = "You have 3 pending message(s) in your GPTQueue inbox.";
    const bytes = terminalPromptBytes(prompt);
    expect(bytes).toBe(`${BRACKETED_PASTE_BEGIN}${prompt}${BRACKETED_PASTE_END}\r`);
  });

  it("never emits a bare-LF submit: newlines stay inside the paste", () => {
    const bytes = terminalPromptBytes("line one\nline two\n");
    expect(bytes.endsWith("\r")).toBe(true);
    expect(bytes.endsWith("\n")).toBe(false);
    expect(bytes.startsWith(BRACKETED_PASTE_BEGIN)).toBe(true);
    expect(bytes.lastIndexOf(BRACKETED_PASTE_END)).toBe(bytes.length - BRACKETED_PASTE_END.length - 1);
  });

  it("uses exactly one CR and no LF outside the paste", () => {
    const bytes = terminalPromptBytes("no newlines here");
    expect(bytes.split("\r").length - 1).toBe(1);
    expect(bytes.includes("\n")).toBe(false);
  });
});
