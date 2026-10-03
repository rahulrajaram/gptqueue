// Terminal prompt submission framing for interactive TUI CLIs.
//
// Proven bug (2026-09-28, Claude Code CLI 2.1.283, live/cell-claude-pi-attempt1):
// writing prompt text terminated by LF does NOT submit in the Claude TUI — the
// text stays in the editor. The reliable submission is a bracketed paste
// (ESC[200~ ... ESC[201~) followed by CR.
//
// This module is pure: it only computes the framed bytes. Callers remain
// responsible for validating the caller-supplied text (e.g. refusing unsafe
// control characters); the framing escape sequences added here are trusted.

/** Terminal control sequence that begins a bracketed paste. */
export const BRACKETED_PASTE_BEGIN = "\u001b[200~";
/** Terminal control sequence that ends a bracketed paste. */
export const BRACKETED_PASTE_END = "\u001b[201~";

/**
 * Frame prompt text for terminal submission: one bracketed paste containing
 * the ENTIRE text (newlines inside the text stay inside the paste; they never
 * act as submit keys), followed by a single CR that submits the prompt.
 */
export const terminalPromptBytes = (text: string): string =>
  `${BRACKETED_PASTE_BEGIN}${text}${BRACKETED_PASTE_END}\r`;
