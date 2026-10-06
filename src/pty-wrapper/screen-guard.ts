/**
 * Screen-state guard for PTY prompt injection (WK-9). "No output for 2 s" is
 * not enough: a permission dialog or a half-typed user prompt is also quiet,
 * and the injected CR would answer the dialog or submit the user's text.
 *
 * The guard keeps a bounded tail of the wrapped program's output (ANSI
 * stripped) and the user's unsubmitted keystrokes, and classifies the screen.
 * It only answers "safe" when it positively recognises an empty input prompt;
 * anything it is unsure about is "unsafe" and the caller retries later.
 */

const TAIL_LIMIT = 8192;

// CSI, OSC and single-char escape sequences.
// eslint-disable-next-line no-control-regex
const ANSI = /\u001b\[[0-?]*[ -/]*[@-~]|\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)|\u001b[@-Z\\-_]/g;

export const stripAnsi = (text: string): string => text.replace(ANSI, "");

export type ScreenProfile = Readonly<{
  /** Text shown only while the input prompt is empty and idle. Null: no positive marker known. */
  promptMarkers: readonly RegExp[];
  /** Text shown by dialogs/menus that a CR would answer. */
  dialogMarkers: readonly RegExp[];
}>;

/**
 * Claude Code: the footer hint "? for shortcuts" is drawn only while the
 * input box is empty and idle; permission and selection dialogs draw
 * "Do you want to ...", numbered "❯ 1." choices and "Esc to cancel".
 */
export const CLAUDE_PROFILE: ScreenProfile = Object.freeze({
  promptMarkers: Object.freeze([/\? for shortcuts/]),
  dialogMarkers: Object.freeze([/Do you want to/i, /❯\s*\d+\./, /Esc to cancel/i, /\(y\/n\)/i, /Press Enter to continue/i, /esc to interrupt/i]),
});

/**
 * Other CLIs: no known positive empty-prompt marker, so the guard can never
 * prove the screen safe and never auto-injects (fail closed). Dialog markers
 * still classify the reason when one is visible.
 */
export const GENERIC_PROFILE: ScreenProfile = Object.freeze({
  promptMarkers: Object.freeze([]),
  dialogMarkers: CLAUDE_PROFILE.dialogMarkers,
});

export const profileFor = (cmd: string): ScreenProfile =>
  /(^|\/)claude$/.test(cmd) ? CLAUDE_PROFILE : GENERIC_PROFILE;

export type ScreenVerdict =
  | Readonly<{ safe: true }>
  | Readonly<{ safe: false; reason: "user_typing" | "dialog" | "no_prompt_marker" | "no_known_prompt" }>;

const lastIndex = (text: string, patterns: readonly RegExp[]): number => {
  let best = -1;
  for (const pattern of patterns) {
    const global = new RegExp(pattern.source, pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`);
    for (const match of text.matchAll(global)) best = Math.max(best, match.index ?? -1);
  }
  return best;
};

/**
 * Pure classification of a stripped output tail plus the user's pending input.
 * `promptFrom`: tail index of the first output drawn after the user's last
 * keystroke. A prompt marker before it predates that keystroke (Ink redraws
 * by erasing lines, so an old marker stays in the tail) and proves nothing.
 */
export const classifyScreen = (tail: string, profile: ScreenProfile, userTyping: boolean, promptFrom = 0): ScreenVerdict => {
  if (userTyping) return { safe: false, reason: "user_typing" };
  const marker = lastIndex(tail, profile.promptMarkers);
  const prompt = marker >= promptFrom ? marker : -1;
  const dialog = lastIndex(tail, profile.dialogMarkers);
  // TUIs redraw in place, so the most recently drawn marker describes the screen.
  if (dialog > prompt) return { safe: false, reason: "dialog" };
  // Fail closed: only a positively recognised empty prompt is safe.
  if (profile.promptMarkers.length === 0) return { safe: false, reason: "no_known_prompt" };
  if (prompt < 0) return { safe: false, reason: "no_prompt_marker" };
  return { safe: true };
};

const hasPrintable = (text: string): boolean =>
  [...text].some((ch) => { const code = ch.codePointAt(0) ?? 0; return code >= 0x20 && code !== 0x7f; });

// One user keystroke (or escape sequence) per token. CSI sequences (arrows,
// bracketed-paste brackets) and ESC+CR (Shift+Enter after /terminal-setup)
// must be recognised before a bare ESC or CR.
// eslint-disable-next-line no-control-regex
const KEY_TOKEN = /\u001b\[[0-?]*[ -/]*[@-~]|\u001bO.|\u001b\r|[\s\S]/gu;

export class ScreenGuard {
  private tail = "";
  /** Stripped output characters appended so far (monotonic; the tail is its suffix). */
  private outputPos = 0;
  /** outputPos at the user's last keystroke (or the last injection). */
  private inputPos = 0;
  private typing = false;
  /** Last key of the previous input chunk: keys usually arrive one per chunk. */
  private lastKey = "";
  /** Inside a user bracketed paste, CR and LF are pasted text, never a submit. */
  private pasting = false;

  constructor(private readonly profile: ScreenProfile) {}

  /** Record output from the wrapped program. */
  onOutput(data: string): void {
    // A full clear starts a new screen; earlier markers no longer describe it.
    const cleared = data.lastIndexOf("\u001b[2J");
    const chunk = stripAnsi(cleared >= 0 ? data.slice(cleared) : data);
    this.tail = (cleared >= 0 ? chunk : this.tail + chunk).slice(-TAIL_LIMIT);
    this.outputPos += chunk.length;
  }

  /**
   * Record keystrokes the user sent to the wrapped program. Only a bare CR
   * submits; backslash+CR, ESC+CR and LF (Ctrl+J) insert a newline into a
   * multi-line draft, and a lone Esc does not clear Claude's prompt, so none
   * of them ends `typing` (SEC-F3). Ctrl-C and Ctrl-U abandon the line.
   */
  onUserInput(data: string): void {
    this.inputPos = this.outputPos;
    for (const key of data.match(KEY_TOKEN) ?? []) {
      const previous = this.lastKey;
      this.lastKey = key;
      if (key === "\u001b[200~") { this.pasting = true; continue; }
      if (key === "\u001b[201~") { this.pasting = false; continue; }
      if (this.pasting) { if (hasPrintable(key) || key === "\r" || key === "\n") this.typing = true; continue; }
      if (key === "\r") {
        if (previous === "\\" || previous === "\u001b") this.typing = true; // newline insertion
        else this.typing = false; // submit
      } else if (key === "\u0003" || key === "\u0015") {
        this.typing = false;
      } else if (key === "\n" || key === "\u001b\r") {
        this.typing = true; // newline insertion: the box is not empty
      } else if (hasPrintable(key) && !key.startsWith("\u001b")) {
        this.typing = true; // printable text, not bare cursor/escape keys
      }
    }
  }

  /** The injected prompt itself is submitted with CR; it leaves no pending input. */
  onInjected(): void { this.typing = false; this.lastKey = "\r"; this.inputPos = this.outputPos; }

  verdict(): ScreenVerdict {
    const tailStart = this.outputPos - this.tail.length;
    return classifyScreen(this.tail, this.profile, this.typing, Math.max(0, this.inputPos - tailStart));
  }
}

export type NudgePolicy = Readonly<{ retryMs: number; renudgeMs: number; maxNudges: number }>;
export const DEFAULT_NUDGE_POLICY: NudgePolicy = Object.freeze({ retryMs: 1000, renudgeMs: 60_000, maxNudges: 5 });

export type NudgeState = Readonly<{ pending: boolean; nudges: number; lastNudgeAt: number | null }>;
export const NUDGE_IDLE: NudgeState = Object.freeze({ pending: false, nudges: 0, lastNudgeAt: null });

export type NudgeAction =
  | Readonly<{ kind: "inject" }>
  | Readonly<{ kind: "wait"; ms: number; reason: string }>
  | Readonly<{ kind: "give_up" }>
  | Readonly<{ kind: "none" }>;

/** New mail re-arms the nudge budget. */
export const onArrival = (state: NudgeState): NudgeState => Object.freeze({ pending: true, nudges: 0, lastNudgeAt: state.lastNudgeAt });
/** The queue drained: nothing left to nudge about. */
export const onDrained = (): NudgeState => NUDGE_IDLE;
export const onNudged = (state: NudgeState, now: number): NudgeState => Object.freeze({ pending: true, nudges: state.nudges + 1, lastNudgeAt: now });

/**
 * Decide the next step while mail is pending: inject only when the session is
 * quiet AND the screen is a recognised empty prompt; re-nudge at most
 * maxNudges times, renudgeMs apart, while the queue stays non-empty.
 */
export const decideNudge = (state: NudgeState, idle: boolean, screen: ScreenVerdict, now: number, policy: NudgePolicy = DEFAULT_NUDGE_POLICY): NudgeAction => {
  if (!state.pending) return { kind: "none" };
  if (state.nudges >= policy.maxNudges) return { kind: "give_up" };
  if (state.nudges > 0 && state.lastNudgeAt !== null && now - state.lastNudgeAt < policy.renudgeMs) {
    return { kind: "wait", ms: policy.renudgeMs - (now - state.lastNudgeAt), reason: "renudge_interval" };
  }
  if (!idle) return { kind: "wait", ms: policy.retryMs, reason: "output_active" };
  if (!screen.safe) return { kind: "wait", ms: policy.retryMs, reason: screen.reason };
  return { kind: "inject" };
};
