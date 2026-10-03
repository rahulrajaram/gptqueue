import { afterEach, describe, expect, it, vi } from "vitest";
import { IdleDetector } from "../src/pty-wrapper/idle-detector.js";

describe("PTY IdleDetector", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("goes idle only after a full quiet period, and output restarts the clock", () => {
    vi.useFakeTimers();
    const detector = new IdleDetector(100);
    let idleEvents = 0;
    detector.on("idle", () => { idleEvents += 1; });

    detector.onOutput();
    vi.advanceTimersByTime(99);
    expect(detector.idle).toBe(false);
    detector.onOutput(); // more output resets the quiet period
    vi.advanceTimersByTime(99);
    expect(idleEvents).toBe(0);
    vi.advanceTimersByTime(1);
    expect(detector.idle).toBe(true);
    expect(idleEvents).toBe(1);

    detector.onOutput();
    expect(detector.idle).toBe(false);
    detector.destroy();
    vi.advanceTimersByTime(1000);
    expect(idleEvents).toBe(1); // destroy cancels the pending timer
  });
});
