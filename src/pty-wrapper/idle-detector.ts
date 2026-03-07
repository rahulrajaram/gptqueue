import { EventEmitter } from "events";

export class IdleDetector extends EventEmitter {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private readonly quiescenceMs: number;
  private _idle = false;

  constructor(quiescenceMs: number = 2000) {
    super();
    this.quiescenceMs = quiescenceMs;
  }

  /** Call this whenever the wrapped process produces output. */
  onOutput(): void {
    this._idle = false;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this._idle = true;
      this.emit("idle");
    }, this.quiescenceMs);
  }

  get idle(): boolean {
    return this._idle;
  }

  destroy(): void {
    if (this.timer) clearTimeout(this.timer);
  }
}
