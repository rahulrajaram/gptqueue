import { createHash, randomBytes } from "node:crypto";
import { createConnection, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";

const MAX_MESSAGE = 16 * 1024 * 1024;
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
type Pending = Readonly<{ resolve(value: Record<string, unknown>): void; reject(error: Error): void }>;

/**
 * Resolve the app-server control endpoint once per client construction. The
 * override is intentionally process-local so an owned acceptance app-server
 * can be selected without changing Codex global configuration.
 */
export const defaultCodexSocketPath = (): string => {
  const configured = process.env.GPTQUEUE_CODEX_APP_SERVER_SOCKET?.trim();
  return configured || join(process.env.CODEX_HOME ?? join(homedir(), ".codex"), "app-server-control", "app-server-control.sock");
};

const frame = (payload: Buffer, opcode = 1): Buffer => {
  if (payload.length > MAX_MESSAGE) throw new Error("Codex message exceeds size limit");
  const mask = randomBytes(4);
  const header = Buffer.alloc(payload.length < 126 ? 2 : payload.length <= 65535 ? 4 : 10);
  header[0] = 0x80 | opcode;
  header[1] = 0x80 | (header.length === 2 ? payload.length : header.length === 4 ? 126 : 127);
  if (header.length === 4) header.writeUInt16BE(payload.length, 2);
  if (header.length === 10) header.writeBigUInt64BE(BigInt(payload.length), 2);
  const masked = Buffer.from(payload);
  for (let i = 0; i < masked.length; i++) masked[i] = masked[i]! ^ mask[i % 4]!;
  return Buffer.concat([header, mask, masked]);
};

/** Connects only to the existing local daemon; it never launches a Codex process. */
export class CodexSocketClient {
  private socket?: Socket;
  private opening?: Promise<void>;
  private buffer: Buffer = Buffer.alloc(0);
  private fragments: readonly Buffer[] = [];
  private fragmentBytes = 0;
  private next = 1;
  private readonly pending = new Map<number, Pending>();
  private closed = false;

  constructor(
    private readonly path = defaultCodexSocketPath(),
    private readonly timeoutMs = 10_000,
  ) {}

  private fail(error: Error): void {
    const socket = this.socket;
    this.socket = undefined;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentBytes = 0;
    for (const request of this.pending.values()) request.reject(error);
    this.pending.clear();
    socket?.destroy();
  }

  private async connect(): Promise<void> {
    if (this.closed) throw new Error("Codex client closed");
    const socket = createConnection(this.path);
    this.socket = socket;
    const key = randomBytes(16).toString("base64");
    const expected = createHash("sha1").update(key + GUID).digest("base64");
    await new Promise<void>((resolve, reject) => {
      let received = Buffer.alloc(0);
      const timer = setTimeout(() => finish(new Error("Codex handshake timeout")), this.timeoutMs);
      const cleanup = () => { clearTimeout(timer); socket.off("data", data); socket.off("error", error); socket.off("close", close); };
      const finish = (failure?: Error, remainder?: Buffer) => {
        cleanup();
        if (failure) { socket.destroy(); reject(failure); return; }
        socket.on("error", (cause) => this.fail(cause));
        socket.on("close", () => { if (this.socket === socket) this.fail(new Error("Codex app-server disconnected")); });
        socket.on("data", (chunk: Buffer) => {
          try { this.read(chunk); } catch { this.fail(new Error("Invalid Codex WebSocket message")); }
        });
        if (remainder?.length) {
          try { this.read(remainder); } catch { this.fail(new Error("Invalid Codex WebSocket message")); }
        }
        resolve();
      };
      const error = (cause: Error) => finish(cause);
      const close = () => finish(new Error("Codex handshake disconnected"));
      const data = (chunk: Buffer) => {
        received = Buffer.concat([received, chunk]);
        const end = received.indexOf("\r\n\r\n");
        if (end < 0) { if (received.length > 16_384) finish(new Error("Codex handshake exceeds size limit")); return; }
        const lines = received.subarray(0, end).toString("ascii").split("\r\n");
        const headers = new Map(lines.slice(1).map((line) => {
          const colon = line.indexOf(":");
          return [line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim()];
        }));
        if (!/^HTTP\/1\.1 101(?: |$)/u.test(lines[0] ?? "") || headers.get("sec-websocket-accept") !== expected ||
            headers.get("upgrade")?.toLowerCase() !== "websocket" ||
            !headers.get("connection")?.toLowerCase().split(/\s*,\s*/u).includes("upgrade")) {
          finish(new Error("Invalid Codex WebSocket handshake")); return;
        }
        finish(undefined, received.subarray(end + 4));
      };
      socket.on("data", data); socket.once("error", error); socket.once("close", close);
      socket.once("connect", () => socket.write(`GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`));
    });
    await this.sendRequest("initialize", { clientInfo: { name: "gptqueue", version: "1.0.0" }, capabilities: { experimentalApi: true } }, new AbortController().signal);
    this.write({ method: "initialized", params: {} });
  }

  private async ready(): Promise<void> {
    if (this.opening) return this.opening;
    if (this.socket && !this.closed) return;
    const opening = this.connect().catch((error: Error) => { this.fail(error); throw error; });
    this.opening = opening;
    try { await opening; } finally { if (this.opening === opening) this.opening = undefined; }
  }

  private read(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0]!;
      if (first & 0x70 || this.buffer[1]! & 0x80) throw new Error("Unexpected WebSocket flags");
      const opcode = first & 15;
      const final = !!(first & 0x80);
      let size = this.buffer[1]! & 127;
      let header = 2;
      if (size === 126) { if (this.buffer.length < 4) return; size = this.buffer.readUInt16BE(2); header = 4; }
      if (size === 127) {
        if (this.buffer.length < 10) return;
        const wide = this.buffer.readBigUInt64BE(2);
        if (wide > BigInt(MAX_MESSAGE)) throw new Error("WebSocket frame exceeds size limit");
        size = Number(wide); header = 10;
      }
      if (size > MAX_MESSAGE || this.fragmentBytes + size > MAX_MESSAGE) throw new Error("WebSocket message exceeds size limit");
      if (opcode >= 8 && (!final || size > 125)) throw new Error("Invalid WebSocket control frame");
      if (this.buffer.length < header + size) return;
      const payload = Buffer.from(this.buffer.subarray(header, header + size));
      this.buffer = this.buffer.subarray(header + size);
      if (opcode === 8) { this.fail(new Error("Codex app-server closed")); return; }
      if (opcode === 9) { this.socket?.write(frame(payload, 10)); continue; }
      if (opcode === 10) continue;
      if (opcode !== 0 && opcode !== 1) throw new Error("Unsupported WebSocket opcode");
      if (opcode === 0 && !this.fragments.length || opcode === 1 && this.fragments.length) throw new Error("Invalid WebSocket continuation");
      this.fragments = [...this.fragments, payload]; this.fragmentBytes += payload.length;
      if (!final) continue;
      const message = JSON.parse(Buffer.concat(this.fragments).toString("utf8")) as Record<string, unknown>;
      this.fragments = []; this.fragmentBytes = 0;
      if (typeof message.id !== "number") continue; // Unsubscribed notifications are intentionally not buffered.
      if (typeof message.method === "string") continue; // Server requests are not client responses; never auto-approve them.
      if (!Object.prototype.hasOwnProperty.call(message, "result") && !Object.prototype.hasOwnProperty.call(message, "error")) continue;
      const pending = this.pending.get(message.id);
      if (!pending) continue;
      if (message.error) pending.reject(new Error(`Codex RPC rejected: ${JSON.stringify(message.error)}`));
      else pending.resolve((message.result ?? {}) as Record<string, unknown>);
    }
  }

  private write(value: Record<string, unknown>): void {
    if (!this.socket || this.closed) throw new Error("Codex socket unavailable");
    this.socket.write(frame(Buffer.from(JSON.stringify(value))));
  }

  private sendRequest(method: string, params: Record<string, unknown>, signal: AbortSignal): Promise<Record<string, unknown>> {
    signal.throwIfAborted();
    const id = this.next++;
    return new Promise((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); signal.removeEventListener("abort", abort); this.pending.delete(id); };
      const fail = (error: Error) => { cleanup(); reject(error); };
      const abort = () => fail(new Error("Codex RPC aborted"));
      const timer = setTimeout(() => fail(new Error("Codex RPC timeout")), this.timeoutMs);
      this.pending.set(id, { resolve: (value) => { cleanup(); resolve(value); }, reject: fail });
      signal.addEventListener("abort", abort, { once: true });
      try { this.write({ id, method, params }); } catch (error) { fail(error as Error); }
    });
  }

  async request(method: string, params: Record<string, unknown>, signal: AbortSignal): Promise<Record<string, unknown>> {
    signal.throwIfAborted();
    await this.ready();
    signal.throwIfAborted();
    return this.sendRequest(method, params, signal);
  }

  async close(): Promise<void> {
    this.closed = true;
    this.fail(new Error("Codex client closed"));
  }
}
