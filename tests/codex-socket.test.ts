import { describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { CodexSocketClient, defaultCodexSocketPath } from "../src/registered-shell/codex-socket.js";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const out = (body: Buffer) => Buffer.concat([Buffer.from([0x81, body.length]), body]);
const makeServer = async (valid = true) => { const root=await mkdtemp(join(tmpdir(),"gptq-sock-"));const path=join(root,"sock");const srv=createServer(s=>{let b=Buffer.alloc(0),ready=false;s.on("data",(chunk: Buffer)=>{b=Buffer.concat([b,chunk]);if(!ready){const e=b.indexOf("\r\n\r\n");if(e<0)return;const h=b.subarray(0,e).toString();const k=/Sec-WebSocket-Key: (.+)/i.exec(h)?.[1]?.trim()??"";const a=createHash("sha1").update(k+GUID).digest("base64");s.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${valid?a:"bad"}\r\n\r\n`);b=b.subarray(e+4);ready=true;}while(b.length>=2){const code=b[1]!&127;let h=2,n=code;if(code===126){if(b.length<4)return;n=b.readUInt16BE(2);h=4}if(b.length<h+4+n)return;const mask=b.subarray(h,h+4);h+=4;const p=Buffer.alloc(n);for(let i=0;i<n;i++)p[i]=b[h+i]!^mask[i%4]!;b=b.subarray(h+n);const m=JSON.parse(p.toString());s.write(out(Buffer.from(JSON.stringify({jsonrpc:"2.0",id:m.id,result:{ok:true}}))));}});});await new Promise<void>(r=>srv.listen(path,r));return{root,path,srv};};
const makeCollisionServer = async (collision: "server-request" | "empty") => {
 const root=await mkdtemp(join(tmpdir(),"gptq-sock-collision-"));const path=join(root,"sock");
 const srv=createServer(s=>{let b=Buffer.alloc(0),ready=false;s.on("data",(chunk: Buffer)=>{b=Buffer.concat([b,chunk]);if(!ready){const e=b.indexOf("\r\n\r\n");if(e<0)return;const h=b.subarray(0,e).toString();const k=/Sec-WebSocket-Key: (.+)/i.exec(h)?.[1]?.trim()??"";const a=createHash("sha1").update(k+GUID).digest("base64");s.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${a}\r\n\r\n`);b=b.subarray(e+4);ready=true;}while(b.length>=2){const code=b[1]!&127;let h=2,n=code;if(code===126){if(b.length<4)return;n=b.readUInt16BE(2);h=4}if(b.length<h+4+n)return;const mask=b.subarray(h,h+4);h+=4;const p=Buffer.alloc(n);for(let i=0;i<n;i++)p[i]=b[h+i]!^mask[i%4]!;b=b.subarray(h+n);const m=JSON.parse(p.toString()) as { id?: number; method?: string };if(m.method==="thread/read"){const malformed=collision==="server-request"?{jsonrpc:"2.0",id:m.id,method:"server/request",params:{approval:"required"}}:{jsonrpc:"2.0",id:m.id};s.write(out(Buffer.from(JSON.stringify(malformed))));}s.write(out(Buffer.from(JSON.stringify({jsonrpc:"2.0",id:m.id,result:{ok:true}}))));}});});
 await new Promise<void>(r=>srv.listen(path,r));return{root,path,srv};
};
const serverFrame = (payload: Buffer, opcode = 1, final = true): Buffer => {
 const header = Buffer.alloc(payload.length < 126 ? 2 : 4);
 header[0] = (final ? 0x80 : 0) | opcode;
 if (payload.length < 126) header[1] = payload.length;
 else { header[1] = 126; header.writeUInt16BE(payload.length, 2); }
 return Buffer.concat([header, payload]);
};
const makeBoundaryServer = async (mode: "fragment" | "ping" | "close-on-request") => {
 const root = await mkdtemp(join(tmpdir(), "gptq-sock-boundary-"));
 const path = join(root, "sock");
 let connectionCount = 0;
 let pongPayload: Buffer | undefined;
 let pongMasked = false;
 let pongResolve: (() => void) | undefined;
 const pongObserved = new Promise<void>(resolve => { pongResolve = resolve; });
 const srv = createServer(socket => {
  connectionCount++;
  const connection = connectionCount;
  let buffer = Buffer.alloc(0), ready = false;
  socket.on("data", (chunk: Buffer) => {
   buffer = Buffer.concat([buffer, chunk]);
   if (!ready) {
    const end = buffer.indexOf("\r\n\r\n"); if (end < 0) return;
    const headers = buffer.subarray(0, end).toString();
    const key = /Sec-WebSocket-Key: (.+)/i.exec(headers)?.[1]?.trim() ?? "";
    const accept = createHash("sha1").update(key + GUID).digest("base64");
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
    buffer = buffer.subarray(end + 4); ready = true;
   }
   while (buffer.length >= 2) {
    const opcode = buffer[0]! & 15, code = buffer[1]! & 127;
    const masked = !!(buffer[1]! & 0x80);
    let header = 2, size = code;
    if (code === 126) { if (buffer.length < 4) return; size = buffer.readUInt16BE(2); header = 4; }
    if (buffer.length < header + 4 + size) return;
    const mask = buffer.subarray(header, header + 4); header += 4;
    const payload = Buffer.alloc(size);
    for (let i = 0; i < size; i++) payload[i] = buffer[header + i]! ^ mask[i % 4]!;
    buffer = buffer.subarray(header + size);
    if (opcode === 9) { socket.write(serverFrame(payload, 10)); continue; }
    if (opcode === 10) { pongPayload = payload; pongMasked = masked; pongResolve?.(); continue; }
    if (opcode !== 1) continue;
    const message = JSON.parse(payload.toString()) as { id?: number; method?: string };
    if (message.method === "initialized") continue;
    if (message.method !== "initialize") {
     if (mode === "close-on-request" && connection === 1) { socket.end(); continue; }
     if (mode === "ping") socket.write(serverFrame(Buffer.from("gptqueue-ping"), 9));
     const response = Buffer.from(JSON.stringify({ jsonrpc: "2.0", id: message.id, result: { ok: true } }));
     if (mode === "fragment" && connection === 1) {
      const split = Math.floor(response.length / 2);
      socket.write(serverFrame(response.subarray(0, split), 1, false));
      socket.write(serverFrame(response.subarray(split), 0));
     } else socket.write(serverFrame(response));
    } else socket.write(serverFrame(Buffer.from(JSON.stringify({ jsonrpc: "2.0", id: message.id, result: {} }))));
   }
  });
 });
 await new Promise<void>(resolve => srv.listen(path, resolve));
 return { root, path, srv, pongObserved, get connectionCount() { return connectionCount; }, get pongPayload() { return pongPayload; }, get pongMasked() { return pongMasked; } };
};
const boundedSignal = () => AbortSignal.timeout(1_000);
const awaitPong = async (observed: Promise<void>): Promise<void> => {
 let timer: ReturnType<typeof setTimeout> | undefined;
 try {
  await Promise.race([observed, new Promise<never>((_, reject) => {
   timer = setTimeout(() => reject(new Error("pong observation timeout")), 1_000);
  })]);
 } finally { clearTimeout(timer); }
};
const closeServer = (srv: ReturnType<typeof createServer>) => new Promise<void>(resolve => srv.close(() => resolve()));
describe("Codex socket",()=>{
 it("performs valid handshake and correlates requests",async()=>{const x=await makeServer();const c=new CodexSocketClient(x.path);expect(await c.request("thread/read",{payload:"x".repeat(300)},new AbortController().signal)).toEqual({ok:true});await c.close();x.srv.close();await rm(x.root,{recursive:true,force:true});});
 it("uses the process-local owned socket override for default clients",async()=>{
  const x=await makeServer();
  vi.stubEnv("GPTQUEUE_CODEX_APP_SERVER_SOCKET",x.path);
  let c: CodexSocketClient | undefined;
  try {
   expect(defaultCodexSocketPath()).toBe(x.path);
   c=new CodexSocketClient();
   expect(await c.request("thread/read",{},new AbortController().signal)).toEqual({ok:true});
  } finally {
   await c?.close();
   vi.unstubAllEnvs();
   x.srv.close();
   await rm(x.root,{recursive:true,force:true});
  }
 });
 it("rejects bad handshakes and pre-aborted requests",async()=>{const x=await makeServer(false);await expect(new CodexSocketClient(x.path,50).request("x",{},new AbortController().signal)).rejects.toThrow(/handshake/i);x.srv.close();await rm(x.root,{recursive:true,force:true});const y=await makeServer();const c=new CodexSocketClient(y.path);const a=new AbortController();a.abort();await expect(c.request("x",{},a.signal)).rejects.toThrow(/aborted/i);await c.close();y.srv.close();await rm(y.root,{recursive:true,force:true});});
 it("does not resolve a client request from a colliding server request",async()=>{const x=await makeCollisionServer("server-request");const c=new CodexSocketClient(x.path);try{await expect(c.request("thread/read",{},new AbortController().signal)).resolves.toEqual({ok:true});}finally{await c.close();x.srv.close();await rm(x.root,{recursive:true,force:true});}});
 it("does not resolve a client request from a numeric id without result or error",async()=>{const x=await makeCollisionServer("empty");const c=new CodexSocketClient(x.path);try{await expect(c.request("thread/read",{},new AbortController().signal)).resolves.toEqual({ok:true});}finally{await c.close();x.srv.close();await rm(x.root,{recursive:true,force:true});}});
 it("reassembles fragmented text and continuation response frames", async () => {
  const x = await makeBoundaryServer("fragment"); const c = new CodexSocketClient(x.path);
  try { await expect(c.request("thread/read", {}, boundedSignal())).resolves.toEqual({ ok: true }); }
  finally { await c.close(); await closeServer(x.srv); await rm(x.root, { recursive: true, force: true }); }
 });
 it("answers an interleaved ping with the exact payload", async () => {
  const x = await makeBoundaryServer("ping"); const c = new CodexSocketClient(x.path);
  try {
   const [result] = await Promise.all([
    c.request("thread/read", {}, boundedSignal()),
    awaitPong(x.pongObserved),
   ]);
   expect(result).toEqual({ ok: true });
   expect(x.pongPayload).toEqual(Buffer.from("gptqueue-ping"));
   expect(x.pongMasked).toBe(true);
  } finally { await c.close(); await closeServer(x.srv); await rm(x.root, { recursive: true, force: true }); }
 });
 it("rejects a pending request on close and reconnects for the next request", async () => {
  const x = await makeBoundaryServer("close-on-request"); const c = new CodexSocketClient(x.path, 500);
  try {
   await expect(c.request("thread/read", {}, boundedSignal())).rejects.toThrow(/closed|disconnected/i);
   await expect(c.request("thread/read", {}, boundedSignal())).resolves.toEqual({ ok: true });
   expect(x.connectionCount).toBe(2);
  } finally { await c.close(); await closeServer(x.srv); await rm(x.root, { recursive: true, force: true }); }
 });
});
