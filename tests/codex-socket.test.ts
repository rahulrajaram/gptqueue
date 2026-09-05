import { describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { CodexSocketClient } from "../src/registered-shell/codex-socket.js";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const out = (body: Buffer) => Buffer.concat([Buffer.from([0x81, body.length]), body]);
const makeServer = async (valid = true) => { const root=await mkdtemp(join(tmpdir(),"gptq-sock-"));const path=join(root,"sock");const srv=createServer(s=>{let b=Buffer.alloc(0),ready=false;s.on("data",chunk=>{b=Buffer.concat([b,chunk]);if(!ready){const e=b.indexOf("\r\n\r\n");if(e<0)return;const h=b.subarray(0,e).toString();const k=/Sec-WebSocket-Key: (.+)/i.exec(h)?.[1].trim()??"";const a=createHash("sha1").update(k+GUID).digest("base64");s.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${valid?a:"bad"}\r\n\r\n`);b=b.subarray(e+4);ready=true;}while(b.length>=2){const code=b[1]!&127;let h=2,n=code;if(code===126){if(b.length<4)return;n=b.readUInt16BE(2);h=4}if(b.length<h+4+n)return;const mask=b.subarray(h,h+4);h+=4;const p=Buffer.alloc(n);for(let i=0;i<n;i++)p[i]=b[h+i]!^mask[i%4]!;b=b.subarray(h+n);const m=JSON.parse(p.toString());s.write(out(Buffer.from(JSON.stringify({jsonrpc:"2.0",id:m.id,result:{ok:true}}))));}});});await new Promise<void>(r=>srv.listen(path,r));return{root,path,srv};};
describe("Codex socket",()=>{
 it("performs valid handshake and correlates requests",async()=>{const x=await makeServer();const c=new CodexSocketClient(x.path);expect(await c.request("thread/read",{payload:"x".repeat(300)},new AbortController().signal)).toEqual({ok:true});await c.close();x.srv.close();await rm(x.root,{recursive:true,force:true});});
 it("rejects bad handshakes and pre-aborted requests",async()=>{const x=await makeServer(false);await expect(new CodexSocketClient(x.path,50).request("x",{},new AbortController().signal)).rejects.toThrow(/handshake/i);x.srv.close();await rm(x.root,{recursive:true,force:true});const y=await makeServer();const c=new CodexSocketClient(y.path);const a=new AbortController();a.abort();await expect(c.request("x",{},a.signal)).rejects.toThrow(/aborted/i);await c.close();y.srv.close();await rm(y.root,{recursive:true,force:true});});
});
