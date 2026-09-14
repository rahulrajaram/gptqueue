import { it, expect } from 'vitest';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { startPiPeer, root, runRoot, redisUrl, until, publicEvidence } from './pi-support.js';

type Json = Record<string, any>;
for (const replyType of ['result', 'error'] as const) {
  it.skipIf(process.env.GPTQUEUE_PI_REPLY_ACTIVATION !== '1')(`idle Pi continues an outstanding task on a correlated ${replyType}`, async () => {
    const directory = join(runRoot, 'pi-reply-activation', randomUUID());
    await mkdir(directory, { recursive: true });
    const receipt: Json = { route: 'pi-rpc-cli', reply_type: replyType, database: 15, passed: false };
    const redis = new Redis(redisUrl), reference = new Client({ name: 'acceptance-reply-service', version: '1' });
    let peer: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    const call = async (name: string, args: Json = {}): Promise<Json> => {
      const result = await reference.callTool({ name, arguments: args });
      if (result.isError) throw new Error(`Reference ${name} failed`);
      return result.structuredContent as Json;
    };
    try {
      await reference.connect(new StdioClientTransport({ command: process.execPath,
        args: [join(root, 'dist/mcp-server/index.js')], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> }));
      const service = `reply-service-${randomUUID()}`;
      await call('register_agent', { name: service, role: 'both', description: 'arithmetic service for this isolated trial' });
      peer = await startPiPeer(join(directory, 'peer'));
      receipt.agent = peer.agent;
      const left = randomInt(1000, 9000), right = randomInt(1000, 9000);
      receipt.inputs = { left, right };
      await peer.prompt(`Send ${service} one task to calculate ${left} + ${right}. End this turn after sending it; the service will answer later. When its result arrives, report only the decimal answer. If it returns an error, report exactly SERVICE_UNAVAILABLE. Do not poll for the answer.`);
      const request = (await until(() => call('receive_message', { timeout: 1 }), r => Boolean(r.message), 15_000)).message;
      receipt.request = request;
      expect(request.from).toBe(peer.agent); expect(request.to).toBe(service); expect(request.type).toBe('task');
      const before = await peer.messages();
      receipt.message_count_before_reply = before.length;
      const expected = replyType === 'result' ? String(left + right) : 'SERVICE_UNAVAILABLE';
      const sent = await call('send_message', { to: peer.agent, type: replyType, in_reply_to: request.id,
        content: replyType === 'result' ? expected : 'The arithmetic service is unavailable; this request failed.', idempotency_key: randomUUID() });
      receipt.reply_send = sent; receipt.expected_continuation = expected;
      const traces = await until(async () => (await redis.xrange(`gptq:inbox-trace:${peer!.agent}`, '-', '+'))
        .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!]))),
      rows => { const claim = rows.find(row => row.stage === 'task_claimed' && row.message_id === sent.message_id);
        return Boolean(claim && rows.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id)); }, 180_000);
      receipt.traces = traces;
      const messages = await until(() => peer!.messages(), (rows: Json[]) => rows.slice(before.length).some(message =>
        message.role === 'assistant' && message.content?.some((part: Json) => part.type === 'text' && part.text?.trim() === expected)), 30_000);
      receipt.messages = publicEvidence(messages); receipt.passed = true;
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      if (peer) { receipt.messages ??= publicEvidence(await peer.messages().catch(() => [])); receipt.events = peer.events; await peer.close(); }
      await call('close_session').catch(() => undefined); await reference.close(); await redis.quit();
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    }
  }, 300_000);
}
