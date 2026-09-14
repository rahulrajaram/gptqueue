import { it, expect } from 'vitest';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { startPiPeer, root, runRoot, redisUrl, until, publicEvidence } from './pi-support.js';
import { checkExchangeEvidence } from './oracle.js';

it.skipIf(process.env.GPTQUEUE_PI_ACCEPTANCE !== '1')('Pi CLI RPC automatically processes a fresh task through its native extension', async () => {
  const directory = join(runRoot, 'pi', randomUUID());
  await mkdir(directory, { recursive: true });
  const receipt: Record<string, unknown> = { route: 'pi-rpc-cli', database: 15,
    inference: 'live-configured-model', passed: false, profile: 'isolated-native-extension; discovery disabled; no added messaging instructions' };
  const redis = new Redis(redisUrl);
  const reference = new Client({ name: 'gptqueue-acceptance-reference', version: '1' });
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [join(root, 'dist/mcp-server/index.js')], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> });
  let peer: Awaited<ReturnType<typeof startPiPeer>> | undefined;
  const call = async (name: string, args: Record<string, unknown> = {}) => {
    const result = await reference.callTool({ name, arguments: args });
    if (result.isError) throw new Error(`Reference ${name} failed`);
    return result.structuredContent as Record<string, any>;
  };
  try {
    await reference.connect(transport);
    const sender = `acceptance-reference-${randomUUID()}`;
    await call('register_agent', { name: sender, role: 'both', description: 'isolated acceptance reference' });
    peer = await startPiPeer(join(directory, 'recipient'));
    receipt.agent = peer.agent; receipt.state = publicEvidence(peer.state); receipt.binding = peer.binding;
    expect(peer.binding?.runtime_id).toBeTruthy();
    await peer.prompt('You are available for bounded arithmetic work. Reply READY.');
    const left = randomInt(1000, 9000), right = randomInt(1000, 9000);
    const sent = await call('send_message', { to: peer.agent, type: 'task',
      content: `Compute ${left} + ${right}. Reply with only the decimal number.`, idempotency_key: randomUUID() });
    receipt.sent = sent; receipt.expected = String(left + right);
    const replyResult = await until(() => call('receive_message', { timeout: 1 }),
      result => Boolean(result.message), 300_000);
    const reply = replyResult.message;
    receipt.reply = reply;
    expect(reply.from).toBe(peer.agent); expect(reply.to).toBe(sender);
    expect(reply.payload.in_reply_to).toBe(sent.message_id);
    expect(reply.payload.content.trim()).toBe(String(left + right));
    const traces = await until(async () => (await redis.xrange(`gptq:inbox-trace:${peer!.agent}`, '-', '+'))
      .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!]))),
      rows => { const claim = rows.find(row => row.stage === 'task_claimed' && row.message_id === sent.message_id);
        return Boolean(claim && rows.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id)); });
    receipt.traces = traces;
    const claim = traces.find(row => row.stage === 'task_claimed' && row.message_id === sent.message_id)!;
    receipt.verdict = checkExchangeEvidence({ sender, recipient: peer.agent,
      expected_reply_content: String(left + right), execution: { status: 'completed' },
      request: { id: sent.message_id, from: sender, to: peer.agent },
      reply: { id: reply.id, from: reply.from, to: reply.to,
        in_reply_to: reply.payload.in_reply_to, content: reply.payload.content.trim() },
      request_consumption: { actor: peer.agent, message_id: claim.message_id!, consumed: true,
        claim_id: claim.claim_id, acknowledged: traces.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id) },
      reply_consumption: { actor: sender, message_id: reply.id, consumed: true, acknowledged: false },
      reply_requires_ack: false,
    });
    expect((receipt.verdict as { outcome: string }).outcome).toBe('meets');
    receipt.passed = true;
  } catch (error) { receipt.error = String(error); throw error; }
  finally {
    if (peer) { receipt.messages = publicEvidence(await peer.messages().catch(() => [])); receipt.events = peer.events; await peer.close(); }
    await call('close_session').catch(() => undefined); await reference.close(); await redis.quit();
    await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
  }
}, 420_000);
