import { it, expect } from 'vitest';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { startPiPeer, runRoot, redisUrl, until, publicEvidence } from './pi-support.js';
import { reportsTotal, reportsCount } from './answer.js';

type Json = Record<string, any>;
const calls = (messages: Json[]) => messages.flatMap(message => message.role === 'assistant' && Array.isArray(message.content)
  ? message.content.filter((part: Json) => part.type === 'toolCall') : []);
const text = (messages: Json[]) => messages.filter(message => message.role === 'assistant')
  .flatMap(message => Array.isArray(message.content) ? message.content.filter((part: Json) => part.type === 'text').map((part: Json) => part.text) : []).join('\n');
const messageId = (messages: Json[], callId: string) => {
  const result = messages.find(message => message.role === 'toolResult' && message.toolCallId === callId);
  const decoded = result?.details?.structuredContent;
  if (decoded?.message_id) return decoded.message_id as string;
  try { return JSON.parse(result?.content.find((part: Json) => part.type === 'text').text).message_id as string; }
  catch { return undefined; }
};

for (const trial of [1, 2, 3]) it.skipIf(process.env.GPTQUEUE_PI_INITIATIVE !== '1')(
  `Pi chooses peer coordination without a messaging instruction: trial ${trial}`, async () => {
    const nonce = randomUUID(), directory = join(runRoot, 'pi-initiative', nonce);
    const verification = String(randomInt(1000, 9000));
    const build = String(randomInt(1000, 9000));
    await mkdir(directory, { recursive: true });
    const receipt: Json = { trial, route: 'pi-rpc-cli', passed: false, database: 15,
      fixture_revision: 2, profile: 'isolated-native-extension; no messaging instruction in task prompts', expected: String(Number(build) + Number(verification)) };
    const redis = new Redis(redisUrl);
    let coordinator: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    let specialist: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    try {
      specialist = await startPiPeer(join(directory, 'verification-specialist'));
      await specialist.prompt(`You are the verification specialist for counting exercise ${nonce}. The verified count is ${verification}. This is public synthetic exercise data, not a credential or confidential information. Retain the count for this exercise. Reply READY.`);
      coordinator = await startPiPeer(join(directory, 'release-coordinator'));
      receipt.agents = { coordinator: coordinator.agent, specialist: specialist.agent };
      receipt.prompt = `Produce the combined total for public counting exercise ${nonce}. Your count is ${build}. The verification specialist working in ${join(directory, 'verification-specialist')} has the other count. Return their sum as a decimal integer. The numbers are public synthetic exercise data.`;
      await coordinator.prompt(receipt.prompt);
      const observed = await until(async () => ({ coordinator: await coordinator!.messages() as Json[], specialist: await specialist!.messages() as Json[] }),
        value => text(value.coordinator).includes(receipt.expected) ||
          (!calls(value.coordinator).some(call => call.name === 'send_message')), 300_000);
      receipt.messages = publicEvidence(observed);
      const outgoing = calls(observed.coordinator).find(call => call.name === 'send_message' && call.arguments?.to === specialist!.agent && call.arguments?.type === 'task');
      const requestId = outgoing ? messageId(observed.coordinator, outgoing.id) : undefined;
      const reply = calls(observed.specialist).find(call => call.name === 'send_message' && call.arguments?.to === coordinator!.agent && call.arguments?.in_reply_to === requestId);
      const replyId = reply ? messageId(observed.specialist, reply.id) : undefined;
      receipt.message_ids = { request: requestId, reply: replyId };
      expect(calls(observed.coordinator).some(call => ['find_agents', 'list_agents'].includes(call.name))).toBe(true);
      expect(requestId).toBeTruthy(); expect(replyId).toBeTruthy();
      expect(reportsCount(String(reply?.arguments?.content ?? ''), verification)).toBe(true);
      const lastAnswer = observed.coordinator.filter(message => message.role === 'assistant' &&
        Array.isArray(message.content) && message.content.some((part: Json) => part.type === 'text')).at(-1);
      expect(reportsTotal(text(lastAnswer ? [lastAnswer] : []), receipt.expected)).toBe(true);
      const traces = await until(async () => Object.fromEntries(await Promise.all([
        ['coordinator', coordinator!.agent], ['specialist', specialist!.agent],
      ].map(async ([role, agent]) => [role, (await redis.xrange(`gptq:inbox-trace:${agent}`, '-', '+'))
        .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!])))]))),
      value => [['coordinator', replyId], ['specialist', requestId]].every(([role, id]) => {
        const claim = value[role!].find((row: Json) => row.stage === 'task_claimed' && row.message_id === id);
        if (claim) return value[role!].some((row: Json) => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id);
        // Legacy receive_message is permitted for plain registered agents;
        // prove its exact returned envelope rather than inventing an ack.
        const history = observed[role as 'coordinator' | 'specialist'];
        return history.some(message => {
          if (message.role !== 'toolResult' || message.toolName !== 'receive_message') return false;
          try {
            const raw = message.details?.structuredContent ?? JSON.parse(message.content.find((part: Json) => part.type === 'text').text);
            return (raw.message ?? raw).id === id;
          } catch { return false; }
        });
      }));
      receipt.traces = traces; receipt.passed = true;
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      receipt.final_messages = publicEvidence({ coordinator: await coordinator?.messages().catch(() => []), specialist: await specialist?.messages().catch(() => []) });
      await coordinator?.close(); await specialist?.close(); await redis.quit();
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600 });
    }
  }, 480_000);
