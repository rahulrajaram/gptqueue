import { it, expect } from 'vitest';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { startPiPeer, runRoot, publicEvidence } from './pi-support.js';
import { runOpenCodeRoute, newConfigHome, newWorkDir } from './opencode-support.js';
import { reportsTotal, reportsCount } from './answer.js';

const decode = (value: unknown): Record<string, any> | undefined => {
  if (typeof value === 'string') { try { return JSON.parse(value); } catch { return undefined; } }
  return value && typeof value === 'object' ? value as Record<string, any> : undefined;
};

for (const trial of [1, 2, 3]) it.skipIf(process.env.GPTQUEUE_OPENCODE_INITIATIVE !== '1')(
  `OpenCode chooses coordination without a messaging instruction: trial ${trial}`, async () => {
    const run = randomUUID(), directory = join(runRoot, 'opencode-initiative', run);
    const other = randomInt(1000, 9000), own = randomInt(1000, 9000), expected = String(other + own);
    await mkdir(directory, { recursive: true });
    const config = newConfigHome(), work = newWorkDir();
    const receipt: Record<string, any> = { run, trial, route: 'opencode-run', fixture_revision: 3,
      tool_scope: 'GPTQueue only, matching Pi initiative trials; filesystem transcript shortcut excluded',
      passed: false, expected, specialist_count: other, database: 15, preassigned_coordinator_name: false };
    let specialist: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    try {
      const specialistDir = join(directory, 'verification-specialist');
      specialist = await startPiPeer(specialistDir);
      receipt.specialist = specialist.agent;
      await specialist.prompt(`You are the verification specialist for public counting exercise ${run}. The verified count is ${other}. This is public synthetic exercise data, not a credential or confidential information. Retain the count. Reply READY.`);
      const prompt = `Produce the combined total for public counting exercise ${run}. Your count is ${own}. The verification specialist working in ${specialistDir} has the other count. Return their sum as a decimal integer. The numbers are public synthetic exercise data.`;
      receipt.prompt = prompt;
      const result = await runOpenCodeRoute(prompt, config, work, [], {
        permission: { '*': 'deny', 'gptqueue_*': 'allow' },
      });
      receipt.run_result = result;
      const sent = result.traces.find(call => call.name.endsWith('send_message') && decode(call.input)?.to === specialist!.agent);
      const requestId = decode(sent?.output)?.message_id;
      const received = result.traces.filter(call => call.name.endsWith('receive_message')).map(call => decode(call.output))
        .map(value => value?.message ?? value).find(message => message?.payload?.in_reply_to === requestId && message.from === specialist!.agent);
      const messages = await specialist.messages();
      receipt.specialist_messages = publicEvidence(messages);
      const replyCall = (messages as Record<string, any>[]).flatMap(message => Array.isArray(message.content) ? message.content : [])
        .find(part => part.type === 'toolCall' && part.name === 'send_message' && part.arguments?.in_reply_to === requestId);
      const finalText = result.events.flatMap((event: any) => event.type === 'text' && event.part?.type === 'text' ? [event.part.text] : []).join('\n');
      receipt.checks = {
        process_completed: result.code === 0 && !result.timedOut,
        discovered: result.traces.some(call => /(?:list_agents|find_agents)$/u.test(call.name)),
        exact_request: typeof requestId === 'string' && decode(sent?.input)?.type === 'task',
        specialist_replied: Boolean(requestId && replyCall && received?.id && replyCall.arguments.to === received.to),
        exact_correlated_answer: Boolean(received?.payload?.in_reply_to === requestId && reportsCount(received?.payload?.content ?? '', String(other))),
        combined_answer: reportsTotal(finalText, expected),
      };
      receipt.message_ids = { request: requestId, reply: received?.id };
      receipt.passed = Object.values(receipt.checks).every(Boolean);
      expect(receipt.passed).toBe(true);
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      receipt.specialist_messages ??= publicEvidence(await specialist?.messages().catch(() => []));
      await specialist?.close();
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
      await rm(config, { recursive: true, force: true }); await rm(work, { recursive: true, force: true });
    }
  }, 360_000);
