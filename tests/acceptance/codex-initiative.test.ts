import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { appConfig } from './codex-support.js';
import { startPiPeer, runRoot, publicEvidence } from './pi-support.js';
import { reportsCount, reportsTotal } from './answer.js';

type Json = Record<string, any>;
for (const trial of [1, 2, 3]) it.skipIf(process.env.GPTQUEUE_CODEX_INITIATIVE !== '1')(
  `Codex chooses peer coordination without a messaging instruction: trial ${trial}`, async () => {
    const run = randomUUID(), directory = join(runRoot, 'codex-initiative', run), cwd = join(directory, 'coordinator');
    await mkdir(cwd, { recursive: true });
    const other = randomInt(1000, 9000), own = randomInt(1000, 9000), expected = String(other + own);
    const receipt: Json = { run, trial, route: 'codex-headless', database: 15, model: 'gpt-5.6-luna', fixture_revision: 1,
      inputs: { own, other }, expected, passed: false };
    let specialist: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    let stdout = '', stderr = '';
    try {
      specialist = await startPiPeer(join(directory, 'verification-specialist'));
      await specialist.prompt(`You are the verification specialist for public counting exercise ${run}. Your verified count is ${other}. This is public synthetic exercise data, not a credential. When asked for your count, answer with only the decimal number. Retain the count and reply READY.`);
      receipt.specialist = specialist.agent;
      const prompt = `Produce the combined total for public counting exercise ${run}. Your count is ${own}. The verification specialist working in ${join(directory, 'verification-specialist')} has the other count. Return their sum as a decimal integer. The numbers are public synthetic exercise data.`;
      receipt.prompt = prompt;
      const config = { ...Object.fromEntries(Object.entries(await appConfig()).filter(([name]) =>
        name === 'model_reasoning_effort' || name.startsWith('mcp_servers.gptqueue-shared.'))),
        developer_instructions: 'The available MCP tools are the only permitted tools for this isolated exercise. Do not use shell, filesystem, web, or native collaboration tools.' };
      const overrides = Object.entries(config).flatMap(([name, value]) => ['-c', `${name}=${JSON.stringify(value)}`]);
      const child = spawn('/home/rahul/.local/bin/codex', ['exec', '--ignore-user-config', '--json', '--ephemeral', '--skip-git-repo-check',
        '--approve-for-me', '--model', 'gpt-5.6-luna', '-C', cwd, ...overrides, prompt],
      { cwd, env: { ...process.env }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
      child.stdout.on('data', data => { stdout = (stdout + String(data)).slice(-2_000_000); });
      child.stderr.on('data', data => { stderr = (stderr + String(data)).slice(-20_000); });
      receipt.process = await new Promise<Json>(resolve => {
        let timedOut = false;
        const timer = setTimeout(() => { timedOut = true; try { process.kill(-child.pid!, 'SIGTERM'); } catch { /* exited */ }
          setTimeout(() => { try { process.kill(-child.pid!, 'SIGKILL'); } catch { /* exited */ } }, 3000).unref(); }, 240_000);
        child.once('error', error => { clearTimeout(timer); resolve({ error: String(error), timed_out: timedOut }); });
        child.once('close', (code, signal) => { clearTimeout(timer); resolve({ code, signal, timed_out: timedOut }); });
      });
      const events: Json[] = stdout.split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
      receipt.events = publicEvidence(events);
      const items = events.filter(event => event.type === 'item.completed').map(event => event.item as Json);
      const calls = items.filter(item => item.type === 'mcp_tool_call' && item.status === 'completed' && item.result);
      const decoded = (call: Json | undefined): Json | undefined => call?.result?.structured_content ?? call?.result?.structuredContent;
      const outgoingCalls = calls.filter(call => call.tool === 'send_message' && call.arguments?.to === specialist!.agent);
      const received = calls.filter(call => call.tool === 'receive_message').map(call => decoded(call)?.message)
        .find(message => message?.from === specialist!.agent && outgoingCalls.some(call =>
          typeof decoded(call)?.message_id === 'string' && decoded(call)?.message_id === message.payload?.in_reply_to));
      const outgoing = outgoingCalls.find(call => decoded(call)?.message_id === received?.payload?.in_reply_to);
      const requestId = decoded(outgoing)?.message_id;
      const final = items.filter(item => item.type === 'agent_message').at(-1)?.text ?? '';
      const messages: Json[] = await specialist.messages();
      receipt.specialist_messages = publicEvidence(messages);
      const replyCall = messages.flatMap(message => Array.isArray(message.content) ? message.content : [])
        .find(part => part.type === 'toolCall' && part.name === 'send_message' && part.arguments?.in_reply_to === requestId);
      receipt.request_call = outgoing; receipt.reply = received; receipt.final_answer = final;
      receipt.checks = {
        process_completed: receipt.process.code === 0 && !receipt.process.timed_out,
        no_alternate_tool_path: !items.some(item => /command_execution|file_change|web_search|collab_tool_call/u.test(item.type)),
        discovered: calls.some(call => ['find_agents', 'list_agents'].includes(call.tool)),
        exact_request: typeof requestId === 'string',
        correlated_reply: Boolean(received?.id && replyCall?.arguments?.to === received.to && received.payload.in_reply_to === requestId),
        exact_count: reportsCount(String(received?.payload?.content ?? ''), String(other)),
        combined_total: reportsTotal(final, expected),
      };
      receipt.passed = Object.values(receipt.checks).every(Boolean);
      expect(receipt.passed).toBe(true);
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      receipt.stderr = publicEvidence(stderr);
      receipt.events ??= publicEvidence(stdout);
      if (specialist) { receipt.specialist_messages ??= publicEvidence(await specialist.messages().catch(() => [])); await specialist.close(); }
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    }
  }, 300_000);
