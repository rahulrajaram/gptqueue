import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { randomInt, randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { SESSION_KEYS } from '../../src/core/keys.js';
import { appConfig, redisUrl, repo } from './codex-support.js';
import { publicEvidence, until } from './pi-support.js';
import { checkExchangeEvidence } from './oracle.js';

type Json = Record<string, any>;
const structured = (value: Json | undefined): Json | undefined => value?.structured_content ?? value?.structuredContent;

it.skipIf(process.env.GPTQUEUE_CODEX_EXCHANGE !== '1')('a native Codex child consumes a private arithmetic task and returns its exact answer', async () => {
  const run = randomUUID(), dir = await mkdtemp(join(tmpdir(), 'gptqueue-codex-peer-'));
  const artifact = join(repo, '.gptqueue/acceptance/20260912-evaluation/codex-exchange', run);
  await mkdir(artifact, { recursive: true });
  const left = randomInt(1000, 9000), right = randomInt(1000, 9000), expected = String(left + right);
  const ready = `ready-${randomUUID()}`, key = `task-${randomUUID()}`;
  const redis = new Redis(redisUrl), receipt: Json = { run, passed: false, route: 'codex-headless-to-native-child', database: 15,
    expected, inputs: { left, right }, automatic_activation_claimed: false };
  let child: ReturnType<typeof spawn> | undefined;
  let stdout = '', stderr = '';
  try {
    const childPrompt = `Use only GPTQueue MCP tools. Get your runtime status. Send one status message to PARENT_AGENT with content ${ready} and idempotency_key ${ready}. Then poll claim_tasks until an arithmetic task arrives. Compute its requested sum. Send a result to task.from, decimal answer only, in_reply_to task.id, idempotency_key task.id. Acknowledge that exact claim after replying. Finish. Do not use shell/files or contact anyone else.`;
    const prompt = `Use only gptqueue-shared MCP tools and native collaboration. First get_runtime_status to learn YOUR parent identity. Spawn exactly ONE gpt-5.6-luna child in a fresh context, without forking this conversation. Give it this exact instruction after replacing PARENT_AGENT with your identity: ${childPrompt}\nNow use receive_message to get the child's ready status ${ready}; its envelope.from is the child's messaging identity. Send THAT identity exactly one task with content "Add ${left} and ${right}; return only the decimal sum.", type task, idempotency_key ${key}. Do not include these arithmetic inputs in the child launch prompt. Retain the returned message_id. Use receive_message until you receive its correlated result. Verify the sender and in_reply_to. Wait for the native child to finish, then report the answer. Do not use shell or files.`;
    const config = Object.fromEntries(Object.entries(await appConfig()).filter(([name]) =>
      name === 'model_reasoning_effort' || name.startsWith('mcp_servers.gptqueue-shared.')));
    const overrides = Object.entries(config).flatMap(([name, value]) => ['-c', `${name}=${JSON.stringify(value)}`]);
    child = spawn('/home/rahul/.local/bin/codex', ['exec', '--ignore-user-config', '--json', '--ephemeral', '--skip-git-repo-check',
      '--approve-for-me', '--model', 'gpt-5.6-luna', '-C', dir, ...overrides, prompt],
    { cwd: dir, env: { ...process.env }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout?.on('data', chunk => { stdout = (stdout + String(chunk)).slice(-2_000_000); });
    child.stderr?.on('data', chunk => { stderr = (stderr + String(chunk)).slice(-40_000); });
    const status = await new Promise<Json>(resolve => {
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; try { process.kill(-child!.pid!, 'SIGTERM'); } catch { /* exited */ }
        setTimeout(() => { try { process.kill(-child!.pid!, 'SIGKILL'); } catch { /* exited */ } }, 3000).unref(); }, 300_000);
      child!.once('error', error => { clearTimeout(timer); resolve({ error: String(error), timed_out: timedOut }); });
      child!.once('close', (code, signal) => { clearTimeout(timer); resolve({ code, signal, timed_out: timedOut }); });
    });
    receipt.process = status;
    const events: Json[] = stdout.split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
    receipt.events = publicEvidence(events);
    const items = events.filter(event => event.type === 'item.completed').map(event => event.item as Json);
    const calls = items.filter(item => item.type === 'mcp_tool_call' && item.status === 'completed' && item.result);
    const parent = structured(calls.find(call => call.tool === 'get_runtime_status')?.result)?.agent;
    const receives = calls.filter(call => call.tool === 'receive_message').map(call => structured(call.result)?.message);
    const readyMessage = receives.find(message => message?.type === 'status' && message.payload?.content === ready);
    const send = calls.find(call => call.tool === 'send_message' && call.arguments?.idempotency_key === key);
    const requestId = structured(send?.result)?.message_id;
    const reply = receives.find(message => requestId && message?.payload?.in_reply_to === requestId);
    const native = items.filter(item => item.type === 'collab_tool_call' && item.tool === 'spawn_agent' && item.status === 'completed');
    const registry = Object.entries(await redis.hgetall(SESSION_KEYS.registry)).flatMap(([name, raw]) => {
      try { const value = JSON.parse(raw); return value.metadata?.working_directory === dir ? [{ name, value }] : []; } catch { return []; }
    });
    receipt.identities = registry; receipt.request = { id: requestId, from: parent, to: send?.arguments?.to }; receipt.reply = reply;
    expect(native).toHaveLength(1);
    expect(native[0]!.receiver_thread_ids).toHaveLength(1);
    expect(native[0]!.prompt).not.toContain(`Add ${left} and ${right}`);
    expect(parent).toBeTruthy(); expect(readyMessage?.from).toBeTruthy(); expect(readyMessage!.from).not.toBe(parent);
    expect(registry.some(row => row.name === parent)).toBe(true);
    expect(registry.some(row => row.name === readyMessage!.from)).toBe(true);
    expect(send?.arguments?.to).toBe(readyMessage!.from);
    const traces = await until(async () => (await redis.xrange(`gptq:inbox-trace:${readyMessage!.from}`, '-', '+'))
      .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!]))),
    rows => { const claim = rows.find(row => row.stage === 'task_claimed' && row.message_id === requestId);
      return Boolean(claim && rows.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id)); }, 15_000);
    receipt.child_trace = traces;
    const claim = traces.find(row => row.stage === 'task_claimed' && row.message_id === requestId)!;
    receipt.verdict = checkExchangeEvidence({ sender: parent, recipient: readyMessage!.from, expected_reply_content: expected,
      execution: { status: 'completed' }, request: receipt.request,
      reply: reply ? { id: reply.id, from: reply.from, to: reply.to, content: reply.payload?.content, in_reply_to: reply.payload?.in_reply_to } : undefined,
      request_consumption: { actor: readyMessage!.from, message_id: claim.message_id!, consumed: true, claim_id: claim.claim_id, acknowledged: true },
      reply_consumption: reply ? { actor: parent, message_id: reply.id, consumed: true, acknowledged: false } : undefined,
      reply_requires_ack: false,
    });
    expect(status.code).toBe(0); expect(receipt.verdict.outcome).toBe('meets'); receipt.passed = true;
  } catch (error) { receipt.error = String(error); throw error; }
  finally {
    receipt.stderr = publicEvidence(stderr);
    if (!receipt.events) receipt.events = publicEvidence(stdout);
    await writeFile(join(artifact, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    await redis.quit(); await rm(dir, { recursive: true, force: true });
  }
}, 340_000);
