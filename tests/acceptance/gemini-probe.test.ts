import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { root, runRoot, redisUrl, publicEvidence } from './pi-support.js';
import { SESSION_KEYS } from '../../src/core/keys.js';
import { nodePrefixPath } from "./local-tools.js";

it.skipIf(process.env.GPTQUEUE_GEMINI_ACCEPTANCE !== '1')('observes Gemini CLI registration and exact self-message mechanics', async () => {
  const run = randomUUID(), directory = join(runRoot, 'gemini', run), cwd = join(directory, 'workspace');
  const agent = `acceptance-gemini-${run}`, nonce = randomUUID();
  await mkdir(join(cwd, '.gemini'), { recursive: true });
  await writeFile(join(cwd, '.gemini/settings.json'), JSON.stringify({
    hooksConfig: { enabled: false }, tools: { core: [] },
    security: { folderTrust: { enabled: false } },
    mcpServers: { gptqueue: { command: process.execPath, args: [join(root, 'dist/mcp-server/index.js')], env: { REDIS_URL: redisUrl }, trust: true } },
  }), { mode: 0o600 });
  const receipt: Record<string, any> = { route: 'gemini-cli', run, passed: false, database: 15,
    scope: 'assisted registration and self-message only; no peer or automatic handling claim' };
  const redis = new Redis(redisUrl);
  try {
    const prompt = `Use only the gptqueue MCP tools. Register yourself as ${agent}, role both. Send yourself a status message with content ${nonce}, idempotency_key ${nonce}. Receive it and verify the exact message id and content. Do not use shell, files, or contact another agent. Return the original tool results.`;
    const child = spawn(nodePrefixPath("bin/gemini"), ['--extensions', 'none', '--allowed-mcp-server-names', 'gptqueue',
      '--approval-mode', 'yolo', '--output-format', 'stream-json', '--prompt', prompt],
    { cwd, env: { ...process.env, NO_UPDATE_NOTIFIER: '1' }, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout = (stdout + String(chunk)).slice(-2_000_000); });
    child.stderr.on('data', chunk => { stderr = (stderr + String(chunk)).slice(-30_000); });
    const result = await new Promise<{ code: number | null; timed_out: boolean; error?: string }>(resolve => {
      let expired = false;
      const timer = setTimeout(() => { expired = true; child.kill('SIGTERM'); setTimeout(() => child.kill('SIGKILL'), 3000).unref(); }, 180_000);
      child.once('error', error => { clearTimeout(timer); resolve({ code: null, timed_out: expired, error: String(error) }); });
      child.once('close', code => { clearTimeout(timer); resolve({ code, timed_out: expired }); });
    });
    const events = stdout.split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
    receipt.process = result; receipt.events = publicEvidence(events); receipt.stderr = publicEvidence(stderr);
    const tools = events.filter(event => event.type === 'tool_use');
    const results = events.filter(event => event.type === 'tool_result');
    const sent = tools.find(event => String(event.tool_name).endsWith('send_message') && event.parameters?.content === nonce);
    const received = tools.find(event => String(event.tool_name).endsWith('receive_message'));
    const decoded = (call: any) => {
      const result = results.find(event => call && event.tool_id === call.tool_id);
      if (!result) return null;
      try { return typeof result.output === 'string' ? JSON.parse(result.output) : result.output; } catch { return null; }
    };
    const sentResult = decoded(sent), receivedResult = decoded(received);
    const message = receivedResult?.message ?? receivedResult;
    receipt.registration_observed = Boolean(await redis.hget(SESSION_KEYS.registry, agent));
    receipt.exact_self_message = Boolean(sentResult?.message_id && message?.id === sentResult.message_id &&
      message.from === agent && message.to === agent && message.payload?.content === nonce);
    receipt.passed = result.code === 0 && !result.timed_out && receipt.registration_observed && receipt.exact_self_message;
    expect(receipt.passed).toBe(true);
  } catch (error) { receipt.error = String(error); throw error; }
  finally {
    await redis.quit();
    await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    await rm(cwd, { recursive: true, force: true });
  }
}, 210_000);
