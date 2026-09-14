import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { Redis } from 'ioredis';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { root, runRoot, redisUrl, publicEvidence, until } from './pi-support.js';
import { SESSION_KEYS } from '../../src/core/keys.js';

const cli = '/home/rahul/nodeenv2251-311/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js';
const quote = (arg: string) => "'" + arg.replaceAll("'", "'\\''") + "'";

for (const mode of ['interactive', 'headless'] as const) it.skipIf(process.env.GPTQUEUE_PI_MODES !== '1')(
  `observes actual Pi ${mode} CLI identity and messaging`, async () => {
    const run = randomUUID(), directory = join(runRoot, `pi-${mode}`, run);
    await mkdir(directory, { recursive: true });
    const extension = join(directory, 'fixture.ts'), sessions = join(directory, 'sessions');
    await writeFile(extension, `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(root, 'dist/registered-shell/pi-extension.js')).href)};\nexport default createRegisteredPiExtension(${JSON.stringify({ redisUrl, nodePath: process.execPath, sidecarPath: join(root, 'bin/gptqueue-session') })});\n`, { mode: 0o600 });
    const receipt: Record<string, any> = { route: `pi-${mode}`, run, passed: false, database: 15 };
    const reference = new Client({ name: `acceptance-${run}`, version: '1' });
    const redis = new Redis(redisUrl);
    let child: ReturnType<typeof spawn> | undefined;
    let output = '';
    const call = async (name: string, args: Record<string, unknown> = {}) => {
      const r = await reference.callTool({ name, arguments: args });
      if (r.isError) throw new Error(`Reference ${name} failed`);
      return r.structuredContent as Record<string, any>;
    };
    try {
      await reference.connect(new StdioClientTransport({ command: process.execPath,
        args: [join(root, 'dist/mcp-server/index.js')], env: { ...process.env, REDIS_URL: redisUrl } as Record<string, string> }));
      const sender = `acceptance-pi-mode-reference-${run}`;
      await call('register_agent', { name: sender, role: 'both', description: 'Owned Pi mode evaluation reference' });
      const nonce = randomUUID();
      const prompt = mode === 'interactive' ? 'You are available for bounded arithmetic work. Reply READY.'
        : `Use your GPTQueue tools to find agent ${sender}. Send it a status message containing exactly ${nonce}, with idempotency_key ${nonce}. Do not contact other agents. Then finish.`;
      const args = [cli, '--offline', '--no-extensions', '--extension', extension, '--no-skills', '--no-prompt-templates',
        '--no-themes', '--no-context-files', '--no-builtin-tools', '--session-dir', sessions,
        ...(mode === 'headless' ? ['--print', '--mode', 'json'] : []), prompt];
      child = mode === 'interactive' ? spawn('script', ['--quiet', '--return', '--command', [process.execPath, ...args].map(quote).join(' '), '/dev/null'],
        { cwd: directory, detached: true, env: { ...process.env, TERM: 'xterm-256color', PI_OFFLINE: '1' }, stdio: ['pipe', 'pipe', 'pipe'] })
        : spawn(process.execPath, args, { cwd: directory, detached: true, env: { ...process.env, PI_OFFLINE: '1' }, stdio: ['pipe', 'pipe', 'pipe'] });
      if (mode === 'headless') child.stdin?.end();
      child.stdout?.on('data', data => { output = (output + String(data)).slice(-500_000); });
      child.stderr?.on('data', data => { output = (output + String(data)).slice(-500_000); });
      child.once('error', error => { receipt.process_error = String(error); });
      const agent = await until(async () => Object.entries(await redis.hgetall(SESSION_KEYS.registry)).find(([, raw]) => {
        try { return JSON.parse(raw).metadata?.working_directory === directory; } catch { return false; }
      })?.[0], value => Boolean(value));
      receipt.agent = agent;
      if (mode === 'interactive') {
        await until(async () => output, value => value.includes('READY'));
        const content = 'Compute 4317 + 2864. Reply with only the decimal number.';
        const request = await call('send_message', { to: agent, type: 'task', content, idempotency_key: nonce });
        receipt.request = { ...request, content };
        const got = await until(() => call('receive_message', { timeout: 1 }), value => Boolean(value.message), 300_000);
        receipt.reply = got.message;
        expect(got.message).toMatchObject({ from: agent, to: sender, payload: { content: '7181', in_reply_to: request.message_id } });
        receipt.traces = await until(async () => (await redis.xrange(`gptq:inbox-trace:${agent}`, '-', '+'))
          .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!]))),
        rows => { const claim = rows.find(row => row.stage === 'task_claimed' && row.message_id === request.message_id);
          return Boolean(claim && rows.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id)); });
      } else {
        const got = await until(() => call('receive_message', { timeout: 1 }), value => Boolean(value.message), 180_000);
        receipt.received = got.message;
        expect(got.message).toMatchObject({ from: agent, to: sender, type: 'status', payload: { content: nonce } });
      }
      receipt.passed = true;
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      if (child?.pid && child.exitCode === null) {
        try { process.kill(-child.pid, 'SIGTERM'); } catch { /* already exited */ }
        await new Promise<void>(resolve => { const timer = setTimeout(() => {
          try { process.kill(-child!.pid!, 'SIGKILL'); } catch { /* already exited */ } resolve();
        }, 3000); child!.once('close', () => { clearTimeout(timer); resolve(); }); });
      }
      receipt.output = publicEvidence(output);
      receipt.session_evidence = await readdir(sessions).then(async files => Promise.all(files.filter(f => f.endsWith('.jsonl')).map(async file =>
        publicEvidence((await readFile(join(sessions, file), 'utf8')).split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } }))
      ))).catch(() => []);
      await call('close_session').catch(() => undefined); await reference.close(); await redis.quit();
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    }
  }, 420_000);
