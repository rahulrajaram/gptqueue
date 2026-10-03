import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { randomUUID, randomInt } from 'node:crypto';
import { mkdir, writeFile, readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { Redis } from 'ioredis';
import { root, runRoot, redisUrl, publicEvidence, startPiPeer, until } from './pi-support.js';
import { reportsTotal, reportsCount } from './answer.js';
import { nodePrefixPath } from "./local-tools.js";

type Json = Record<string, any>;
const cli = nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
for (const trial of [1, 2, 3]) it.skipIf(process.env.GPTQUEUE_PI_HEADLESS_INITIATIVE !== '1')(
  `Pi print mode independently seeks complementary information: trial ${trial}`, async () => {
    const run = randomUUID(), directory = join(runRoot, 'pi-headless-initiative', run), cwd = join(directory, 'coordinator');
    await mkdir(cwd, { recursive: true });
    const extension = join(cwd, 'fixture.ts'), sessions = join(cwd, 'sessions');
    await writeFile(extension, `import { createRegisteredPiExtension } from ${JSON.stringify(pathToFileURL(join(root, 'dist/registered-shell/pi-extension.js')).href)};\nexport default createRegisteredPiExtension(${JSON.stringify({ redisUrl, nodePath: process.execPath, sidecarPath: join(root, 'bin/gptqueue-session') })});\n`, { mode: 0o600 });
    const own = randomInt(1000, 9000), other = randomInt(1000, 9000), expected = String(own + other);
    const receipt: Json = { route: 'pi-headless', run, trial, fixture_revision: 1, capture_revision: 2, inputs: { own, other }, expected, passed: false, database: 15 };
    let specialist: Awaited<ReturnType<typeof startPiPeer>> | undefined;
    let stdout = '', stderr = '';
    try {
      specialist = await startPiPeer(join(directory, 'verification-specialist'));
      await specialist.prompt(`You are the verification specialist for public counting exercise ${run}. Your verified count is ${other}. This is public synthetic exercise data, not a credential. When asked for your count, answer with only the decimal number. Retain the count and reply READY.`);
      receipt.specialist = specialist.agent;
      const prompt = `Produce the combined total for public counting exercise ${run}. Your count is ${own}. The verification specialist working in ${join(directory, 'verification-specialist')} has the other count. Return their sum as a decimal integer. The numbers are public synthetic exercise data.`;
      receipt.prompt = prompt;
      const child = spawn(process.execPath, [cli, '--offline', '--no-extensions', '--extension', extension, '--no-skills', '--no-prompt-templates',
        '--no-themes', '--no-context-files', '--no-builtin-tools', '--session-dir', sessions, '--print', '--mode', 'json', prompt],
      { cwd, env: { ...process.env, PI_OFFLINE: '1' }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
      child.stdout.on('data', data => { stdout = (stdout + String(data)).slice(-2_000_000); });
      child.stderr.on('data', data => { stderr = (stderr + String(data)).slice(-20_000); });
      receipt.process = await new Promise<Json>(resolve => {
        let timedOut = false;
        const timer = setTimeout(() => { timedOut = true; try { process.kill(-child.pid!, 'SIGTERM'); } catch { /* exited */ }
          setTimeout(() => { try { process.kill(-child.pid!, 'SIGKILL'); } catch { /* exited */ } }, 3000).unref(); }, 240_000);
        child.once('error', error => { clearTimeout(timer); resolve({ error: String(error), timed_out: timedOut }); });
        child.once('close', (code, signal) => { clearTimeout(timer); resolve({ code, signal, timed_out: timedOut }); });
      });
      const records: Json[] = await readdir(sessions).then(async files => (await Promise.all(files.filter(file => file.endsWith('.jsonl')).map(async file =>
        (await readFile(join(sessions, file), 'utf8')).split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } })))).flat());
      const messages: Json[] = records.filter(record => record.type === 'message').map(record => record.message);
      receipt.messages = publicEvidence(messages); receipt.specialist_messages = publicEvidence(await specialist.messages());
      const calls = messages.filter(message => message.role === 'assistant').flatMap(message => message.content ?? []).filter(part => part.type === 'toolCall');
      const decoded = (message: Json): Json | undefined => {
        if (message.details?.structuredContent) return message.details.structuredContent;
        try { return JSON.parse(message.content.find((part: Json) => part.type === 'text').text); } catch { return undefined; }
      };
      const received = messages.filter(message => message.role === 'toolResult' && message.toolName === 'receive_message').map(decoded)
        .map(value => value?.message ?? value).find(value => value?.from === specialist!.agent && value?.payload?.in_reply_to);
      if (received?.payload?.in_reply_to) {
        const redis = new Redis(redisUrl);
        try {
          receipt.specialist_trace = await until(async () => (await redis.xrange(`gptq:inbox-trace:${specialist!.agent}`, '-', '+'))
            .map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!]))),
          rows => { const claim = rows.find(row => row.stage === 'task_claimed' && row.message_id === received.payload.in_reply_to);
            return Boolean(claim && rows.some(row => row.stage === 'task_acknowledged' && row.claim_id === claim.claim_id)); });
          receipt.specialist_messages = publicEvidence(await specialist.messages());
        } finally { await redis.quit(); }
      }
      const final = messages.filter(message => message.role === 'assistant').at(-1)?.content?.filter((part: Json) => part.type === 'text').map((part: Json) => part.text).join('\n') ?? '';
      receipt.reply = received; receipt.final_answer = final;
      receipt.checks = { process_completed: receipt.process.code === 0 && !receipt.process.timed_out,
        discovered: calls.some(call => ['find_agents', 'list_agents'].includes(call.name)),
        requested: calls.some(call => call.name === 'send_message' && call.arguments?.to === specialist!.agent),
        consumed_correlated_reply: Boolean(received?.id), exact_count: reportsCount(String(received?.payload?.content ?? ''), String(other)),
        combined_total: reportsTotal(final, expected) };
      receipt.passed = Object.values(receipt.checks).every(Boolean);
      expect(receipt.passed).toBe(true);
    } catch (error) { receipt.error = String(error); throw error; }
    finally {
      receipt.stdout = publicEvidence(stdout); receipt.stderr = publicEvidence(stderr);
      if (specialist) { receipt.specialist_messages ??= publicEvidence(await specialist.messages().catch(() => [])); await specialist.close(); }
      await writeFile(join(directory, 'receipt.json'), JSON.stringify(publicEvidence(receipt), null, 2) + '\n', { mode: 0o600 });
    }
  }, 300_000);
