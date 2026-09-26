/**
 * Approval-gating regression matrix (see docs/QUICKSTART.md,
 * "Approval-restricted Codex sessions").
 *
 * A Codex session running approval_policy="never" rejects gptqueue MCP calls
 * client-side with "MCP tool call requires approval, but approval policy is
 * never" — the call never reaches the server. The scoped per-server opt-in
 * `mcp_servers.gptqueue-shared.default_tools_approval_mode="approve"` must
 * restore both lookups and sends under that same global policy.
 *
 * Opt-in (spawns codex twice against the test Redis db): GPTQUEUE_CODEX_APPROVAL=1
 */
import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { appConfig } from './codex-support.js';
import { runRoot } from './pi-support.js';

type Json = Record<string, any>;
type Mode = 'control' | 'approve';

const runCodex = async (mode: Mode): Promise<{ events: Json[]; output: string }> => {
  const directory = join(runRoot, 'codex-approval', mode, randomUUID());
  await mkdir(directory, { recursive: true });
  // Keep only gptqueue-shared entries: with --ignore-user-config, a disabling
  // override for another server would define a transport-less server entry
  // ("invalid transport in mcp_servers.<name>") on codex 0.157.0.
  const base = Object.fromEntries(Object.entries(await appConfig()).filter(([name]) =>
    name === 'model_reasoning_effort' || name.startsWith('mcp_servers.gptqueue-shared.')));
  const config: Json = { ...base, approval_policy: 'never' };
  if (mode === 'approve') config['mcp_servers.gptqueue-shared.default_tools_approval_mode'] = 'approve';
  const overrides = Object.entries(config).flatMap(([name, value]) => ['-c', `${name}=${JSON.stringify(value)}`]);
  const prompt = 'Call the MCP tool list_agents on server gptqueue-shared with no arguments. Then reply with just the number of agents returned, or the exact error you got.';
  return await new Promise<Json[]>((resolve, reject) => {
    let stdout = '';
    const child = spawn(process.env.GPTQUEUE_CODEX_BIN || 'codex',
      ['exec', '--ignore-user-config', '--json', '--ephemeral', '--skip-git-repo-check', '-C', directory, ...overrides, prompt],
      { cwd: directory, env: { ...process.env }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', data => { stdout = (stdout + String(data)).slice(-2_000_000); });
    child.stderr.on('data', data => { stdout = (stdout + String(data)).slice(-20_000); });
    if (process.env.GPTQUEUE_CODEX_APPROVAL_DEBUG) child.stderr.on('data', () => {}); // keep pipe flowing
    const timer = setTimeout(() => {
      try { process.kill(-child.pid!, 'SIGTERM'); } catch { /* exited */ }
      setTimeout(() => { try { process.kill(-child.pid!, 'SIGKILL'); } catch { /* exited */ } }, 3000).unref();
    }, 240_000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('close', () => {
      clearTimeout(timer);
      resolve({ events: stdout.split('\n').flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } }), output: stdout });
    });
  });
};

const listAgentsCall = (events: Json[]): Json | undefined =>
  events.filter(event => event.type === 'item.completed')
    .map(event => event.item as Json)
    .find(item => item.type === 'mcp_tool_call' && item.tool === 'list_agents');

for (const mode of ['control', 'approve'] as const) it.skipIf(process.env.GPTQUEUE_CODEX_APPROVAL !== '1')(
  `approval_policy=never ${mode === 'control' ? 'rejects gptqueue tool calls client-side' : 'permits them with default_tools_approval_mode=approve'}: ${mode}`, async () => {
    const { events, output } = await runCodex(mode);
    const call = listAgentsCall(events);
    if (!call) {
      // Surface the child output so harness failures are diagnosable from CI logs.
      console.error(`[codex-approval-gating:${mode}] no list_agents call; raw output tail:`, output.slice(-3000));
    }
    expect(call, 'a list_agents mcp_tool_call item must appear').toBeDefined();
    if (mode === 'control') {
      expect(call!.status).toBe('failed');
      expect(String(call!.error?.message)).toContain('requires approval, but approval policy is never');
    } else {
      expect(call!.status).toBe('completed');
      const structured = call!.result?.structured_content ?? call!.result?.structuredContent;
      expect(Array.isArray(structured?.agents)).toBe(true);
      expect(structured.agents.length).toBeGreaterThanOrEqual(1);
    }
  });
