#!/usr/bin/env node
// Drive the gptqueue MCP server over stdio exactly as Claude Code would.
//   selftest                          two throwaway agents: list, send+idempotent resend,
//                                     non-consuming peek, claim, acknowledge, cleanup
//   send --to NAME --content TEXT --key KEY [--type task|result|status|error|ping]
//   claim [--max N] [--no-ack]        claim (and by default acknowledge) this agent's inbox
//   list [--all]                      online agents (name, client, cwd); --all includes offline
//   unregister                        delete this agent and its queue (throwaway names only)
// The agent name comes from GPTQ_AGENT_NAME or gptqueue-claude-name for $PWD.
import { execFileSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const here = join(dirname(fileURLToPath(import.meta.url)), "..", "bin");
const home = process.env.GPTQUEUE_HOME ?? join(here, "..");
const sdk = join(home, "node_modules/@modelcontextprotocol/sdk/dist/esm/client");
const { Client } = await import(join(sdk, "index.js"));
const { StdioClientTransport } = await import(join(sdk, "stdio.js"));

const [cmd, ...rest] = process.argv.slice(2);
const opt = (flag, fallback) => {
  const i = rest.indexOf(flag);
  return i >= 0 ? rest[i + 1] : fallback;
};

async function connect(name) {
  const transport = new StdioClientTransport({
    command: join(here, "gptqueue-claude-mcp"),
    env: { ...process.env, GPTQ_AGENT_NAME: name },
    stderr: "inherit",
  });
  const client = new Client({ name: "gptqueue-claude-verify", version: "1" });
  await client.connect(transport);
  const call = async (tool, args = {}) => {
    const res = await client.callTool({ name: tool, arguments: args });
    const text = res.content?.map((c) => c.text ?? "").join("") ?? "";
    let body;
    try { body = JSON.parse(text); } catch { body = text; }
    if (res.isError) throw new Error(`${tool} failed: ${text}`);
    return res.structuredContent ?? body;
  };
  return { client, call, name };
}

const peek = (name) =>
  Number(execFileSync(join(here, "gptqueue-inbox-peek"), ["--name", name]).toString().trim());

function check(cond, label, detail) {
  console.log(`${cond ? "PASS" : "FAIL"} ${label}${detail ? `: ${detail}` : ""}`);
  if (!cond) process.exitCode = 1;
}

async function selftest() {
  const tag = randomUUID().slice(0, 8);
  const a = await connect(`claude-verify-a-${tag}`);
  const b = await connect(`claude-verify-b-${tag}`);
  try {
    const listed = await a.call("list_agents");
    const names = (listed.agents ?? listed).map((x) => x.name);
    check(names.includes(a.name) && names.includes(b.name), "list_agents shows both agents");

    const key = `verify-${tag}-to-b`;
    const first = await a.call("send_message", { to: b.name, content: `hello ${tag}`, type: "ping", idempotency_key: key });
    const again = await a.call("send_message", { to: b.name, content: `hello ${tag}`, type: "ping", idempotency_key: key });
    console.log("send #1:", JSON.stringify(first));
    console.log("send #2 (same key):", JSON.stringify(again));
    check(peek(b.name) === 1, "idempotent resend queued exactly one message (peek, non-consuming)");
    check(peek(b.name) === 1, "peek did not consume");

    // The key is scoped to the sender: the same key to another recipient is not delivered.
    const other = await a.call("send_message", { to: a.name, content: `other ${tag}`, type: "ping", idempotency_key: key });
    check(other.status === "duplicate" && peek(a.name) === 0, "idempotency_key is sender-scoped (reuse for another recipient is dropped)", other.status);

    const claim = await b.call("claim_tasks", { max_batch: 4 });
    const tasks = claim.tasks ?? claim.claim?.tasks ?? [];
    console.log("claim:", JSON.stringify(claim));
    check(tasks.length === 1 && JSON.stringify(tasks[0]).includes(`hello ${tag}`), "claim_tasks returned the message");
    check(peek(b.name) === 0, "inbox empty while claim is outstanding");
    const claimId = claim.claim_id ?? claim.claim?.claim_id;
    const ack = await b.call("acknowledge_tasks", { claim_id: claimId });
    console.log("ack:", JSON.stringify(ack));
    check(true, "acknowledge_tasks accepted");
  } finally {
    for (const c of [a, b]) {
      await c.call("unregister_agent", {}).catch((e) => console.log(`cleanup ${c.name}: ${e.message}`));
      await c.client.close();
    }
  }
}

function selfName() {
  return process.env.GPTQ_AGENT_NAME ||
    execFileSync(join(here, "gptqueue-claude-name"), [process.cwd()]).toString().trim();
}

async function send() {
  const to = opt("--to"), content = opt("--content"), key = opt("--key");
  if (!to || !content || !key) throw new Error("send needs --to, --content and --key");
  const me = await connect(selfName());
  try {
    console.log(JSON.stringify(await me.call("send_message", { to, content, type: opt("--type", "task"), idempotency_key: key }), null, 2));
  } finally { await me.client.close(); }
}

async function claim() {
  const me = await connect(selfName());
  try {
    const res = await me.call("claim_tasks", { max_batch: Number(opt("--max", "16")) });
    console.log(JSON.stringify(res, null, 2));
    const id = res.claim_id ?? res.claim?.claim_id;
    if (id && !rest.includes("--no-ack")) console.log(JSON.stringify(await me.call("acknowledge_tasks", { claim_id: id })));
  } finally { await me.client.close(); }
}

async function list() {
  const me = await connect(`claude-verify-list-${randomUUID().slice(0, 8)}`);
  try {
    const res = await me.call("list_agents");
    for (const a of res.agents ?? res) {
      if (a.name === me.name || (!a.online && !rest.includes("--all"))) continue;
      console.log([a.online ? "online " : "offline", a.name, a.client ?? "-", a.working_directory ?? "-"].join("\t"));
    }
  } finally {
    await me.call("unregister_agent", {}).catch(() => {});
    await me.client.close();
  }
}

async function unregister() {
  const me = await connect(selfName());
  try { console.log(JSON.stringify(await me.call("unregister_agent", {}))); }
  finally { await me.client.close(); }
}

const commands = { selftest, send, claim, list, unregister };
if (!commands[cmd]) {
  console.error("usage: gptqueue-claude-verify.mjs selftest | send --to N --content T --key K [--type T] | claim [--max N] [--no-ack] | list [--all] | unregister");
  process.exit(2);
}
await commands[cmd]();
