#!/usr/bin/env node
// Install gptqueue's runtime as immutable, verified releases on local disk (RT-1).
//
//   install-runtime.mjs install  [--prefix DIR] [--source DIR] [--allow-dirty] [--init-bound-ms N] [--hook-bound-ms N] [--no-activate]
//   install-runtime.mjs activate --id ID [--prefix DIR] [--expect-current ID] [--service UNIT] [--health-url URL]
//                                [--deadline-ms N] [--interval-ms N] [--journal-out FILE] [--json]
//   install-runtime.mjs status   [--prefix DIR] [--json]
//   install-runtime.mjs rollback [--prefix DIR]
//   install-runtime.mjs prune    --keep N [--prefix DIR]
//   install-runtime.mjs plan     [--prefix DIR] [--home DIR] [--from-root DIR]   (prints only)
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { writeFileSync } from "node:fs";
import { privateRedisEnv, startOwnedRedis } from "./lib/owned-redis.mjs";
import { activateWithReadiness } from "./lib/runtime-activate.mjs";
import { ReleaseError, install, killVerificationChildren, prune, rollback, status } from "./lib/runtime-release.mjs";
import { buildPlan, renderPlan } from "./lib/runtime-plan.mjs";

const DEFAULT_PREFIX = join(homedir(), ".local", "share", "gptqueue");
const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    prefix: { type: "string", default: DEFAULT_PREFIX },
    source: { type: "string", default: join(dirname(fileURLToPath(import.meta.url)), "..") },
    "allow-dirty": { type: "boolean", default: false },
    "no-activate": { type: "boolean", default: false },
    "init-bound-ms": { type: "string", default: "3000" },
    "hook-bound-ms": { type: "string", default: "3000" },
    id: { type: "string" },
    "expect-current": { type: "string" },
    service: { type: "string", default: "mcp-gptqueue.service" },
    "health-url": { type: "string", default: "http://127.0.0.1:8101/health" },
    "deadline-ms": { type: "string", default: "15000" },
    "interval-ms": { type: "string", default: "250" },
    "journal-out": { type: "string" },
    keep: { type: "string" },
    home: { type: "string", default: homedir() },
    "from-root": { type: "string" },
    json: { type: "boolean", default: false },
  },
});
const prefix = resolve(values.prefix);
const print = (value) => console.log(typeof value === "string" ? value : JSON.stringify(value, null, 2));

async function main(command) {
  switch (command) {
    case "install": {
      // Private socket-only Redis (no TCP); verification children get its URL.
      const redis = await startOwnedRedis();
      // Every Redis endpoint alias in this process (and so in every child) names the private instance.
      Object.assign(process.env, privateRedisEnv(process.env, redis.url));
      // A signal aimed at this pid alone must not orphan Redis or the
      // verification children (the default action skips finally and "exit").
      // A signal that arrives during the synchronous npm install is handled
      // once it returns; the staging it leaves is reclaimed by the next run.
      for (const [signal, number] of [["SIGTERM", 15], ["SIGINT", 2], ["SIGHUP", 1]]) {
        process.once(signal, () => { killVerificationChildren(); redis.stop(); process.exit(128 + number); });
      }
      try {
        return await install({
          source: resolve(values.source), prefix, allowDirty: values["allow-dirty"], redisUrl: redis.url,
          initBoundMs: Number(values["init-bound-ms"]), hookBoundMs: Number(values["hook-bound-ms"]),
          activateRelease: !values["no-activate"],
        });
      } finally { await redis.close(); }
    }
    case "activate": {
      if (values.id === undefined) throw new ReleaseError("bad_argument", "activate needs --id ID");
      const result = await activateWithReadiness({
        prefix, id: values.id, expectCurrent: values["expect-current"], service: values.service,
        healthUrl: values["health-url"], deadlineMs: Number(values["deadline-ms"]), intervalMs: Number(values["interval-ms"]),
      });
      // result (and journal) is frozen; report the write without mutating it.
      let reported = result;
      if (values["journal-out"] !== undefined && result.journal?.text !== undefined) {
        try {
          writeFileSync(values["journal-out"], result.journal.text);
          reported = { ...result, journal: { ...result.journal, writtenTo: values["journal-out"] } };
        } catch (error) {
          // A failed evidence write must not mask the activation outcome.
          reported = { ...result, journalWriteError: String(error?.message ?? error) };
        }
      }
      if (result.error !== undefined) {
        console.error(JSON.stringify(reported, null, 2));
        process.exitCode = 1;
        return undefined; // the failure record went to stderr; never also to stdout
      }
      return reported;
    }
    case "status": return status(prefix);
    case "rollback": return rollback(prefix);
    case "prune":
      if (values.keep === undefined) throw new ReleaseError("bad_argument", "prune needs --keep N");
      return prune(prefix, Number(values.keep));
    case "plan": {
      const s = status(prefix);
      const current = s.releases.find((r) => r.id === s.current);
      const plan = buildPlan({ home: values.home, prefix, fromRoot: values["from-root"], verify: current ? { claudeHook: { ms: current.claudeHookMs } } : undefined });
      return values.json ? plan : renderPlan(plan);
    }
    default:
      throw new ReleaseError("bad_argument", "usage: install-runtime.mjs install|activate|status|rollback|prune|plan");
  }
}

try {
  const out = await main(positionals[0]);
  if (out !== undefined) print(out);
} catch (error) {
  const code = error instanceof ReleaseError ? error.code : "internal_error";
  console.error(JSON.stringify({ error: code, message: error.message, ...(error.verify ? { verify: error.verify } : {}) }, null, 2));
  process.exitCode = 1;
}
