#!/usr/bin/env node
// Install gptqueue's runtime as immutable, verified releases on local disk (RT-1).
//
//   install-runtime.mjs install  [--prefix DIR] [--source DIR] [--allow-dirty] [--init-bound-ms N] [--hook-bound-ms N] [--no-activate]
//   install-runtime.mjs status   [--prefix DIR] [--json]
//   install-runtime.mjs rollback [--prefix DIR]
//   install-runtime.mjs prune    --keep N [--prefix DIR]
//   install-runtime.mjs plan     [--prefix DIR] [--home DIR] [--from-root DIR]   (prints only)
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { startOwnedRedis } from "./lib/owned-redis.mjs";
import { ReleaseError, install, prune, rollback, status } from "./lib/runtime-release.mjs";
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
      const redis = await startOwnedRedis({ stderr: "ignore" });
      try {
        return await install({
          source: resolve(values.source), prefix, allowDirty: values["allow-dirty"], redisUrl: redis.url,
          initBoundMs: Number(values["init-bound-ms"]), hookBoundMs: Number(values["hook-bound-ms"]),
          activateRelease: !values["no-activate"],
        });
      } finally { redis.stop(); }
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
      throw new ReleaseError("bad_argument", "usage: install-runtime.mjs install|status|rollback|prune|plan [--prefix DIR]");
  }
}

try {
  print(await main(positionals[0]));
} catch (error) {
  const code = error instanceof ReleaseError ? error.code : "internal_error";
  console.error(JSON.stringify({ error: code, message: error.message, ...(error.verify ? { verify: error.verify } : {}) }, null, 2));
  process.exitCode = 1;
}
