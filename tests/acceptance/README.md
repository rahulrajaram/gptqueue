# Acceptance harness verification

The production TypeScript configuration includes only `src`. Check acceptance
sources explicitly before running native conformance:

```sh
node ./node_modules/typescript/bin/tsc -p tests/acceptance/tsconfig.json
```

Every vitest run gets its own private, socket-only Redis (no URL to pass;
`docs/OPERATING_RULES.md` §10). Native conformance fixtures that need a
separate instance create one with `startOwnedRedis` (the same socket-only
helper); they must never connect to a shared server, and the URL validators
here refuse every TCP Redis URL. An owned URL is a unix-socket path
(`<run dir>/redis.sock?db=N`), not a TCP URL: never parse it with `new URL()`.
Build receipts with `ownedRedisReceipt` and child configuration with
`geminiMcpSettings` / `mcpEnvAliases` (`owned-redis.ts`, `qualification-codex.ts`),
and child environments with `privateRedisEnv`, which names the instance under
every Redis endpoint variable (`REDIS_URL`, `GPTQUEUE_REDIS_URL`, ...). Opt-in native
gates require exclusive scheduling, existing authentication, and bounded cleanup.

The frozen inventory is in `qualification-routes.ts`. Adapter conformance,
ordered communication pairs, automatic activation, and initiative are distinct
obligations. A passing adapter smoke test does not qualify its communication
pairs. Retained evidence may be re-adjudicated with a documented oracle change;
preserve the original execution result and record the derivative separately.
