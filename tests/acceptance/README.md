# Acceptance harness verification

The production TypeScript configuration includes only `src`. Check acceptance
sources explicitly before running native conformance:

```sh
node ./node_modules/typescript/bin/tsc -p tests/acceptance/tsconfig.json
```

Run focused tests with `REDIS_URL=redis://127.0.0.1:6379/15`. Native conformance
fixtures must create their own Redis process with `startOwnedRedis` or
`openOwnedRedis`; they must never connect to the shared server. Opt-in native
gates require exclusive scheduling, existing authentication, and bounded cleanup.

The frozen inventory is in `qualification-routes.ts`. Adapter conformance,
ordered communication pairs, automatic activation, and initiative are distinct
obligations. A passing adapter smoke test does not qualify its communication
pairs. Retained evidence may be re-adjudicated with a documented oracle change;
preserve the original execution result and record the derivative separately.
