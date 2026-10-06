# End-to-end acceptance tests (normal suite)

Real gptqueue components against the owned Redis, with protocol-faithful fake
native hosts (see `harness.ts`): Codex app-server v2 JSON-RPC, the Pi extension
API driving the real registered Pi sidecar, the OpenCode SDK client driving the
real plugin, and, for Claude, the real event-driven watcher (`watchInbox`) and
Stop hook (`runClaudeHook`). The Claude stand-in publishes a real session record
for its own pid (legacy /proc scan off), so the Stop hook resolves a confirmed
identity and blocks a finished turn while mail is pending; each Claude injection
records whether a Stop-hook block or the idle watcher made it, and Claude WK-2 and
LT-4 require the Stop-hook block. WK-1/DL-5 and DL-8 read consumption from server
state (inbox trace rows, claims index and claim records). Run one file at a time:

    node scripts/with-owned-redis.mjs -- npx --no-install vitest run tests/acceptance-e2e/wake.test.ts

| File | Criteria |
|---|---|
| `wake.test.ts` | WK-1/DL-5 (exact-exchange oracle), WK-2, WK-3, WK-4 (N=20, <= 4 activations), WK-6, per runtime |
| `concurrency.test.ts` | DL-8: 4 senders x 3 recipients x 25, concurrent claim/ack, lease recovery |
| `identity.test.ts` | ID-1 / ID-5 per runtime |
| `latency-<runtime>.test.ts` | LT-0..LT-4; hops are defined in `latency.ts` |

The latency tests always print p50/p95/p99. Optional environment variables:
`GPTQUEUE_LATENCY_ITERATIONS` (default 50), `GPTQUEUE_LATENCY_OUT=<file>` (results
from each runtime are merged into this JSON file), and `GPTQUEUE_LATENCY_ASSERT=1`
(enforces the provisional targets: LT-2 p95 <= 50 ms, LT-3 p95 <= 300 ms, or <= 1 s
for Claude, and LT-4 p95 <= 300 ms).
