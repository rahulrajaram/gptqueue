# Semantic review record — communication-lifecycle-v1

- Reviewed model: `MODULE.json`, module digest `2cf84260bc2786dc…` (full digest
  recorded in the compiled `bundle.json`), bundle id `b33c84bf7a038d3e…`.
- Structural admission: `metabuilder harness check` → `valid: true`.
- Independent reviewer: general-purpose subagent, READ-ONLY pass, verdict
  **APPROVE-WITH-NOTES** (2026-06 campaign epoch 1). The reviewer verified
  every named product behavior against committed source
  (`close-session.ts`, `send-message.ts`, `redis-client.ts`,
  `session-store.ts`, `http.ts`) and found **no over-assertion** of GPTQueue
  semantics.

## Hypothesis → requirement binding

| Hypothesis | Requirement | Evidence action |
| --- | --- | --- |
| H1 registration/readiness | `registration-and-topology` | `topology-scenarios` |
| H2 1→1 | `registration-and-topology` | `topology-scenarios` |
| H3 1→N fan-out (ordinary send_message calls) | `registration-and-topology` | `topology-scenarios` |
| H4 3×2 complete bipartite | `registration-and-topology` | `topology-scenarios` |
| H5 retry idempotency | `retry-idempotency` | `idempotency-scenario` |
| H6 process continuity (stateless session re-binding) | `process-continuity` | `continuity-scenario` |
| H7 stale transport recovery | `process-continuity` | `continuity-scenario` |
| H8 exit and cleanup to baseline | `exit-and-cleanup` | `cleanup-scenario` |
| H9 typed backpressure | `backpressure` | `backpressure-scenario` |

## Reviewer findings and disposition

1. H9 "earlier queued messages remain receivable" missing from acceptance →
   **applied**: acceptance now requires every pre-refusal message received
   intact with attribution and idempotency key.
2. H7 documented refusal not pinned to the 404 session-expired shape →
   **applied**: acceptance pins the documented 404 response directing
   re-initialization (`src/transports/http.ts`).
3. H1 status surface unchecked → **applied**: `get_queue_status` now part of
   the registration acceptance.
4. H8 lease-refresh stop only implied → **applied**: acceptance requires the
   documented session-unavailable error on re-binding a closed session.
5. Hypothesis ids not traceable → **applied**: `[H#]` prefixes in statements.
6. Correlation clause localized → **applied**: restated in idempotency and
   continuity acceptance.

## Accepted deviations (recorded, not defects)

- H6/H7 exercise over a Unix-domain-socket HTTP transport inside the
  network-less MetaBuilder sandbox (wire-identical at the application layer).
- The vitest/Node-24 focused suite runs controller-side; toolchain admission
  (`/usr`, `/bin`, rustup only) cannot express the Node 24 interpreter.
- Three consecutive bounded rounds are inside each scenario action per the
  acceptance text, not a MetaBuilder repeat-until loop.

Gate status: **passed**. Implementation of the runner and the focused suite
is now authorized; MetaBuilder actions still acquire no source-mutation
authority — repairs land as normal governed repository commits before any
fresh run is authored.
