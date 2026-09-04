# Independent semantic review record — communication-lifecycle-v2

- Reviewed model: `MODULE.json` (schema v3), module digest `ed02aa621f7dbe9e…`,
  compiled bundle id `8d781b72e33ff9e0…`.
- Structural admission: `metabuilder harness check --input bundle.json` →
  `valid: true`. The bundle's `capability_ceiling` grants only
  `workspace_read` + `process_spawn` (no network, no source-write,
  `max_action_timeout_seconds: 900`).
- Reviewer: INDEPENDENT general-purpose subagent (did not author the module),
  READ-ONLY pass on the module; the runner implementation does not yet exist
  for the two new scenarios (see Finding 1). Ground truth: `NEXT_SHELL_PROMPT.md`
  requirements N1/N2. Fixture judged against `harness/lifecycle/scenarios.mjs`,
  `server-fixture.mjs`, `lifecycle-client.mjs`, `run-scenario.mjs`.

## Verdict

**adequate-with-findings**

The two new requirement blocks are faithful codifications of N1/N2 with no
weakening, no dropped clauses, and no sneaked-in ordering assumptions. The
dependency graph is coherent and the compiled workflow is a valid topological
ordering. The fixture already provides every primitive the new acceptances
need (SIGKILL + respawn, redis surviving the kill, UDS transport, session
re-binding) and only modest plumbing is required. All findings below are LOW —
none blocks admission of the model — but several are worth tightening before
(and while) the runner is extended.

## Checklist answers

1. **Fidelity to N1/N2.** H10 (`concurrent-interleaving`) and H11
   (`restart-durability`) are faithful.
   - N1's "2x3 complete bipartite matrix, two senders transmit concurrently
     (e.g. Promise.all over the ordinary send_message calls)", "exactly six
     unique edges", "correct attribution", "zero duplicates/cross-delivery per
     receiver", "three consecutive bounded rounds", "correlate by scenario id,
     round id, sender, receiver, unique per-edge idempotency key", "Do NOT
     assume any delivery ordering" → all present. The only additions are
     conservative formalizations of what N1 already implies: "zero missing
     edges" (implied by "exactly six unique edges") and "each of the three
     receivers observes exactly two messages, exactly one attributed to each
     sender" (the 2x3 counting). Nothing weakened, nothing dropped.
   - N2's "message accepted (send returned `sent`) immediately before a real
     server SIGKILL + respawn", "still receivable afterward by session
     re-binding", "three consecutive bounded rounds" → all present. The added
     "respawned server was a genuinely fresh process (new pid, health observed
     before the re-bind)" strengthens, does not weaken. Caveat: "a fresh
     client process" is an addition beyond N2's "session re-binding" and its
     literal reading is stricter than the v1 precedent (Finding 2).
2. **Hypothesis→action binding & acceptance observability/determinism.**
   - Binding: H10 → `concurrent-interleaving` → `concurrent-interleaving-scenario`;
     H11 → `restart-durability` → `restart-durability-scenario`. Both actions
     exist, carry argv, timeout, toolchain (`/usr/bin/node`),
     `node_modules`+`dist` aux digests. Structurally complete.
   - Observability: every H10/H11 sub-property is phrased as a result-summary
     fact. Exception: H10 acceptance bullet 2's "results are not ordered,
     sequenced, or serialized by the scenario … matched purely by idempotency
     key and attribution, never by arrival order" is a *code-construction*
     guarantee a JSON summary cannot literally "report" (Finding 3).
   - Determinism: no fixed sleeps in any fixture path (`pollUntil` deadline
     polling; drain uses bounded `receive_message` timeouts). No arrival-order
     dependence (multiset comparison via `assertExactDeliveries`). H11's "new
     pid" is a plain comparison (no pid conditioning on reuse — negligible).
     No reliance on undefined behavior.
3. **Carried-forward v1 requirements.** Verified programmatically: all six
   shared requirement objects (`registration-and-topology`,
   `retry-idempotency`, `process-continuity`, `exit-and-cleanup`,
   `backpressure`, `target-typecheck`) are **byte-identical** between v1 and
   v2. Dependency DAG is coherent and acyclic: `target-typecheck` →
   `registration-and-topology` → `retry-idempotency` → `process-continuity` →
   `exit-and-cleanup` → `backpressure`; new edges: `concurrent-interleaving`
   → (only) `registration-and-topology`, `restart-durability` →
   `concurrent-interleaving`. Every cited `depends_on` predecessor appears
   earlier in the compiled `main` sequence. Bundle check passed.
4. **Workflow ordering.** Compiled steps confirm the sequence
   `typecheck → topology → idempotency → continuity → cleanup → backpressure →
   concurrent → restart`. `restart-durability` (step 8) runs after its only
   dependency `concurrent-interleaving` (step 7). Sound for the DAG as
   declared.
5. **Expressibility / sandbox compliance.** The acceptance text as written can
   be satisfied inside the profile, and nothing forces a rule violation.
   - SIGKILL + respawn: `killServer()`/`spawnServer()` already kill the HTTP
     child and respawn against the same in-sandbox `redis-server`, whose data
     stays in memory (the redis child is not killed; `--appendonly no`, socket
     path not unlinked). The queued message + session records live in redis, so
     the re-bound receiver session is durable across the kill — the same
     mechanism H7 already proves.
   - Re-binding: `reboundClient(socketPath, sessionId)` (already exercised in
     v1 continuity) resumes by retained session id without re-registering.
   - Capability ceiling (`workspace_read`, `process_spawn`) is sufficient:
     every path is over UDS (`node:http socketPath`), all writes go to a
     `/tmp` fixture dir, all children are `/usr/bin/node` /
     `/usr/bin/redis-server`. No network egress, no target-source writes, no
     Node-24 interpreter as an action. Two wording-level gaps to close before
     run authoring: (a) H11's literal "fresh client process" (Finding 2) and
     (b) the fixture/spawn path does not yet expose the server PIDs needed to
     assert "new pid" (Finding 2, plumbing); neither is a sandbox violation,
     both are runner-extension requirements.
6. **Bounds.** `max_epochs 2`, `max_elapsed_seconds 3600`.
   Calibrated against the completed `communication-lifecycle-v1-e2` run
   (journal): 6 actions finished in **126.8 s** (typecheck 1.2 s, worst single
   action backpressure 80.5 s). Adding two scenarios (drain- and restart-shaped,
   tens of seconds each) keeps 8 actions realistically in the **3–6 minute**
   range — well under 3600 s. The *nominal* sum of per-action ceilings
   (600 + 7×900 = 6900 s) exceeds the campaign bound, but ceilings are not a
   budget; the campaign `max_elapsed_seconds` (which the journal shows accreting
   campaign-globally) is the hard stop and would only trip if multiple actions
   hung near their ceilings simultaneously. Acceptable for a bounded campaign;
   tightening per-scenario timeouts would close the theoretical gap (Finding 5).
7. **Aux digests.** Verified with the same installed binary this session:
   `metabuilder harness auxiliary-directory digest` yields
   `node_modules = bf36b3d418a645fc…` (5786 entries) and
   `dist = c74428731c362e19…` (156 entries) — **byte-exact matches** for the
   v2 module's pins, and `dist` matches v1's pin as well. Shape is plausible
   (64-hex SHA-256, distinct per-directory tree digests). Note: v1 pinned
   `node_modules = 6a94b302…`, which no longer matches the current tree —
   node_modules drifted after v1's authoring (the exact drift the
   sandbox-runtime skill warns about); v2's pins are the current values, so
   they hold. PASS.

## Findings

1. **Low — Acceptance cites runner entry points that do not exist yet.**
   `concurrent-interleaving` acceptance ("`run-scenario.mjs concurrent --rounds 3`")
   and `restart-durability` acceptance ("`run-scenario.mjs restart --rounds 3`")
   reference subcommands absent from `harness/lifecycle/run-scenario.mjs`
   (`SCENARIOS` = only topology|idempotency|continuity|cleanup|backpressure)
   and no `runConcurrentScenario`/`runRestartScenario` exists in
   `scenarios.mjs`. This is the planned model-first sequencing (review precedes
   implementation), so it is not a compile defect — `harness check` validated
   because check does not cross-check acceptance prose against the runner.
   But it means: (a) the module's two new evidence actions are not yet
   *executable* (a run authored now would exit 2 on both), and (b) bundle
   validity must not be read as executable acceptance.

2. **Low — H11's "a fresh client process" is stricter than N2 and than v1's
   own precedent.** N2 says only "session re-binding"; the module/
   acceptance says a "fresh client process re-binding the retained receiver
   session_id (without re-registering)". v1's `process-continuity` used the
   same "replacement client process" wording yet was implemented in-process
   via `reboundClient` (a fresh transport, same process), and v1's REVIEW
   recorded that as an accepted interpretation. If v2's author reuses the
   in-process rebound client, the literal word "process" is unmet; if they
   want the letter, they must spawn a `/usr/bin/node` child client (expressible,
   no new toolchain). Also, to assert "the respawned server was a genuinely
   fresh process (new pid,…)", the fixture needs to expose the spawned
   server's PID (current `spawnServer()` stores `this.server` but `killServer`
   does not return/compare PIDs) — small plumbing, not a blocker.

3. **Low — H10 acceptance bullet 2 folds an authoring guarantee into a
   summary-observable clause.** "The result summary reports zero missing edges …
   ; the concurrent send results are not ordered, sequenced, or serialized by
   the scenario; every edge is matched purely by its idempotency key and
   attribution, never by arrival order." A summary cannot "report" that the
   author did not serialize or did not sort by arrival — that is a
   code-construction invariant. Observable and deterministic as intended, but
   under-specified as evidence. Recommend the acceptance require the summary to
   carry an explicit concurrency indicator (e.g. `concurrentIssuance: 6`,
   `matching: "idempotency-key"`) so the non-ordering property is evidenced
   rather than assumed by convention.

4. **Low — `restart-durability` depends on `concurrent-interleaving` without
   semantic basis.** Restart durability shares nothing with concurrency; the
   dependency's only effect is forcing run order, which the sequence already
   guarantees. Ordering-wise harmless (valid, acyclic), but (a) it overstates a
   semantic claim the hypotheses don't back, and (b) it couples epochs: a
   concurrent-scenario failure in epoch 1 blocks restart evidence, and with
   `max_epochs 2` + one-repair-per-defect that concentrates risk. A cleaner
   edge would be `restart-durability` → `process-continuity` (which establishes
   the re-binding-across-SIGKILL mechanism) or no dependency edge.

5. **Low — Per-action timeout ceilings sum above the campaign budget.**
   `typecheck-noemit` 600 s + seven 900 s scenario actions = 6900 s nominal
   worst case vs. `max_elapsed_seconds 3600`. Empirically irrelevant (126.8 s
   for the 6-action v1-e2 run) and ceilings are not a budget — but if the
   8-action run is ever exercised, the heads-up is that a single epoch cannot
   guarantee all eight actions fit the campaign bound by plan alone. Optional
   tightening (e.g. 600 s scenario timeouts) would remove the theoretical
   mismatch.

## Disposition

1. accept-with-note — required next step is to extend `run-scenario.mjs` +
   `scenarios.mjs` with `concurrent` and `restart` (and register them in
   `SCENARIOS`) before any run is authored; treat those as part of the
   authorized implementation slice, not as a rerun of this model review.
2. fix-before-run — align H11 acceptance wording with v1's established
   "fresh transport connection" precedent, or deliberately implement a spawned
   `/usr/bin/node` client + add server-PID capture/compare to
   `server-fixture.mjs`. Do this during runner implementation.
3. fix-before-run — amend H10 acceptance bullet 2 so the non-ordering property
   is evidenced by an explicit summary field (e.g. `concurrentIssuance`,
   `matching`), keeping determinism unchanged.
4. accept-with-note — harmless as written; prefer re-wiring the dependency to
   `process-continuity` (or dropping the edge, relying on the sequence) if the
   model is re-touched. Do not hold admission on it.
5. accept-with-note — tighten scenario action timeouts if desired; no action
   required for admission.

## Hypothesis-to-evidence binding table

| Hypothesis | Requirement | Acceptance | Evidence action | argv |
| --- | --- | --- | --- | --- |
| [H10] concurrent-interleaving | `concurrent-interleaving` | 2 bullets: exactly six unique edges; per-receiver exactly two messages, one per sender; zero duplicates/cross-deliveries; zero missing edges; no ordering assumed; correlation by scenario/round/sender/receiver/edge-key | `concurrent-interleaving-scenario` | `/usr/bin/node harness/lifecycle/run-scenario.mjs concurrent --rounds 3` |
| [H11] restart-durability | `restart-durability` | 2 bullets: accepted-send immediately before SIGKILL + respawn; re-bind retained receiver session_id without re-registering; message received with attribution and edge-key; no re-registration; byte-for-byte survival; fresh process (new pid), health before re-bind | `restart-durability-scenario` | `/usr/bin/node harness/lifecycle/run-scenario.mjs restart --rounds 3` |

## Notes on the skill family (what helped / went stale)

- Correct and current this session: `metabuilder harness qualification
  prepare|report|check` (package verified no `metabuilder qualify` family);
  `harness check` / `harness compile` operate on the *compiled bundle*
  (`--input …/bundle.json`) — feeding `MODULE.json` yields "compiled harness is
  missing module", and the consumer-qualification skill *does* document
  `check --input metabuilder.bundle.json`, so the skill is right and only the
  prompt's shorthand "compile + check" under-specifies the input.
- Stale/incomplete: neither the sandbox-runtime skill nor the specs in
  `/home/rahul/Documents/metabuilder` (the doc/spec grep for
  `max_elapsed_seconds` returns nothing) documents (a) `max_action_timeout_seconds`
  is capped at 900 in the compiled `capability_ceiling` and (b) the campaign
  `max_elapsed_seconds` accretes campaign-globally across epochs (inferred from
  `journal.jsonl`). For an 8-action module whose per-action ceiling sum
  (6900 s) exceeds the 3600 s budget, that interaction is the sharp edge and
  had to be discovered, not read. Recommend the skill add an explicit note:
  per-action `timeout_seconds` are ceilings capped at 900; the campaign elapsed
  bound is the real budget.
- The "recompute aux digests with the current binary immediately before `harness
  author` / node_modules drifts silently" warning is accurate and already paid
  dividends: v1's pinned `node_modules` (6a94b302…) no longer matches the current
  tree, while v2's pins match this session's computation exactly.
- No guidance induced any unsafe action during this review; the only friction
  was discovering the elapsed-budget semantics and the check-input contract
  empirically.

Gate status: **passed (adequate-with-findings)**. The model stays unchanged;
the three fix-before-run items land as part of the runner/acceptance-wording
implementation slice that this review authorizes next, and a fresh run may be
authored only after `run-scenario.mjs` exposes the `concurrent` and `restart`
scenarios and the digests are recomputed.