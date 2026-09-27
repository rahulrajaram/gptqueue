# Code-review harness

This repository's standing multi-perspective code review, adapted from the
review-pipeline package at
`~/Documents/codereview/review-pipeline/` (whose perspective prompts
are used verbatim and whose parallel-arms-plus-barrier module semantics this
harness executes).

## When to convene

- After any arc that lands a new subsystem or crosses ~500 changed lines.
- Before pushing a batch of commits to the remote.
- Whenever the principal requests a review.

## Execution contract

0. **Dispatch throttling and provider backoff (mandatory).** Reviewer arms
   share one provider account, so an unbounded fan-out trips provider rate
   limits (observed 2026-09-12: ten simultaneous sub-agent dispatches; two
   arms failed with "Rate limit reached for requests" from the GLM
   provider). The controlling policy lives in the review package at
   `review-pipeline/scripts/provider_backoff.py` (`dispatch_wave`,
   `retry_with_backoff`, `backoff_delay`; covered by
   `tests/test_provider_backoff.py`) and is:

   - Dispatch arms in **bounded waves**: at most **4 concurrent arms**
     (`DEFAULT_CONCURRENCY`). An orchestrator using interactive sub-agents
     (e.g. the Task tool) applies the same bound by staggering dispatch —
     launch the next arm only as earlier ones complete.
   - A rate-limited arm is **re-dispatched with full-jitter exponential
     backoff**: delay = `random() * min(30s, 1s * 2**attempt)`, hard cap
     **30 seconds** (`MAX_BACKOFF_SECONDS`), up to 8 attempts
     (`DEFAULT_MAX_ATTEMPTS`). Randomization prevents a throttled fleet
     from re-synchronizing on the cap boundary.
   - Rate-limit detection is **provider-tolerant**: GLM/z.ai ("Rate limit
     reached for requests"), DeepSeek ("rate limit exceeded"), and
     OpenAI/Codex ("429", "insufficient_quota", "overloaded_error")
     phrasings all count as retryable throttles.
   - **Anything else fails loud and is never retried** — structural
     contract violations, identity mismatches, and genuine defects must
     surface immediately, as in an unassisted run.
   - A completed arm is never re-run; only the failed arm is. The barrier
     still requires every arm to produce a fresh, non-empty report.

1. **Arms.** Ten reviewer arms, one per perspective prompt in the package's
   `prompts/` directory: cleanliness, correctness, cyclomatic-complexity,
   fp-refine-adherence, idiomaticity, proximity-consistency, security,
   test-coverage, test-tautologicalness, theme-alignment. Each arm is one
   subagent (deepseek-v4-flash-0731 or the reviewer family the principal
   names) that:
   - reads its perspective prompt verbatim and follows it exactly;
   - reviews the target working tree READ-ONLY (no file creation, mutation,
     or deletion in the target repo);
   - writes its complete findings to
     `<run-root>/reviews/<perspective>.md` (create the run root first);
   - reports only a one-line severity summary in its final message.
2. **Barrier.** After all ten arms return, verify exactly ten non-empty
   `reviews/*.md` files. A missing or empty file fails the barrier: re-run
   that arm, never merge a half-valid result. The known provider glitch
   (tool-call markup emitted as final output with zero tool uses) is handled
   by discarding the turn and re-running the arm.
3. **Distillation.** One distillation arm reads the package's
   `prompts/distillation.md` plus all ten reviews and writes
   `<run-root>/distilled-review.md`. It must verify every high/critical
   claim against the target source before accepting it (mark unverifiable
   claims UNVERIFIED), deduplicate cross-arm findings with citations,
   reconcile severity conflicts keeping the higher severity with a note,
   and end with a prioritized top-10 fix list plus explicitly rejected
   findings with reasons.
4. **Cleanup.** Review arms never commit. The orchestrator presents the
   distilled report to the principal; fixes are then planned as normal
   governed slices.

## Run history

- `2026-08-29` — first convening: target = working tree after the
  reliability arc (24 commits). Run root:
  `~/Documents/codereview/review-pipeline/runs/gptqueue-20260829/`.
- `2026-09-27` — 16 perspectives (13 package defaults plus performance,
  resource-lifecycle, boundary-contracts) on tree `4632f20` (0.1.0 release
  candidate). Arms on `zai/glm-5.3-flash`, orchestration and distillation on
  `zai/glm-5.3` via `pi`, as a recorded operator extension. Run root:
  `/workspace/codereview/review-pipeline/runs/gptqueue-20260927-glm53/` in the
  development guest.

## Enforcement

The pre-push hook enforces that a push carries only reviewed work. The
implementation lives in `.githooks/lib/review-gate.sh`, wired into
`.githooks/pre-push` (which the installed `pre-push` dispatcher execs).

The gate is CONTENT-anchored, not commit-SHA-anchored: what it protects is
whether the pushed *tree* was reviewed, independent of how history was
shaped afterward. Squash, rebase, and reword all pass automatically when
the pushed content is the reviewed content.

Every `distilled-review.md` must declare a machine-readable
`reviewed-tree: <tree-digest>` line recording the exact tree it examined.

Decision table (pushed tree vs reviewed tree):

| Delta | Result |
|---|---|
| Identical digest | Pass ("history shape irrelevant") |
| Docs/notes only (`*.md`, `docs/`) | Allow with a loud notice |
| Any source surface (src, scripts, hooks, config, Lua) | Refuse; the unreviewed delta is named |
| Principal waiver (`.gptqueue/review-override`) | Allow with a loud waiver notice |
| Review package absent / no reviewed-tree digest | Fail-open with a warning (documented limitation) |

Known limitation: the rule is path-classified, not semantics-classified —
a docs-file change that alters agent behavior (e.g. AGENTS.md) passes
without review, so behavior-bearing prose changes should still trigger a
re-convene by convention.

Runs directory: the gate reads the newest `$CODE_REVIEW_RUNS_DIR/*/distilled-review.md`
(default `~/Documents/codereview/review-pipeline/runs`); set
`CODE_REVIEW_RUNS_DIR` when the review package lives elsewhere.

Waiver: to push reviewed-but-unreiterated work deliberately, create the
override marker (`touch .gptqueue/review-override`). The next push allows with
a WAIVER notice. No hook removes the marker; delete it when review work is
complete.
