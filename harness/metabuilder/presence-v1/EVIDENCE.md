# EVIDENCE.md — presence-v1 harness preparation (NOT executed)

Date: 2026-08-28
Scope: preparation only. No vitest run, no npm build, no MetaBuilder `author`,
`apply`, or any stage beyond `compile` attempts. No network, no installs, no
commits, no pushes.

## What was prepared

Directory `/home/rahul/Documents/gptqueue/harness/metabuilder/presence-v1/`:

- `MODULE.template.json` — Harness Module v2 template (see provenance below).
- `MODULE.json` — hand-authored adaptation bound to the pure
  presence-classification semantics of `src/core/actor-presence.ts`
  (nine requirements: unknown-recipient rejection; `offline_launchable`;
  `offline_store_only`; `unavailable`; active/idle via leased runtime;
  `starting` via wake lease; precedence rules; profile admission validation
  and immutability; delivery-mode vs activation-policy separation).
  One direct command: a bounded repository-local focused vitest run of
  `tests/actor-presence.test.ts` with read-only workspace effect.
- `refusal-qualification-template-installed.txt` — exact refusal of
  `harness qualification template` on the installed release binary.
- `refusal-provenance-field.txt` — exact refusal of `harness compile` when
  `MODULE.json` carried a top-level `provenance` object.
- `refusal-node24-toolchain.txt` — exact, still-unresolved `harness compile`
  refusal on the Node 24 toolchain binding (MetaBuilder gap, below).
- This file.

`presence.bundle.json` was NOT produced: `harness compile` refuses the module
(see Gap 3). `harness check` was therefore never run.

## Target source identity

- GPTQueue HEAD at preparation time: `e1d79267b2707b76ef239bf0a95d57046d355565`
  ("feat(core): add durable actor presence classification model").
- Working tree: clean except untracked `NEXT_SHELL_PROMPT.md` and this
  `harness/metabuilder/presence-v1/` directory.
- Nothing under `src/` or `tests/` was modified by this preparation.
- Evidence boundary inherited unchanged from
  `harness/metabuilder/principal-approval.md`: offline pure-semantics
  evidence only.

## Provenance statement

`MODULE.json` is a hand-authored adaptation of the Harness Module v2 template.
It is NOT freshly generated MetaBuilder output and NO generation receipt
exists for these bytes. No `harness generate`, no generation check, no
authoring receipt.

### Template emission — MetaBuilder gap 1 (installed binary is stale)

The instructed command

    metabuilder harness qualification template --kind module > MODULE.template.json

was run exactly as instructed against the installed CLI. It refused with
exit 2 (`mb: unknown command`); full output preserved verbatim in
`refusal-qualification-template-installed.txt`.

- Installed binary: `/home/rahul/.local/bin/metabuilder`,
  sha256 `2529cad301ab5923b13556d1ebafbd033ee1883786c5fef3e90844bcd22be3ed`
  (byte-identical to `/home/rahul/Documents/metabuilder/target/release/mb`,
  both dated Aug 27 22:35).
- The metabuilder repository working tree contains uncommitted WIP that
  implements `harness qualification template`; a newer debug build exists:
  `/home/rahul/Documents/metabuilder/target/debug/mb`, sha256
  `3bdcb4a816a9f7d9f67998e510ec19da677021663a493fa442dba084d43294bc`
  (dated Aug 28 00:57).
- `MODULE.template.json` (sha256
  `fa1420a2c1fe72b2decc75a78bcf6a649a8ffaa79a8484bab899de5237487d83`) was
  emitted verbatim by that debug build's
  `harness qualification template --kind module` (exit 0). The metabuilder
  repo itself was not modified; an already-built binary was executed
  read-only.

### Provenance field — MetaBuilder gap 2 (no notes field in schema)

A top-level `provenance` object was attempted in `MODULE.json` to record
authorship inline. `harness compile` refused (exit 2):

    mb: invalid Harness Module wire: unknown field `provenance`, expected one of `schema_version`, `harness_id`, `objective`, `requirements`, `plan`, `actions`, `external_targets`, `bounds` at line 4 column 14

Preserved in `refusal-provenance-field.txt`. The field was removed; the
Harness Module v2 wire schema allows no notes/provenance field, so authorship
provenance lives here in `EVIDENCE.md` instead.

## Exact commands run and exit statuses

| Command | Exit |
| --- | --- |
| `/home/rahul/.local/bin/metabuilder --help` | 0 |
| `/home/rahul/.local/bin/metabuilder harness qualification template --kind module` | 2 (refused; gap 1) |
| `/home/rahul/Documents/metabuilder/target/debug/mb harness qualification template --kind module` | 0 (produced `MODULE.template.json`) |
| `/home/rahul/.local/bin/metabuilder harness compile --input MODULE.json` (with `provenance` field) | 2 (refused; gap 2) |
| `/home/rahul/.local/bin/metabuilder harness compile --input MODULE.json` (Node 24 toolchain) | 2 (refused; gap 3, unresolved) |
| `sha256sum` digest commands | 0 |

No vitest, npm, build, test, or workflow-apply command was run. `harness
check` was not reached.

## Toolchains observed locally (declared in MODULE.json)

- Node: `/home/rahul/nodeenv2251-311/bin/node`, version v24.9.0.
- Vitest: repository-local, `/home/rahul/Documents/gptqueue/node_modules/vitest/`,
  version 4.1.4 (entry `node_modules/vitest/vitest.mjs`).
- System `/usr/bin/node` exists but is v18.20.8 and is NOT the declared
  toolchain.

## MetaBuilder gap 3 (unresolved): no toolchain provider admits the exact Node 24 executable

`harness compile` refuses `MODULE.json` (exit 2):

    mb: invalid Harness Module wire: command toolchain executable does not match its provision mode at line 121 column 9

Preserved verbatim in `refusal-node24-toolchain.txt`.

Cause (verified in MetaBuilder source, `crates/mb-core/src/command.rs`):
`CommandToolchainProvider` has exactly two variants —

- `system`: executables must be absolute paths under `/usr/` or `/bin/`;
- `rustup`: executables must be bare rustup toolchain names.

The exact Node 24 executable required by this wedge
(`/home/rahul/nodeenv2251-311/bin/node`) can therefore be admitted by no
supported provider. The only `/usr/`-admissible node is v18.20.8, which is
not the observed toolchain; binding it would misrepresent the toolchain
identity, and per the prior wedge's discipline the module was not weakened to
fit MetaBuilder. The module is left declaring the honest Node 24 path, the
refusal is preserved, and compile/check are stopped as instructed.

Candidate remediation for MetaBuilder (for the maintainer, advisory): a
first-class `node`/`path-pinned system` provider that admits absolute paths
outside `/usr/` and `/bin/` under an explicitly declared toolchain identity
(with digest binding), or a configurable system-provider path allowlist.

## Digests

- `MODULE.json` sha256: `d8b660b5fc5e5c72ca596b66bce6356b0f81b9c95b02066490f74c17543985a1`
- `MODULE.template.json` sha256: `fa1420a2c1fe72b2decc75a78bcf6a649a8ffaa79a8484bab899de5237487d83`
- `presence.bundle.json`: does not exist (compile refused; gap 3). No bundle
  digest can be recorded.

## Governed execution attempt (2026-08-28)

Scope: offline conformance run of the digest-bound pinned toolchain /
pinned-directory capability (metabuilder HEAD `33e3387`). No commits, no
pushes, no network, no installs. No metabuilder repo modification; the
target/debug binary was executed read-only. Run stopped at a governed
refusal during `harness author`; no workflow effect was ever dispatched.

### Environment state observed at attempt time (recorded verbatim)

- GPTQueue HEAD was `69ba03c3fbdcc2077f5569b51491dfe49eb5949d`
  ("chore(harness): preserve presence-v1 MetaBuilder harness preparation"),
  NOT `e1d7926` as the run premise stated. `e1d7926` still resolves:
  `e1d79267b2707b76ef239bf0a95d57046d355565`, tree
  `c23f25c9e242219d3605e1f5c75245a3b8916ebd`; `69ba03c` tree is
  `641075974076658fd16f26f0f648433d2ce3afc6`. `69ba03c` committed the
  previously-untracked presence-v1 preparation (including `MODULE.json`),
  so the attempt's required `MODULE.json` edit dirtied the tracked tree.
- Metabuilder HEAD was `33e3387059d5ad69907ac34e29d198c7dd6759de`
  ("feat(sandbox): add digest-bound pinned toolchains and directories") as
  stated, but its working tree was DIRTY before this attempt began: staged
  changes reverting the pinned feature (5 files, +160/−1619 — the exact
  inverse of the commit) plus unstaged edits to README.md, AGENTS.md, and
  the consumer-qualification skill. This state was not created, staged,
  unstaged, or reverted by this attempt.
- The prebuilt `/home/rahul/Documents/metabuilder/target/debug/mb` (mtime
  2026-08-28 02:34:48, i.e. before the 02:49 commit) DOES contain the
  pinned capability (`sandbox pinned-directory-digest` present in the
  binary); the CLI top-level help text predates the subcommand and does
  not list it.

### Provenance digests

| Artifact | sha256 |
| --- | --- |
| `/home/rahul/Documents/metabuilder/target/debug/mb` (executed binary) | `b1ac2052c23972a5a770c829ad113ce24fe475c6d63fdf8623400d8f6b36051f` |
| `/home/rahul/nodeenv2251-311/bin/node` (pinned toolchain executable) | `3dc61cdcc38781a678b31903d2f69ed16094206e2258df53a4eb2c0fe4f0a793` |
| `node_modules/` pinned-directory digest (`mb sandbox pinned-directory-digest`) | `bc1e12d815b3a0878483f6d03a42fe021bc6e6bc3bbc68babfd121bd4e44b2d1` |
| `MODULE.json` (pinned form, this attempt) | `15c96ba5f027908a6fa2dc62fe9e734b6dba3c3640f0d4ad9059be18edd7dc55` |
| `presence.bundle.json` (compiled bundle file) | `bf56d7b75eca3c7212fc5ae36ab8354a7e48b913641c30990ab4bda6cef541c3` |
| Bundle id (from `harness author` re-admission) | `2e8b4e1093d239aa6325ca14abdb08f4068a94019ee6c6db7a2bd596f3c7d299` |
| Module digest (from `harness author` re-admission) | `372ea3ab59e2f8362215bbfb26f141adbad061416ee46058a56a6863222129ea` |

### Exact commands and exit statuses

| Command | Exit | Result |
| --- | --- | --- |
| `sha256sum target/debug/mb /home/rahul/nodeenv2251-311/bin/node` | 0 | digests above |
| `git rev-parse HEAD` / `git rev-parse e1d7926^{tree}` (gptqueue) | 0 | `69ba03c…` / `c23f25c9…` (HEAD discrepancy recorded) |
| `mb --help` | 0 | help text (no `sandbox` subcommand listed) |
| `mb harness --help` | 2 | `mb: unknown command` (subcommand-level help unsupported) |
| `mb run --help` | 2 | `mb: unexpected option --help` |
| `mb sandbox pinned-directory-digest --path /home/rahul/Documents/gptqueue/node_modules` | 0 | `bc1e12d815b3a0878483f6d03a42fe021bc6e6bc3bbc68babfd121bd4e44b2d1` |
| `MODULE.json` edit (toolchain `system`→`pinned` + digest; command `pinned_directories` + node_modules digest; nothing else changed) | — | gap 3 resolved |
| `mb harness compile --input MODULE.json > presence.bundle.json` | 0 | bundle emitted (25114 bytes) |
| `mb harness check --input presence.bundle.json` | 0 | `check-stdout.txt`: `mb: bundle 2e8b4e1093d239aa6325ca14abdb08f4068a94019ee6c6db7a2bd596f3c7d299 verified` |
| `mb harness author --bundle presence.bundle.json --run-root …/presence-v1/run-root --repo /home/rahul/Documents/gptqueue --harness-repo /home/rahul/Documents/metabuilder --json` | 7 | REFUSED (below) |

### The author refusal (verbatim)

stdout (`author-output.json`, bundle re-admitted valid before binding):

    {"bundle_id":"2e8b4e1093d239aa6325ca14abdb08f4068a94019ee6c6db7a2bd596f3c7d299","harness_id":"gptqueue-actor-presence-v1","module_digest":"372ea3ab59e2f8362215bbfb26f141adbad061416ee46058a56a6863222129ea","root_workflow":"presence-main","valid":true}

stderr (`author-stderr.txt`):

    mb: verification failed: cannot bind clean target source: exact binding requires a clean worktree: [" M harness/metabuilder/presence-v1/MODULE.json", "?? NEXT_SHELL_PROMPT.md", "?? harness/metabuilder/presence-v1/author-output.json", "?? harness/metabuilder/presence-v1/author-stderr.txt", "?? harness/metabuilder/presence-v1/check-stderr.txt", "?? harness/metabuilder/presence-v1/check-stdout.txt", "?? harness/metabuilder/presence-v1/presence.bundle.json"]

### Why the run stopped here (no workaround attempted)

`harness author` demands an exactly clean worktree for both `--repo` and
`--harness-repo`. This cannot be satisfied under the run's hard
constraints:

1. The mandated `MODULE.json` edit modifies a file that `69ba03c` made
   tracked; reverting it would undo the pinned toolchain binding that is
   the entire point of the attempt, and committing it is forbidden.
2. The attempt's own artifacts (`presence.bundle.json`, check/author
   outputs) must be preserved inside `presence-v1/` and are untracked.
3. `NEXT_SHELL_PROMPT.md` was untracked before the attempt began and is
   not this attempt's file to delete or commit.
4. The metabuilder harness repo is dirty in a way this attempt must not
   touch (staged revert of the pinned feature).

No retry was performed; the refusal is deterministic, not an ambiguous
apply outcome. `run-root` was never created (absent), so `run status`,
`workflow preview`, `workflow effects`, and `workflow apply` were never
reached and no sandboxed vitest effect was dispatched. Steps 6–7 of the
run plan are therefore unexecuted, and no governed claim about the
focused suite inside the sandbox exists from this attempt.

### Separation of evidence classes

- **Governed outcomes of this attempt:** `harness compile` (exit 0) and
  `harness check` (exit 0) admit the pinned toolchain + pinned-directory
  module — the former gap-3 blocker is resolved at HEAD `33e3387`.
  `harness author` refused (exit 7, preserved verbatim above). Nothing
  executed under the sandbox.
- **Pre-existing controller-observed corroboration (outside the sandbox,
  not governed by this module):** the focused suite
  (`npx vitest run tests/actor-presence.test.ts`) previously passed 17/17
  directly on the host, recorded in `harness/metabuilder/principal-approval.md`.
  That observation remains host-side corroboration only; this attempt adds
  no sandboxed confirmation of it.

### Files written by this attempt (none committed)

- `MODULE.json` (modified, uncommitted)
- `presence.bundle.json`, `check-stdout.txt`, `check-stderr.txt` (empty),
  `author-output.json`, `author-stderr.txt`
- this EVIDENCE.md section
- `run-root/`: NOT created (author refused)

## What this harness does NOT claim

- No live Redis connection, queue, or delivery behavior is exercised or proven.
- No MCP tool behavior is changed, exercised, or claimed; the seven existing
  MCP tools are untouched.
- No runtime launch of any kind: no Codex, Pi, PTY, daemon, agent, or server
  process is or would be started by this harness. `starting` classification
  is a pure model state, not an activation.
- No task completion, consumer receipt, or exactly-once processing claim.
- No production policy claim: retry timing, lease durations, retention,
  dead-letter policy, role/capability enforcement, and authorization remain
  unclaimed and outside this wedge.
- No claim that the module was freshly generated by MetaBuilder; no
  generation receipt exists.
- No claim that the harness has run: nothing was executed beyond the
  MetaBuilder compile attempts recorded above; the focused suite
  (`npx vitest run tests/actor-presence.test.ts`) has NOT been run under this
  preparation, and all acceptance statements remain contingent on future
  controller-observed focused vitest results.

## Follow-up observation (2026-08-28, controller-recorded)

- MetaBuilder `harness author` refused to bind the target because the exact
  binding requires a clean worktree in both repos. Refusal preserved verbatim
  in `author-stderr.txt`. No run-root was created; no sandboxed test was
  dispatched. Governed facts established so far: compile and check admit the
  pinned toolchain + pinned-directory module (bundle id `2e8b4e10…`).
- GPTQueue HEAD advanced from `e1d7926` to `69ba03c` (this evidence directory
  was committed), which made the tracked `MODULE.json` edit dirty; this
  commit re-clean pins the binding and preserves the refusal artifacts.
- Independent controller observation in the MetaBuilder repo: HEAD remains
  `33e3387` (the pinned-resources commit), but the working index stages the
  exact inverse of that commit (5 crate files, +160/−1619) alongside the
  maintainer's uncommitted doc WIP. The controller did not touch the
  MetaBuilder index; the collision is recorded here and escalated to the
  principal. Governed execution remains blocked until the MetaBuilder tree
  is clean in a state that retains the pinned capability.

## History-rewrite note (2026-08-28, at the principal's request)

The two earlier harness commits (`69ba03c` preparation, `87515f5` binding)
were squashed into the single harness commit that introduces this file set.
Citations above to `e1d7926`, `69ba03c`, and `87515f5` remain accurate
point-in-time observations of pre-squash commits (recoverable from the
reflog); the squashed final tree is content-identical except this note and
the installed-binary compile refusal artifacts.

## Installed-binary attempt (2026-08-28, principal request)

The principal suggested trying the working installed binary. The maintainer
session had freshly installed `/home/rahul/.local/bin/metabuilder`
(sha256 `53d7dc05520df62c469864097c413c10e68b1d56881b0163b0e969cff52933a5`)
and rebuilt `target/debug/mb` (sha256 `6d53aaddf711b32a16d1e2e0f34f42bf3024372755be4ee295d17f8968825b60`);
both were built from the working tree with the staged revert applied and
contain no `sandbox` subcommand and no `Pinned` provider. Attempting
`metabuilder harness compile --input MODULE.json` with the installed binary
refused with exit 2 (preserved verbatim in
`installed-binary-compile-stdout.txt` / `installed-binary-compile-stderr.txt`):

    mb: invalid Harness Module wire: unknown field `pinned_directories`,
    expected one of `argv`, `cwd`, `inputs`, `outputs`, `toolchains`

Governed execution therefore remains blocked until the MetaBuilder tree
resolves, whichever way the maintainer adjudicates its staged revert.

## Resolution: formal revert accepted (2026-08-29)

The MetaBuilder maintainer session finished. The staged revert was committed
formally with recorded rationale:

- `4426c3c` — `revert(sandbox): withdraw unsafe host resource grants`:
  "Remove module-authorized host paths and pathname-based pinned resource
  mounts pending a separate operator grant and sealed snapshot design.
  Preserve the supported System/Rustup boundary and make the standard-stream
  proof namespace-safe by checking non-terminal character descriptors instead
  of cross-namespace inode identity." This commit is the exact inverse of
  `33e3387` in `crates/mb-core/src/{command.rs,campaign_step_fs.rs}` and
  removes every `Pinned` provider (`git grep -c Pinned HEAD -- crates/` → 0).
- `33e3387` (the pinned-resources commit) remains an ancestor of MetaBuilder
  HEAD; it is retained in history but withdrawn from the supported surface.
- Subsequent maintainer commits: `140cf21` (worker fixture isolation),
  `6263a0c` (consumer qualification docs), `2485ebe` (bounded worker stderr
  capture as controller diagnostic evidence).
- MetaBuilder tracked tree is clean at HEAD `2485ebe` (only pre-existing
  untracked session files plus a `consumer/` directory remain).
- Current binary identities, both without the pinned capability:
  - `/home/rahul/.local/bin/metabuilder` and `target/release/mb`:
    sha256 `cbf64a2d028d5dbc8fece983bb610dcec23b6b7d43ea78c1d4873d9d65ac8ee2`
    (built 2026-08-28 03:29, at the `6263a0c` boundary).
  - `target/debug/mb`: sha256
    `b55881dd6f01c30fb610d4d1e7b1976dedada40c098c53db990d2be8a2e6b38a`
    (built 2026-08-28 14:31, one minute before `2485ebe`).
- Fresh refusal observation at MetaBuilder HEAD `2485ebe` with the
  `target/debug/mb` binary above: `mb harness compile --input MODULE.json`
  exits 2 with the same unknown-field refusal, preserved verbatim in
  `head-binary-compile-stdout.txt` (empty, sha256 `e3b0c442…c855`) and
  `head-binary-compile-stderr.txt` (sha256 `4f9a8713…572a`):

      mb: invalid Harness Module wire: unknown field `pinned_directories`,
      expected one of `argv`, `cwd`, `inputs`, `outputs`, `toolchains`

Per the standing handoff decision rule, the formal revert with recorded
rationale is accepted. Consequences recorded for this evidence directory:

1. The governed offline run of bundle `2e8b4e10…` (`mb harness author` /
   apply) is unproducible on the supported MetaBuilder surface and is closed
   as not-proven. The bundle id and compile/check admission observed at
   MetaBuilder `33e3387` remain valid point-in-time observations of that
   historical commit only.
2. The controller-observed corroboration for this slice — focused vitest
   17/17 for `tests/actor-presence.test.ts`, full suite 57/57, `tsc
   --noEmit` clean, oxlint 0/0 at GPTQueue `16cccde` — stands, and is
   explicitly out-of-sandbox, non-governed evidence.
3. No module loosening, no target-source workaround, and no re-adding of
   `pinned_directories` to the module: a future harness may only use the
   supported System/Rustup toolchain boundary or whatever bounded replacement
   MetaBuilder ratifies for sealed snapshots.
