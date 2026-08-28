# Advisory gap — MetaBuilder toolchain provider cannot admit a path-pinned Node 24

Date: 2026-08-28
Target: GPTQueue slice `gptqueue-actor-presence-v1` (module `MODULE.json`,
sha256 `d8b660b5fc5e5c72ca596b66bce6356b0f81b9c95b02066490f74c17543985a1`)
MetaBuilder: installed `/home/rahul/.local/bin/metabuilder` (stale, lacks
`harness qualification`) and uncommitted WIP `target/debug/mb`
(sha256 `3bdcb4a8…`, which served the module template verbatim).

## The gap

`harness compile` refuses the module's only honest toolchain binding:

    mb: invalid Harness Module wire: command toolchain executable does not
    match its provision mode at line 121 column 9

Root cause in `crates/mb-core/src/command.rs`
(`CommandToolchainRequirement::new`):

- `CommandToolchainProvider::System` accepts only absolute executables under
  `/usr/` or `/bin/` (with no `.`/`..` path parts).
- `CommandToolchainProvider::Rustup` accepts only bare toolchain names.

The exact observed Node 24 executable required by the focused suite is
`/home/rahul/nodeenv2251-311/bin/node` (v24.9.0). The only `/usr/bin/node`
is v18.20.8, and vitest 4.1.4 requires `^20 || ^22 || >=24`; executing under
`/usr/bin/node` fails in ESM loading. Substituting it would bind a
misrepresentative toolchain, so the module keeps the honest Node 24 path and
the compile refusal is preserved (`refusal-node24-toolchain.txt`). The
weakening discipline in the MetaBuilder README was honored: nothing was
routed around and nothing is labeled governed.

## Requested generic capability (advisory; MetaBuilder retains judgment)

A third provision mode — e.g. `CommandToolchainProvider::Pinned` or a
configurable allowlist extension of `System` — that:

1. accepts absolute executable paths outside `/usr` and `/bin`;
2. binds each executable's SHA-256 digest in the requirement so bundles stay
   content-addressed and replayable;
3. records the resolved executable + digest in the run journal so
   controller-observed evidence can distinguish the pinned toolchain from a
   system default;
4. preserves refusal behavior for paths that fail digest verification.

This keeps target-specific behavior outside the core: the provider remains
generic (any absolute path + digest), with GPTQueue supplying the path and
digest in its module.

## Secondary gaps observed

1. The installed `metabuilder` binary predates the `harness qualification`
   subcommands; the template had to be emitted by the WIP
   `target/debug/mb`. Reinstalling is a MetaBuilder-side action.
2. Harness Module wire format rejects unknown top-level fields, so
   hand-authorship provenance cannot be embedded in the module; it lives in
   `EVIDENCE.md` instead. A first-class `provenance` object (author,
   adaptation-of-template digest, absence-of-generation-receipt statement)
   would remove the misrepresentation risk at the artifact level.

## Current evidence state for this slice

- Governed offline execution: **blocked** by the gap above; no bundle exists,
  no `harness author`/`apply` was attempted.
- Controller-observed corroboration (separate class, per the approved
  evidence boundary): focused suite 17/17, full suite 57/57,
  `tsc --noEmit` exit 0, oxlint 0 warnings/errors at GPTQueue commit
  `e1d7926`.
