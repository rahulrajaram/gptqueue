# Agent execution baseline

A capability contract for running Codex-based agents autonomously. The
config must be expressive enough **by construction** (versioned keys and
tiers) and **proven by behavior** (executable probes) — never assumed from
a file that merely looks right. Background: docs/QUICKSTART.md
"Approval-restricted Codex sessions".

## Tiers

| Tier | Audience | Contents |
|---|---|---|
| `base` | every environment (host and sandbox) | `sandbox_mode = "workspace-write"`, `[sandbox_workspace_write] network_access = true`, trusted project paths, per-server `mcp_servers."gptqueue-shared".default_tools_approval_mode = "approve"` |
| `sandbox-auto` | automation launches | `base` + `approval_policy = "never"` (layer via `codex -p sandbox-auto`) |
| `sandbox-max` | sandbox only, opt-in | `sandbox-auto` + `sandbox_mode = "danger-full-access"` (layer via `codex -p sandbox-max`) |

Templates live in `config/codex/`; deployment copies go to
`$CODEX_HOME/<name>.config.toml` (the `-p/--profile` lookup path).

Why `never` for automation instead of `on-request` + auto-review: a
headless run gated on an approval decision can deadlock or silently stall;
`never` + a generous sandbox makes every outcome deterministic — the
operation either runs inside the sandbox or fails fast with the exact
approval error, which pipelines can trap and report.

Why per-server `approve` on the messaging server: verified on codex
0.157.0, `"auto"` does NOT unblock MCP tool calls under a global `never`
policy and neither do `readOnlyHint` annotations; `"approve"` does, and it
scopes the liberty to gptqueue tools only. Installers and hooks never
write this key — it is an explicit operator choice recorded here.

## Parity rule: host ⊆ sandbox

The host is the more restrictive environment by design. Every capability
granted in the host tier must also exist in the sandbox tier; the sandbox
may add liberties (the `sandbox-auto`/`sandbox-max` profiles) that a bare
host must refuse. Both environments share this manifest via git, so
parity is checkable per-environment with the same tooling:

```bash
node scripts/gptqueue-doctor.mjs config            # lint local $CODEX_HOME
node scripts/gptqueue-doctor.mjs config --tier sandbox-auto
```

`doctor config` is the cheap static check. The behavioral proof is the
acceptance matrix (`GPTQUEUE_CODEX_APPROVAL=1`), which asserts both the
rejection signature under `never` and the remedy under `approve` — run it
after any codex upgrade, since upstream enum/key changes are the main
drift risk.

## Required keys

| Key | Value | Tier | Verified by | Why |
|---|---|---|---|---|
| `sandbox_mode` | `workspace-write` | base | `doctor config` | headless builds/tests need writes without escalation |
| `sandbox_workspace_write.network_access` | `true` | base | `doctor config` | loopback LLM gateway, MCP servers, package installs |
| `mcp_servers."gptqueue-shared".default_tools_approval_mode` | `approve` | base | `doctor config` | peer lookup + messaging usable under any approval policy |
| `projects.<path>.trust_level` | `trusted` | base | manual | no per-project trust prompts for known trees |
| `$CODEX_HOME/sandbox-auto.config.toml` | exists | sandbox | `doctor config --tier sandbox-auto` | automation tier |
| `$CODEX_HOME/sandbox-max.config.toml` | exists | sandbox | `doctor config --tier sandbox-max` | VM-contained maximum liberty tier |

`sandbox-max` is legal only where execution is already VM-contained; a
bare host must not carry it.

## One-shot preflight probe

```bash
codex exec -p sandbox-auto --skip-git-repo-check -C /tmp/preflight \
  'Create the file hello.txt containing "ok", then call the MCP tool
   list_agents on gptqueue-shared. Reply DONE if both succeeded, or the
   exact errors.'
```

Both operations must succeed for the tier to be considered usable.
