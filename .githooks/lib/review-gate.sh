#!/usr/bin/env bash
# commithooks/lib/review-gate.sh — Pre-push review-freshness gate.
#
# Usage: source "$REPO_ROOT/.githooks/lib/review-gate.sh"
#
# Blocks a push when any pushed commit is not covered by the latest distilled
# code review (see docs/CODE_REVIEW_HARNESS.md). The gate is deliberately
# fail-open: it warns and allows when the code-review package directory is
# absent or when a recorded scope HEAD cannot be resolved in this repo, so an
# environment without review state never silently bricks normal pushes.
#
# Escape hatch: if the file <repo-root>/.gptqueue/review-override exists, the
# push proceeds after printing a loud WAIVER notice. That file is the
# principal's deliberate override and must never be auto-deleted by a hook.
#
# Env:
#   CODE_REVIEW_RUNS_DIR  override the runs dir (default:
#                         /home/rahul/Documents/codereview/review-pipeline/runs)

if [ "${_COMMITHOOKS_REVIEW_GATE_LOADED:-}" = "1" ]; then
  return 0
fi
_COMMITHOOKS_REVIEW_GATE_LOADED=1

# Locate the newest distilled-review.md and extract the scope HEAD sha (short
# form as written in the report), resolving it to a full sha via git.
# Sets: REVIEW_GATE_NEWEST_FILE, REVIEW_GATE_HEAD (full sha).
# On success returns 0; on "fail-open" conditions returns 2 after warning.
commithooks_resolve_review_scope() {
  local runs_dir="${CODE_REVIEW_RUNS_DIR:-/home/rahul/Documents/codereview/review-pipeline/runs}"

  if [ ! -d "$runs_dir" ]; then
    commithooks_warn "[pre-push] REVIEW-GATE UNAVAILABLE: no code-review runs dir at:"
    commithooks_warn "            $runs_dir"
    commithooks_warn "  The code-review package directory is absent; allowing push with the"
    commithooks_warn "  review-freshness gate DISABLED. (Documented limitation in"
    commithooks_warn "  docs/CODE_REVIEW_HARNESS.md — no review state to compare against.)"
    commithooks_warn "  Re-run the harness per docs/CODE_REVIEW_HARNESS.md to re-arm the gate."
    return 2
  fi

  local entry
  entry="$(find "$runs_dir" -maxdepth 2 -type f -name distilled-review.md \
             -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n1)"
  if [ -z "$entry" ]; then
    commithooks_warn "[pre-push] REVIEW-GATE UNAVAILABLE: no distilled-review.md under $runs_dir."
    commithooks_warn "  Allowing push (nothing to compare against)."
    return 2
  fi

  REVIEW_GATE_NEWEST_FILE="${entry#* }"

  # Scope HEAD is written as `Scope verified: HEAD `ff2ac66`, ...` in the
  # distilled report; accept short shas (7-40 hex) and resolve to the full sha.
  local cand=""
  cand="$(sed -n 's/.*HEAD[[:space:]]*`\([0-9a-fA-F]\{7,40\}\)`.*/\1/p' \
            "$REVIEW_GATE_NEWEST_FILE" | head -n1)"
  if [ -z "$cand" ]; then
    cand="$(sed -n 's/.*HEAD[[:space:]:]*[[:space:]]*\([0-9a-fA-F]\{7,40\}\).*/\1/p' \
              "$REVIEW_GATE_NEWEST_FILE" | head -n1)"
  fi

  if [ -z "$cand" ]; then
    commithooks_warn "[pre-push] REVIEW-GATE UNAVAILABLE: no scope HEAD found in"
    commithooks_warn "            $REVIEW_GATE_NEWEST_FILE"
    commithooks_warn "  Allowing push (scope not recorded)."
    return 2
  fi

  REVIEW_GATE_HEAD="$(git rev-parse --verify "$cand^{commit}" 2>/dev/null || true)"
  if [ -z "$REVIEW_GATE_HEAD" ]; then
    commithooks_warn "[pre-push] REVIEW-GATE UNAVAILABLE: could not resolve scope HEAD '$cand'"
    commithooks_warn "  (from $REVIEW_GATE_NEWEST_FILE) in this repository."
    commithooks_warn "  Allowing push (fail-open)."
    return 2
  fi

  return 0
}

# Enforce the review-freshness rule for the given pushed (local ref, local sha)
# pair. Returns 0 if covered, 1 if the tip contains unreviewed commits.
commithooks_check_pushed_ref() {
  local local_ref="$1"
  local local_sha="$2"
  local zero="0000000000000000000000000000000000000000"

  [ "$local_sha" = "$zero" ] && return 0   # branch deletion — nothing to review
  [ -n "$local_sha" ] || return 0

  local count
  count="$(git rev-list --count "$REVIEW_GATE_HEAD..$local_sha" 2>/dev/null || true)"
  if [ -z "$count" ] || [ "$count" -le 0 ]; then
    return 0
  fi

  commithooks_red "[pre-push] BLOCK: $local_ref contains $count commit(s) created after the last code review."
  commithooks_red "  Review scope HEAD: $REVIEW_GATE_HEAD  ($REVIEW_GATE_NEWEST_FILE)"
  commithooks_red "  Unreviewed commits in $REVIEW_GATE_HEAD..$local_sha:"
  # shellcheck disable=SC2086
  git log --oneline "$REVIEW_GATE_HEAD..$local_sha" 2>/dev/null | sed 's/^/    /' >&2 || true
  return 1
}

# Main gate. Reads the pre-push stdin stream:
#   <local ref> <local sha> <remote ref> <remote sha>
# Args passed through are the remote name/url (ignored here).
commithooks_check_review_freshness() {
  local repo_root
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  [ -n "$repo_root" ] || repo_root="$PWD"
  local override_file="$repo_root/.gptqueue/review-override"

  # 1. Principal's deliberate escape hatch — always honored, never auto-deleted.
  if [ -f "$override_file" ]; then
    commithooks_warn "==========================================================="
    commithooks_warn "[pre-push] REVIEW-FRESHNESS WAIVER"
    commithooks_warn "  .gptqueue/review-override present: $override_file"
    commithooks_warn "  Push proceeds WITHOUT the review-freshness gate."
    commithooks_warn "  This override was created deliberately by the principal"
    commithooks_warn "  and will not be removed automatically."
    commithooks_warn "==========================================================="
    return 0
  fi

  # 2. Resolve the latest distilled review scope (fail-open on absence).
  local REVIEW_GATE_HEAD=""
  local REVIEW_GATE_NEWEST_FILE=""
  if ! commithooks_resolve_review_scope; then
    return 0  # warned already; allow
  fi

  # 3. Evaluate each pushed branch tip.
  local local_ref local_sha remote_ref remote_sha
  local blocked=0
  local seen=0
  while read -r local_ref local_sha remote_ref remote_sha; do
    [ -n "$local_ref" ] || continue
    seen=1
    if ! commithooks_check_pushed_ref "$local_ref" "$local_sha"; then
      blocked=1
    fi
  done

  if [ "$blocked" = "1" ]; then
    commithooks_red ""
    commithooks_red "  REFUSING PUSH: unreviewed commits detected above."
    commithooks_red "  Run the code-review harness (docs/CODE_REVIEW_HARNESS.md) on the new arc,"
    commithooks_red "  then push once it records the new scope HEAD in a distilled review."
    commithooks_red "  ESCAPE HATCH: for a deliberate exception, create $override_file"
    commithooks_red "  and push again — the gate will then print a WAIVER notice and allow."
    return 1
  fi

  if [ "$seen" = "1" ]; then
    commithooks_green "[pre-push] Review-freshness gate passed: all pushed commits are at or before"
    commithooks_green "            review scope HEAD $REVIEW_GATE_HEAD."
  fi
  return 0
}