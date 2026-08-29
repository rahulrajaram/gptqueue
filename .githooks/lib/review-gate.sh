#!/usr/bin/env bash
# commithooks/lib/review-gate.sh — content-anchored review-freshness gate.
#
# The gate's guarantee is CONTENT coverage, not commit-SHA coverage:
# history rewrites (squash, rebase, reword) pass automatically when the
# pushed tree is identical to a reviewed tree; any source-surface change
# since the reviewed tree requires a fresh review. Docs-only deltas pass
# with a notice. The principal's waiver (.gptqueue/review-override)
# overrides everything.

if [ "${_COMMITHOOKS_REVIEW_GATE_LOADED:-}" = "1" ]; then
  return 0
fi
_COMMITHOOKS_REVIEW_GATE_LOADED=1

COMMITHOOKS_REVIEW_RUNS_DIR="${CODE_REVIEW_RUNS_DIR:-$HOME/Documents/codereview/review-pipeline/runs}"

# Classify a changed path: 0 = source surface (requires review),
# 1 = docs/notes only (allowed with notice).
commithooks_path_is_docs_only() {
  case "$1" in
    *.md|NEXT_SHELL_PROMPT.md|docs/*) return 0 ;;
    *) return 1 ;;
  esac
}

# Read the newest run's reviewed-tree digest. Echoes the digest, or returns 1.
commithooks_resolve_reviewed_tree() {
  local latest
  latest="$(ls -1t "$COMMITHOOKS_REVIEW_RUNS_DIR"/*/distilled-review.md 2>/dev/null | head -1 || true)"
  [ -n "$latest" ] || return 1
  grep -m1 '^reviewed-tree: ' "$latest" 2>/dev/null | awk '{print $2}'
}

commithooks_check_review_freshness() {
  # Consume the pre-push ref stream: lines of "<local> <local-sha> <remote> <remote-sha>".
  local pushed_shas=()
  local line lref lsha
  while read -r lref lsha _rest; do
    [ -n "$lsha" ] || continue
    case "$lref" in refs/heads/*) pushed_shas+=("$lsha") ;; esac
  done
  [ "${#pushed_shas[@]}" -gt 0 ] || return 0

  local reviewed_tree
  if ! reviewed_tree="$(commithooks_resolve_reviewed_tree)"; then
    echo "[pre-push] WARNING: no code-review runs found in $COMMITHOOKS_REVIEW_RUNS_DIR — review gate DISABLED (fail-open)." >&2
    return 0
  fi
  if [ -z "$reviewed_tree" ]; then
    echo "[pre-push] WARNING: newest distilled review has no reviewed-tree digest — review gate DISABLED (fail-open)." >&2
    return 0
  fi

  local sha fail=0
  for sha in "${pushed_shas[@]}"; do
    local pushed_tree delta docs_only=1 stat
    pushed_tree="$(git rev-parse "$sha^{tree}" 2>/dev/null)" || continue

    if [ "$pushed_tree" = "$reviewed_tree" ]; then
      echo "[pre-push] OK: pushed tree $pushed_tree matches reviewed tree (content reviewed; history shape irrelevant)."
      continue
    fi

    # Trees differ: classify the delta by path.
    stat="$(git diff --name-only "$reviewed_tree" "$pushed_tree" 2>/dev/null || true)"
    if [ -z "$stat" ]; then
      continue
    fi
    while IFS= read -r p; do
      [ -n "$p" ] || continue
      if ! commithooks_path_is_docs_only "$p"; then
        docs_only=0
        break
      fi
    done <<< "$stat"

    if [ "$docs_only" -eq 1 ]; then
      echo "[pre-push] NOTICE: pushed tree differs from reviewed tree by docs-only changes:" >&2
      echo "$stat" | sed 's/^/    /' >&2
      echo "[pre-push] NOTICE: allowed without re-review (no source-surface changes)." >&2
      continue
    fi

    fail=1
    echo "[pre-push] BLOCK: pushed tree differs from the reviewed tree by SOURCE changes." >&2
    echo "  Reviewed tree: $reviewed_tree" >&2
    echo "  Pushed tree:   $pushed_tree (from $lref @ $lsha)" >&2
    echo "  Unreviewed source delta:" >&2
    git diff --stat "$reviewed_tree" "$pushed_tree" | sed 's/^/    /' >&2
    echo "  Convene the code-review harness (docs/CODE_REVIEW_HARNESS.md) on this state," >&2
    echo "  or use the escape hatch below for a deliberate exception." >&2
  done

  if [ "$fail" -eq 1 ]; then
    echo "  ESCAPE HATCH: for a deliberate exception, create .gptqueue/review-override and push again." >&2
    if [ -f .gptqueue/review-override ]; then
      echo "===========================================================" >&2
      echo "[pre-push] REVIEW-FRESHNESS WAIVER" >&2
      echo "  .gptqueue/review-override present: push proceeds WITHOUT the gate." >&2
      echo "  This override was created deliberately by the principal." >&2
      echo "===========================================================" >&2
      return 0
    fi
    return 1
  fi
  return 0
}
