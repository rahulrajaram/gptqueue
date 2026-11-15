#!/usr/bin/env python3
"""Plan, apply, or roll back the local Codex inbox-binding and Stop-gate hooks."""
from __future__ import annotations

import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("gptqueue_registration_installer", ROOT / "scripts/install-shell-registration.py")
assert SPEC and SPEC.loader
registration = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = registration
SPEC.loader.exec_module(registration)


# Binding runs in the background: retrying avoids a SessionStart/MCP-readiness
# dependency cycle. The Stop gate must be synchronous, because an async hook
# cannot block a turn from ending; it no-ops quickly for threads that are not
# bound to gptqueue, since the shared app-server runs it for every thread.
HOOK_ENTRIES = {
    "SessionStart": {"async": True, "timeout": 35},
    "UserPromptSubmit": {"async": True, "timeout": 35},
    "Stop": {"timeout": 15},
}


def hook_config(content: bytes, node: str, redis_url: str | None = None) -> tuple[bytes, list[str], list[str]]:
    """Add each missing GPTQueue entry. Existing GPTQueue entries and unrelated hooks stay as they are.

    Returns the desired bytes and the events whose entry is new and which already had one.
    """
    config = json.loads(content or b"{}")
    if not isinstance(config, dict) or not isinstance(config.get("hooks", {}), dict):
        raise ValueError("Expected a hooks JSON object")
    hooks = config.setdefault("hooks", {})
    # The hook must probe the same Redis the sidecar uses (its --redis-url);
    # without it the hook falls back to REDIS_URL/db 0 (R1).
    command = shlex.join([node, str(ROOT / "bin/gptqueue-codex-hook")]
                         + (["--redis-url", redis_url] if redis_url else []))
    new: list[str] = []
    existing: list[str] = []
    for event, entry in HOOK_ENTRIES.items():
        groups = hooks.get(event, [])
        if not isinstance(groups, list):
            raise ValueError(f"Invalid hook groups: {event}")
        if any("gptqueue-codex-hook" in str(group) for group in groups):
            existing.append(event)
            continue
        hooks[event] = [*groups, {"hooks": [{"type": "command", "command": command, **entry}]}]
        new.append(event)
    if not new:
        return content, new, existing
    return (json.dumps(config, indent=2) + "\n").encode(), new, existing


def supersede(manifest: Path) -> Path | None:
    """Keep an earlier installation's manifest as a receipt; the new manifest records the bytes this upgrade replaces."""
    if not manifest.exists():
        return None
    index = 1
    while (archived := manifest.with_name(f"manifest.superseded-{index}.json")).exists():
        index += 1
    os.replace(manifest, archived)
    return archived


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true")
    modes.add_argument("--rollback", action="store_true")
    parser.add_argument("--codex-dir", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/gptqueue/inbox-activation")
    parser.add_argument("--node-bin", default=shutil.which("node"))
    parser.add_argument("--redis-url",
                        help="Redis URL the hook probes; pass the sidecar's --redis-url. "
                             "Omit to keep the hook's REDIS_URL/db 0 fallback")
    args = parser.parse_args()
    state = args.state_dir.absolute()
    manifest = state / "manifest.json"
    if args.rollback:
        with (state / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            registration.rollback(manifest)
        print(json.dumps({"mode": "rollback"}))
        return
    if not args.node_bin or not Path(args.node_bin).is_absolute() or not os.access(args.node_bin, os.X_OK):
        raise ValueError("--node-bin must name an absolute Node executable")
    if not (ROOT / "dist/registered-shell/codex-hook.js").is_file():
        raise ValueError("Build GPTQueue before installing inbox activation")
    if args.redis_url:
        registration.validate_redis_url(args.redis_url)
    target = args.codex_dir.absolute() / "hooks.json"
    original = registration.snapshot(target)
    desired, new, existing = hook_config(original.content, args.node_bin, args.redis_url)
    print(json.dumps({"mode": "apply" if args.apply else "plan", "target": str(target),
                      "manifest": str(manifest), "sha256": registration.digest(desired),
                      "hooks": list(HOOK_ENTRIES),
                      "new_entries": new,
                      "existing_entries": existing,
                      "requires_hook_trust": new,
                      "next_step": (f"Review and trust the new GPTQueue hook(s) {', '.join(new)} in Codex /hooks, then resume the session."
                                    if new else "Nothing to add; the GPTQueue hooks are already installed."),
                      "source": str(ROOT / "bin/gptqueue-codex-hook")}, indent=2))
    if args.apply and new:
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (state / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            # Upgrade: rollback restores the bytes this apply replaces (recorded in the new manifest).
            archived = supersede(manifest)
            try:
                registration.apply_changes(manifest, {target: desired}, {target: original})
            except BaseException:
                if archived is not None and not manifest.exists():
                    os.replace(archived, manifest)
                raise


if __name__ == "__main__":
    main()
