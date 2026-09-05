#!/usr/bin/env python3
"""Plan, apply, or roll back the local Codex inbox-binding hook."""
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


def hook_config(content: bytes, node: str) -> bytes:
    config = json.loads(content or b"{}")
    if not isinstance(config, dict) or not isinstance(config.get("hooks", {}), dict):
        raise ValueError("Expected a hooks JSON object")
    hooks = config.setdefault("hooks", {})
    command = shlex.join([node, str(ROOT / "bin/gptqueue-codex-hook")])
    for event in ("SessionStart", "UserPromptSubmit"):
        groups = hooks.setdefault(event, [])
        if not isinstance(groups, list):
            raise ValueError(f"Invalid hook groups: {event}")
        if any("gptqueue-codex-hook" in str(group) for group in groups):
            raise ValueError("An inbox-binding hook is already configured; use its existing installation or roll it back first")
        # Background retry avoids a SessionStart/MCP-readiness dependency cycle.
        groups.append({"hooks": [{"type": "command", "command": command,
                                  "async": True, "timeout": 35}]})
    return (json.dumps(config, indent=2) + "\n").encode()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true")
    modes.add_argument("--rollback", action="store_true")
    parser.add_argument("--codex-dir", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/gptqueue/inbox-activation")
    parser.add_argument("--node-bin", default=shutil.which("node"))
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
    target = args.codex_dir.absolute() / "hooks.json"
    original = registration.snapshot(target)
    desired = hook_config(original.content, args.node_bin)
    print(json.dumps({"mode": "apply" if args.apply else "plan", "target": str(target),
                      "manifest": str(manifest), "sha256": registration.digest(desired),
                      "hooks": ["SessionStart", "UserPromptSubmit"],
                      "requires_hook_trust": True,
                      "next_step": "Review and trust both GPTQueue hooks in Codex /hooks, then resume the session.",
                      "source": str(ROOT / "bin/gptqueue-codex-hook")}, indent=2))
    if args.apply:
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (state / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            registration.apply_changes(manifest, {target: desired}, {target: original})


if __name__ == "__main__":
    main()
