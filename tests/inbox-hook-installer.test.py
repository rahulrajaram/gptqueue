import json
import os
import shutil
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/install-inbox-activation.py"
NODE = Path(os.environ.get("NODE") or shutil.which("node") or "node")


class InboxHookInstallerTest(unittest.TestCase):
    def run_installer(self, *args):
        return subprocess.run(["python3", str(SCRIPT), *args], text=True, capture_output=True)

    def paths(self, root):
        codex = root / "codex"
        state = root / "state"
        codex.mkdir()
        return codex, state

    def test_plan_is_read_only_and_apply_rollback_restores_exact_bytes(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); codex, state = self.paths(root)
            hooks = codex / "hooks.json"
            original = b'{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"keep"}]}]}}\n'
            hooks.write_bytes(original); os.chmod(hooks, 0o640)
            common = ["--codex-dir", str(codex), "--state-dir", str(state), "--node-bin", str(NODE)]
            plan = self.run_installer(*common)
            self.assertEqual(plan.returncode, 0, plan.stderr)
            self.assertEqual(hooks.read_bytes(), original)
            self.assertFalse(state.exists())
            applied = self.run_installer("--apply", *common)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            configured = json.loads(hooks.read_text())
            self.assertEqual(len(configured["hooks"]["SessionStart"]), 2)
            self.assertEqual(len(configured["hooks"]["UserPromptSubmit"]), 1)
            # Binding hooks run in the background; the Stop gate must be synchronous to block.
            binding = configured["hooks"]["UserPromptSubmit"][0]["hooks"][0]
            self.assertIs(binding["async"], True)
            stop = configured["hooks"]["Stop"][0]["hooks"][0]
            self.assertNotIn("async", stop)
            self.assertIn("gptqueue-codex-hook", stop["command"])
            self.assertEqual(json.loads(plan.stdout)["hooks"], ["SessionStart", "UserPromptSubmit", "Stop"])
            self.assertTrue((state / "manifest.json").is_file())
            rolled = self.run_installer("--rollback", "--codex-dir", str(codex), "--state-dir", str(state))
            self.assertEqual(rolled.returncode, 0, rolled.stderr)
            self.assertEqual(hooks.read_bytes(), original)
            self.assertEqual(hooks.stat().st_mode & 0o777, 0o640)
            self.assertTrue((state / "rolled-back.json").is_file())

    def test_apply_upgrades_an_existing_installation_and_rollback_restores_it(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); codex, state = self.paths(root)
            hooks = codex / "hooks.json"
            gq = f"{NODE} {ROOT / 'bin/gptqueue-codex-hook'}"
            existing = {"hooks": {
                "SessionStart": [{"hooks": [{"type": "command", "command": "keep-me"}]},
                                 {"hooks": [{"type": "command", "command": gq, "async": True, "timeout": 30}]}],
                "UserPromptSubmit": [{"hooks": [{"type": "command", "command": gq, "async": True, "timeout": 30}]}],
                "Stop": [{"hooks": [{"type": "command", "command": "unrelated-stop"}]}],
            }}
            original = (json.dumps(existing) + "\n").encode()
            hooks.write_bytes(original); os.chmod(hooks, 0o600)
            # A manifest left by the earlier installation is superseded, not a reason to refuse.
            state.mkdir(mode=0o700)
            (state / "manifest.json").write_text(json.dumps({"version": 1, "phase": "applied", "files": []}))
            common = ["--codex-dir", str(codex), "--state-dir", str(state), "--node-bin", str(NODE)]

            plan = self.run_installer(*common)
            self.assertEqual(plan.returncode, 0, plan.stderr)
            planned = json.loads(plan.stdout)
            self.assertEqual(planned["new_entries"], ["Stop"])
            self.assertEqual(planned["existing_entries"], ["SessionStart", "UserPromptSubmit"])
            self.assertEqual(planned["requires_hook_trust"], ["Stop"])
            self.assertEqual(hooks.read_bytes(), original)

            applied = self.run_installer("--apply", *common)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            configured = json.loads(hooks.read_text())
            # Existing gptqueue entries and unrelated hooks are untouched; only Stop gains the gate.
            self.assertEqual(configured["hooks"]["SessionStart"], existing["hooks"]["SessionStart"])
            self.assertEqual(configured["hooks"]["UserPromptSubmit"], existing["hooks"]["UserPromptSubmit"])
            self.assertEqual(configured["hooks"]["Stop"][0], existing["hooks"]["Stop"][0])
            self.assertEqual(len(configured["hooks"]["Stop"]), 2)
            self.assertIn("gptqueue-codex-hook", configured["hooks"]["Stop"][1]["hooks"][0]["command"])
            manifest = json.loads((state / "manifest.json").read_text())
            self.assertEqual(manifest["files"][0]["original_hex"], original.hex())
            self.assertEqual(len(list(state.glob("manifest.superseded-*.json"))), 1)

            again = self.run_installer("--apply", *common)
            self.assertEqual(again.returncode, 0, again.stderr)
            self.assertEqual(json.loads(again.stdout)["new_entries"], [])

            rolled = self.run_installer("--rollback", "--codex-dir", str(codex), "--state-dir", str(state))
            self.assertEqual(rolled.returncode, 0, rolled.stderr)
            self.assertEqual(hooks.read_bytes(), original)

    def test_apply_pins_the_hook_to_redis_url_and_keeps_the_fallback_without_it(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); codex, state = self.paths(root)
            hooks = codex / "hooks.json"
            common = ["--apply", "--codex-dir", str(codex), "--state-dir", str(state), "--node-bin", str(NODE)]

            # R1: without --redis-url the hook keeps its REDIS_URL/db 0 fallback (existing installs keep working).
            applied = self.run_installer(*common)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            configured = json.loads(hooks.read_text())
            for event in ("SessionStart", "UserPromptSubmit", "Stop"):
                command = configured["hooks"][event][0]["hooks"][0]["command"]
                self.assertIn("gptqueue-codex-hook", command)
                self.assertNotIn("--redis-url", command)

            rolled = self.run_installer("--rollback", "--codex-dir", str(codex), "--state-dir", str(state))
            self.assertEqual(rolled.returncode, 0, rolled.stderr)

            # With --redis-url every written entry passes it to the hook, so the gate probes the sidecar's instance.
            url = "redis://127.0.0.1:6379/3"
            applied_url = self.run_installer("--redis-url", url, *common)
            self.assertEqual(applied_url.returncode, 0, applied_url.stderr)
            configured = json.loads(hooks.read_text())
            for event in ("SessionStart", "UserPromptSubmit", "Stop"):
                command = configured["hooks"][event][0]["hooks"][0]["command"]
                self.assertIn("gptqueue-codex-hook", command)
                self.assertIn(f"--redis-url {url}", command)

            # The pinned URL must be a valid Redis URL, like the sidecar's own --redis-url.
            bad = self.run_installer("--redis-url", "redis://127.0.0.1:6379/3?bad=1",
                                     "--codex-dir", str(codex), "--state-dir", str(state), "--node-bin", str(NODE))
            self.assertNotEqual(bad.returncode, 0)

    def test_refuses_symlink_without_following_or_modifying_target(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); codex, state = self.paths(root)
            target = root / "real-hooks.json"
            target.write_text('{"hooks":{}}')
            link = codex / "hooks.json"
            link.symlink_to(target)
            result = self.run_installer("--apply", "--codex-dir", str(codex), "--state-dir", str(state), "--node-bin", str(NODE))
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(link.resolve(), target)
            self.assertEqual(target.read_text(), '{"hooks":{}}')
            self.assertFalse(state.exists())


if __name__ == "__main__":
    unittest.main()
