import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/install-inbox-activation.py"
NODE = Path(os.environ.get("NODE", "/home/rahul/nodeenv2251-311/bin/node"))


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
            self.assertTrue((state / "manifest.json").is_file())
            rolled = self.run_installer("--rollback", "--codex-dir", str(codex), "--state-dir", str(state))
            self.assertEqual(rolled.returncode, 0, rolled.stderr)
            self.assertEqual(hooks.read_bytes(), original)
            self.assertEqual(hooks.stat().st_mode & 0o777, 0o640)
            self.assertTrue((state / "rolled-back.json").is_file())

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
