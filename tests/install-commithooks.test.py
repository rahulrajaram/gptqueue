import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/install-commithooks"
SOURCE = Path(os.environ.get("COMMITHOOKS_DIR", "/workspace/commithooks"))


@unittest.skipUnless((SOURCE / "lib" / "common.sh").is_file(), f"commithooks source absent at {SOURCE}")
class InstallCommithooksTest(unittest.TestCase):
    """Runs the installer only inside throwaway repositories, never this checkout."""

    def env(self, home):
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(HOME=str(home), GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", COMMITHOOKS_DIR=str(SOURCE))
        return env

    def git(self, cwd, env, *args):
        return subprocess.run(["git", *args], cwd=cwd, env=env, text=True, capture_output=True, check=True).stdout.strip()

    def git_path(self, cwd, env, *args):
        return (Path(cwd) / self.git(cwd, env, "rev-parse", *args)).resolve()

    def make_repo(self, root, env):
        main = root / "main"
        main.mkdir()
        self.git(main, env, "init", "-q")
        self.git(main, env, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init")
        return main

    def install(self, cwd, env):
        result = subprocess.run(["python3", str(SCRIPT)], cwd=cwd, env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Commithooks installed", result.stdout)

    def assert_installed(self, cwd, env):
        hooks = self.git_path(cwd, env, "--git-path", "hooks")
        lib = self.git_path(cwd, env, "--git-common-dir") / "lib"
        self.assertEqual((hooks / "pre-commit").read_bytes(), (SOURCE / "pre-commit").read_bytes())
        self.assertTrue(os.access(hooks / "pre-commit", os.X_OK))
        # The repo's .githooks stubs source "$(git rev-parse --git-common-dir)/lib".
        self.assertTrue((lib / "common.sh").is_file())

    def commit(self, cwd, env, message):
        return subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", message],
            cwd=cwd, env=env, text=True, capture_output=True,
        )

    def track_stubs(self, main, env):
        """Track this repository's own .githooks stubs, as a gptqueue checkout does."""
        (main / ".githooks").mkdir()
        for stub in ("pre-commit", "commit-msg"):
            shutil.copy2(ROOT / ".githooks" / stub, main / ".githooks" / stub)
        self.git(main, env, "add", ".githooks")
        self.git(main, env, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "chore: track hook stubs")

    def assert_hooks_run(self, checkout, env):
        (checkout / "note.txt").write_text(checkout.name)
        self.git(checkout, env, "add", "note.txt")
        # A rejected non-conventional message proves the stub ran with its library.
        rejected = self.commit(checkout, env, "not conventional")
        self.assertNotEqual(rejected.returncode, 0, f"{checkout.name}: commit-msg stub did not run")
        accepted = self.commit(checkout, env, f"chore: commit from {checkout.name}")
        self.assertEqual(accepted.returncode, 0, f"{checkout.name}: {accepted.stdout}{accepted.stderr}")

    def test_main_checkout_installs_into_effective_hooks_dir(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); env = self.env(root)
            main = self.make_repo(root, env)
            self.install(main, env)
            self.assert_installed(main, env)

    def test_linked_worktree_installs_where_git_runs_hooks(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); env = self.env(root)
            main = self.make_repo(root, env)
            linked = root / "linked"
            self.git(main, env, "worktree", "add", "-q", str(linked))
            self.install(linked, env)
            self.assert_installed(linked, env)
            per_worktree = self.git_path(linked, env, "--git-dir")
            self.assertFalse((per_worktree / "hooks" / "pre-commit").exists(), "dispatcher written where git never looks")

    def test_symlinked_hooks_dir_is_preserved(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); env = self.env(root)
            main = self.make_repo(root, env)
            managed = root / "operator-hooks"
            managed.mkdir()
            hooks = main / ".git" / "hooks"
            shutil.rmtree(hooks)
            hooks.symlink_to(managed)
            result = subprocess.run(["python3", str(SCRIPT)], cwd=main, env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Preserving custom hooks directory", result.stdout)
            self.assertEqual(list(managed.iterdir()), [], "operator-managed hooks directory was populated")
            self.assertFalse((main / ".git" / "lib").exists())

    def test_linked_worktree_install_serves_main_and_sibling_checkouts(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); env = self.env(root)
            main = self.make_repo(root, env)
            self.track_stubs(main, env)
            first, second = root / "first", root / "second"
            for linked in (first, second):
                self.git(main, env, "worktree", "add", "-q", str(linked))
            self.install(first, env)
            for checkout in (main, second, first):
                self.assert_hooks_run(checkout, env)

    def test_stubs_fall_back_to_a_per_worktree_library(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); env = self.env(root)
            main = self.make_repo(root, env)
            self.track_stubs(main, env)
            linked = root / "linked"
            self.git(main, env, "worktree", "add", "-q", str(linked))
            self.install(linked, env)
            # An older installer left the library in the linked worktree's own git dir.
            common = self.git_path(linked, env, "--git-common-dir")
            (common / "lib").rename(self.git_path(linked, env, "--git-dir") / "lib")
            self.assert_hooks_run(linked, env)


if __name__ == "__main__":
    unittest.main()
