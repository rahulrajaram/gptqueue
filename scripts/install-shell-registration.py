#!/usr/bin/env python3
"""Plan, apply, or roll back the local Codex/Pi GPTQueue connection settings."""
from __future__ import annotations

import argparse
import copy
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from urllib.parse import urlsplit

import tomllib

ROOT = Path(__file__).resolve().parents[1]
SIDECAR = ROOT / "bin/gptqueue-session"
PI_EXTENSION = ROOT / "dist/registered-shell/pi-extension.js"
SHIM_NAME = "gptqueue-registration.ts"


@dataclass(frozen=True)
class Snapshot:
    exists: bool
    content: bytes
    mode: int


def snapshot(path: Path) -> Snapshot:
    if path.is_symlink():
        raise ValueError(f"Refusing symlink target: {path}")
    return (Snapshot(True, path.read_bytes(), stat.S_IMODE(path.stat().st_mode))
            if path.exists() else Snapshot(False, b"", 0o600))


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def atomic_write(path: Path, content: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def restore(path: Path, original: Snapshot) -> None:
    if original.exists:
        atomic_write(path, original.content, original.mode)
    else:
        path.unlink(missing_ok=True)


def without_server(config: dict, name: str) -> dict:
    result = copy.deepcopy(config)
    servers = result.get("mcp_servers", {})
    servers.pop(name, None)
    if not servers:
        result.pop("mcp_servers", None)
    return result


def table_path(header: str) -> tuple[str, ...]:
    """Parse TOML header spelling, including quoted keys, with the stdlib parser."""
    value = tomllib.loads(header + "\n__gptqueue_header_probe__ = true\n")
    keys = ()
    while isinstance(value, dict) and "__gptqueue_header_probe__" not in value:
        key, value = next(iter(value.items()))
        keys += (key,)
        if isinstance(value, list):
            value = value[0]
    return keys


def codex_config(content: bytes, name: str, node: str, redis_url: str) -> bytes:
    source = content.decode()
    before = tomllib.loads(source)
    headers = list(re.finditer(r"(?m)^\s*(\[\[?[^\n]+?\]\]?)\s*(?:#[^\n]*)?$", source))
    # Remove only complete sections belonging to this exact MCP server.
    ranges = [(header.start(), headers[index + 1].start() if index + 1 < len(headers) else len(source))
              for index, header in enumerate(headers)
              if table_path(header.group(1))[:2] == ("mcp_servers", name)]
    for start, end in reversed(ranges):
        source = source[:start] + source[end:]
    block = (f"[mcp_servers.{json.dumps(name)}]\n"
             f"command = {json.dumps(node)}\n"
             f"args = {json.dumps([str(SIDECAR), '--client', 'codex', '--redis-url', redis_url])}\n"
             "required = true\nstartup_timeout_sec = 15\ntool_timeout_sec = 70\n")
    result = (source.rstrip() + "\n\n" + block).encode()
    if without_server(before, name) != without_server(tomllib.loads(result.decode()), name):
        raise ValueError("Unrelated Codex settings changed; unsupported TOML layout")
    return result


def pi_config(content: bytes, names: tuple[str, ...]) -> bytes:
    config = json.loads(content or b'{"mcpServers": {}}')
    if not isinstance(config, dict) or not isinstance(config.get("mcpServers", {}), dict):
        raise ValueError("Malformed Pi MCP configuration")
    servers = config.get("mcpServers", {})
    result = {**config, "mcpServers": {key: value for key, value in servers.items() if key not in names}}
    return (json.dumps(result, indent=2, ensure_ascii=False) + "\n").encode()


def pi_shim(extension: Path, node: str, redis_url: str) -> bytes:
    options = {"redisUrl": redis_url, "nodePath": node, "sidecarPath": str(SIDECAR)}
    return ("// Managed by GPTQueue's reversible shell registration installer.\n"
            "export default async function (pi) {\n  try {\n"
            f"    const {{ createRegisteredPiExtension }} = await import({json.dumps(extension.as_uri())});\n"
            f"    await createRegisteredPiExtension({json.dumps(options)})(pi);\n"
            "  } catch (error) {\n    console.error('[gptqueue] Required extension failed:', error);\n"
            "    process.exit(1);\n  }\n}\n").encode()


def save_manifest(path: Path, value: dict) -> None:
    atomic_write(path, (json.dumps(value, indent=2) + "\n").encode())


def apply_changes(manifest_path: Path, changes: dict[Path, bytes], originals: dict[Path, Snapshot]) -> None:
    if manifest_path.exists():
        raise ValueError("Existing manifest; rollback first")
    if any(snapshot(path) != original for path, original in originals.items()):
        raise ValueError("Concurrent edit detected; no changes applied")
    records = [{"path": str(path), "original_exists": originals[path].exists,
                "original_hex": originals[path].content.hex(), "original_mode": originals[path].mode,
                "installed_sha256": digest(content)} for path, content in changes.items()]
    manifest = {"version": 1, "phase": "prepared", "files": records}
    # Durable originals precede the first settings write, including crash recovery.
    save_manifest(manifest_path, manifest)
    applied = []
    try:
        for path, content in changes.items():
            if snapshot(path) != originals[path]:
                raise ValueError(f"Concurrent edit: {path}")
            atomic_write(path, content)
            applied.append(path)
        save_manifest(manifest_path, {**manifest, "phase": "applied"})
    except BaseException:
        for path in reversed(applied):
            if snapshot(path).content != changes[path]:
                raise ValueError(f"Concurrent edit during recovery: {path}; backup retained")
            restore(path, originals[path])
        # Keep the prepared backup if restoration itself fails.
        manifest_path.unlink()
        raise


def rollback(manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_bytes())
    originals = {Path(item["path"]): Snapshot(item["original_exists"], bytes.fromhex(item["original_hex"]),
                                           item["original_mode"]) for item in manifest["files"]}
    for item in manifest["files"]:
        path = Path(item["path"])
        current = snapshot(path)
        installed = current.exists and current.mode == 0o600 and digest(current.content) == item["installed_sha256"]
        interrupted_original = manifest["phase"] == "prepared" and current == originals[path]
        if not installed and not interrupted_original:
            raise ValueError(f"Rollback refused; target changed: {path}")
    for path, original in originals.items():
        restore(path, original)
    # Keep the backup as a receipt while permitting a subsequent installation.
    os.replace(manifest_path, manifest_path.with_name("rolled-back.json"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    parser.add_argument("--redis-url")
    parser.add_argument("--server-name", default="gptqueue-shared")
    parser.add_argument("--pi-server-name")
    parser.add_argument("--codex-dir", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--pi-dir", type=Path, default=Path.home() / ".pi/agent")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/gptqueue/shell-registration")
    parser.add_argument("--node-bin", default=shutil.which("node"))
    parser.add_argument("--pi-extension", type=Path, default=PI_EXTENSION)
    args = parser.parse_args()
    manifest_path = args.state_dir.absolute() / "manifest.json"
    if args.rollback:
        with (manifest_path.parent / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            rollback(manifest_path)
        print(json.dumps({"mode": "rollback"}))
        return
    if not args.redis_url:
        raise ValueError("--redis-url is required for plan/apply")
    try:
        url = urlsplit(args.redis_url)
        if (url.scheme not in ("redis", "rediss") or not url.hostname or url.query or url.fragment
                or not re.fullmatch(r"/(?:[0-9]+)?", url.path or "/")):
            raise ValueError()
        _ = url.port
    except ValueError:
        raise ValueError("Invalid Redis URL") from None
    if not args.node_bin or not Path(args.node_bin).is_absolute() or not os.access(args.node_bin, os.X_OK):
        raise ValueError("--node-bin must be an absolute executable")
    if not SIDECAR.is_file() or not (ROOT / "dist/registered-shell/server.js").is_file() or not args.pi_extension.is_file():
        raise ValueError("Missing built shell integration; run npm run build")
    codex = args.codex_dir.absolute() / "config.toml"
    pi = args.pi_dir.absolute() / "mcp.json"
    shim = args.pi_dir.absolute() / "extensions" / SHIM_NAME
    originals = {path: snapshot(path) for path in (codex, pi, shim)}
    if originals[shim].exists and not manifest_path.exists():
        raise ValueError(f"Unowned extension exists: {shim}")
    changes = {
        codex: codex_config(originals[codex].content, args.server_name, args.node_bin, args.redis_url),
        pi: pi_config(originals[pi].content, (args.server_name, args.pi_server_name or args.server_name)),
        shim: pi_shim(args.pi_extension.resolve(), args.node_bin, args.redis_url),
    }
    plan = {"mode": "apply" if args.apply else "plan", "targets": [
        {"path": str(path), "before_sha256": digest(originals[path].content), "after_sha256": digest(content)}
        for path, content in changes.items()]}
    if args.apply:
        manifest_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(manifest_path.parent, 0o700)
        with (manifest_path.parent / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            apply_changes(manifest_path, changes, originals)
    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        raise SystemExit(str(error)) from None
