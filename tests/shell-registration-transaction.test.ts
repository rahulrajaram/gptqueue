import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, writeFile, chmod, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const script = join(process.cwd(), "scripts/install-shell-registration.py");
const run = (code: string) => spawnSync("python3", ["-c", code], { encoding: "utf8" });
const roots: string[] = [];
afterEach(async () => { await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true }))); });

describe("installer transaction primitives", () => {
  it("writes the prepared manifest first and restores every file after failure", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptq-tx-"));
    roots.push(root);
    const paths = [0, 1, 2].map(i => join(root, `file-${i}`));
    for (const [i, p] of paths.entries()) { await writeFile(p, `original-${i}`); await chmod(p, 0o640 + i); }
    const manifest = join(root, "manifest.json");
    const code = `import importlib.util,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location("x", ${JSON.stringify(script)})
m=importlib.util.module_from_spec(spec); sys.modules[spec.name]=m; spec.loader.exec_module(m)
paths=[Path(x) for x in ${JSON.stringify(paths)}]; originals={p:m.snapshot(p) for p in paths}; changes={p:f"changed-{i}".encode() for i,p in enumerate(paths)}; mf=Path(${JSON.stringify(manifest)})
real=m.atomic_write; calls=[]
def fail(path, content, mode=0o600):
 if path == mf: return real(path, content, mode)
 calls.append(mf.exists())
 if len(calls)==2: raise OSError("injected")
 return real(path, content, mode)
m.atomic_write=fail
try: m.apply_changes(mf, changes, originals)
except OSError: pass
else: raise AssertionError("failure not injected")
assert calls[0] is True and not mf.exists()
assert all(m.snapshot(p)==originals[p] for p in paths)
`;
    const result = run(code); expect(result.status, result.stderr).toBe(0);
  });

  it("rolls back a prepared manifest with partially applied files", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptq-tx-")); roots.push(root); const paths = [0, 1, 2].map(i => join(root, `file-${i}`));
    for (const [i, p] of paths.entries()) { await writeFile(p, `original-${i}`); await chmod(p, 0o640 + i); }
    const manifest = join(root, "manifest.json");
    const code = `import importlib.util,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location("x", ${JSON.stringify(script)}); m=importlib.util.module_from_spec(spec); sys.modules[spec.name]=m; spec.loader.exec_module(m)
paths=[Path(x) for x in ${JSON.stringify(paths)}]; old={p:m.snapshot(p) for p in paths}; new={p:f"installed-{i}".encode() for i,p in enumerate(paths)}
records=[{"path":str(p),"original_exists":old[p].exists,"original_hex":old[p].content.hex(),"original_mode":old[p].mode,"installed_sha256":m.digest(new[p])} for p in paths]
mf=Path(${JSON.stringify(manifest)}); m.save_manifest(mf,{"version":1,"phase":"prepared","files":records}); m.atomic_write(paths[0],new[paths[0]]); m.rollback(mf)
assert all(m.snapshot(p)==old[p] for p in paths)
`;
    const result = run(code); expect(result.status, result.stderr).toBe(0);
  });
});
