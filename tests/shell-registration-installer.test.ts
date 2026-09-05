import { describe, expect, it } from "vitest";
import { mkdtemp, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const script = join(process.cwd(), "scripts/install-shell-registration.py");
const run = (args: string[]) => spawnSync("python3", [script, ...args], { encoding: "utf8" });
const dirs = async () => { const root = await mkdtemp(join(tmpdir(), "gptq-installer-")); const ext = join(root, "pi-extension.js"); await (await import("node:fs/promises")).writeFile(ext, "export default async () => {}\n"); return { root, codex: join(root, "codex"), pi: join(root, "pi"), state: join(root, "state"), ext }; };

describe("shell registration installer", () => {
  it("plans without writing", async () => {
    const d = await dirs(); const r = run(["--redis-url", "redis://127.0.0.1:6379/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]);
    expect(r.status).toBe(0); expect(r.stdout).toContain('"mode": "plan"'); await expect(stat(d.state)).rejects.toThrow();
  });
  it("applies and rolls back exact temporary files", async () => {
    const d = await dirs(); const args = ["--redis-url", "redis://127.0.0.1:6379/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state];
    const c = join(d.codex, "config.toml"); const m = join(d.pi, "mcp.json");
    await (await import("node:fs/promises")).mkdir(d.codex, { recursive: true }); await (await import("node:fs/promises")).mkdir(d.pi, { recursive: true });
    await (await import("node:fs/promises")).writeFile(c, '[model]\nname = "keep"\n[mcp_servers.other]\nurl = "http://other"\n'); await (await import("node:fs/promises")).writeFile(m, '{"mcpServers":{"other":{"url":"http://other"},"gptqueue-shared":{"url":"old"}}}\n');
    const beforeC = await readFile(c); const beforeM = await readFile(m); const beforeCM = (await stat(c)).mode & 0o777; const beforeMM = (await stat(m)).mode & 0o777; expect(run(["--apply", ...args]).status).toBe(0); expect((await readFile(c, "utf8"))).toContain('mcp_servers.other'); expect(run(["--rollback", "--state-dir", d.state, "--codex-dir", d.codex, "--pi-dir", d.pi]).status).toBe(0); expect(await readFile(c)).toEqual(beforeC); expect(await readFile(m)).toEqual(beforeM); expect((await stat(c)).mode & 0o777).toBe(beforeCM); expect((await stat(m)).mode & 0o777).toBe(beforeMM); await expect(stat(join(d.pi, "extensions/gptqueue-registration.ts"))).rejects.toThrow(); expect(await stat(join(d.state, "rolled-back.json"))).toBeTruthy();
  });
  it("refuses malformed TOML and concurrent edits", async () => {
    const d = await dirs(); const c = join(d.codex, "config.toml"); await (await import("node:fs/promises")).mkdir(d.codex, { recursive: true }); await (await import("node:fs/promises")).writeFile(c, "["); expect(run(["--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]).status).not.toBe(0);
  });
  it("creates a complete Codex entry when config is absent", async () => {
    const d = await dirs(); const args = ["--apply", "--redis-url", "redis://x/0", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state];
    expect(run(args).status).toBe(0); const text = await readFile(join(d.codex, "config.toml"), "utf8"); expect(text).toContain("command ="); expect(text).toContain("required = true"); expect(text).toContain("startup_timeout_sec = 15"); expect(text).toContain("--redis-url");
  });
  it("refuses a foreign shim and preserves files", async () => {
    const d = await dirs(); const fs = await import("node:fs/promises"); await fs.mkdir(join(d.pi, "extensions"), { recursive: true }); const shim = join(d.pi, "extensions/gptqueue-registration.ts"); await fs.writeFile(shim, "foreign"); const before = await readFile(shim); const r = run(["--apply", "--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]); expect(r.status).not.toBe(0); expect(await readFile(shim)).toEqual(before);
  });
  it("refuses applying over an existing manifest", async () => {
    const d = await dirs(); const args = ["--apply", "--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]; expect(run(args).status).toBe(0); expect(run(args).status).not.toBe(0);
  });
  it("refuses rollback after a target edit", async () => {
    const d = await dirs(); const fs = await import("node:fs/promises"); const args = ["--apply", "--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]; expect(run(args).status).toBe(0); const c = join(d.codex, "config.toml"); await fs.appendFile(c, "# changed\n"); const p = join(d.pi, "mcp.json"); const keep = await readFile(p); expect(run(["--rollback", "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]).status).not.toBe(0); expect(await readFile(p)).toEqual(keep);
  });
  it("preserves unrelated MCP tables and restores modes", async () => {
    const d = await dirs(); const fs = await import("node:fs/promises"); await fs.mkdir(d.codex, { recursive: true }); const c = join(d.codex, "config.toml"); await fs.writeFile(c, '[mcp_servers.other]\nurl = "http://other"\n[mcp_servers.gptqueue-shared]\nurl = "old"\n[mcp_servers.other.env]\nTOKEN = "opaque"\n'); await fs.chmod(c, 0o640); const args = ["--apply", "--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]; expect(run(args).status).toBe(0); expect(run(["--rollback", "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]).status).toBe(0); expect((await stat(c)).mode & 0o777).toBe(0o640); expect(await readFile(c, "utf8")).toContain('TOKEN = "opaque"');
  });
  it("rejects malformed Pi JSON and invalid Redis URL without changing files", async () => {
    const d = await dirs(); const fs = await import("node:fs/promises"); await fs.mkdir(d.pi, { recursive: true }); const p = join(d.pi, "mcp.json"); await fs.writeFile(p, "[]"); const before = await readFile(p); const base = ["--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]; expect(run(["--redis-url", "redis://x/15?bad=1", ...base]).status).not.toBe(0); expect(run(["--redis-url", "redis://x/15", ...base]).status).not.toBe(0); expect(await readFile(p)).toEqual(before);
  });
  it("refuses symlink targets without changing the link", async () => {
    const d = await dirs(); const fs = await import("node:fs/promises"); await fs.mkdir(d.codex, { recursive: true }); const target = join(d.root, "target.toml"); await fs.writeFile(target, "[mcp_servers.other]\nurl=\"x\"\n"); const link = join(d.codex, "config.toml"); await fs.symlink(target, link); const base = ["--redis-url", "redis://x/15", "--node-bin", process.execPath, "--pi-extension", d.ext, "--codex-dir", d.codex, "--pi-dir", d.pi, "--state-dir", d.state]; expect(run(base).status).not.toBe(0); expect(await fs.readlink(link)).toBe(target);
  });
});
