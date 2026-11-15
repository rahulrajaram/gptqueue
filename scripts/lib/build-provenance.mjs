import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstatSync, readFileSync, readdirSync, readlinkSync, writeFileSync } from "node:fs";
import { join, relative, resolve, sep } from "node:path";

const hash = (content) => createHash("sha256").update(content).digest("hex");

/** Hash every file and symlink below src using sorted relative paths. */
export function digestSourceTree(srcDir) {
  const root = resolve(srcDir);
  const files = [];
  const visit = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile() || entry.isSymbolicLink()) {
        const rel = relative(root, path).split(sep).join("/");
        const st = lstatSync(path);
        const digest = st.isSymbolicLink() ? `link:${readlinkSync(path)}` : hash(readFileSync(path));
        files.push([rel, digest]);
      }
    }
  };
  visit(root);
  files.sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  return hash(files.map(([path, digest]) => `${path}\0${digest}\n`).join(""));
}

export function writeBuildManifest(rootDir = process.cwd()) {
  const root = resolve(rootDir);
  // A source tarball without .git still builds; install-runtime requires git
  // and refuses a null gitHead, so provenance stays strict where it matters.
  let gitHead = null;
  try { gitHead = execFileSync("git", ["-C", root, "rev-parse", "HEAD"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim(); }
  catch { gitHead = null; }
  const sourceDigest = digestSourceTree(join(root, "src"));
  const manifestPath = join(root, "dist", "build-manifest.json");
  writeFileSync(manifestPath, `${JSON.stringify({ gitHead, sourceDigest }, null, 2)}\n`);
  return Object.freeze({ gitHead, sourceDigest });
}
