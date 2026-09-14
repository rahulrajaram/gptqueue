import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

const piDist = "/home/rahul/nodeenv2251-311/lib/node_modules/@earendil-works/pi-coding-agent/dist";

describe("owned Pi child resource-loader scope", () => {
  it.skipIf(!existsSync(join(piDist, "index.js")))("loads only the explicitly supplied owned extension under a fresh profile", async () => {
    const root = await mkdtemp(join(tmpdir(), "gptqueue-pi-isolation-"));
    const profile = join(root, "profile");
    const cwd = join(root, "cwd");
    const extension = join(root, "owned-sentinel.mjs");
    const previous = process.env.PI_CODING_AGENT_DIR;
    try {
      await mkdir(profile, { recursive: true });
      await mkdir(cwd, { recursive: true });
      await writeFile(extension, "export default function ownedSentinel() { globalThis.__gptqueueOwnedSentinel = (globalThis.__gptqueueOwnedSentinel ?? 0) + 1; return { name: 'owned-sentinel' }; }\n");
      delete (globalThis as Record<string, unknown>).__gptqueueOwnedSentinel;
      process.env.PI_CODING_AGENT_DIR = profile;
      const sdk = await import(pathToFileURL(join(piDist, "index.js")).href) as any;
      const settings = sdk.SettingsManager.inMemory({});
      const manager = new sdk.DefaultPackageManager({ cwd, agentDir: profile, settingsManager: settings });
      const resolved = await manager.resolve();
      const explicit = await manager.resolveExtensionSources([extension], { temporary: true });
      expect(resolved.extensions.filter((item: any) => item.enabled)).toHaveLength(0);
      expect(explicit.extensions.map((item: any) => resolve(item.path))).toContain(resolve(extension));
      const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: profile, settingsManager: settings,
        noExtensions: false, additionalExtensionPaths: [extension], noSkills: true, noPromptTemplates: true,
        noThemes: true, noContextFiles: true });
      await loader.reload();
      expect((globalThis as Record<string, unknown>).__gptqueueOwnedSentinel).toBe(1);
      expect(loader.getExtensions().errors).toHaveLength(0);
      expect(loader.getExtensions().extensions.map((item: any) => resolve(item.path))).toEqual([resolve(extension)]);
    } finally {
      if (previous === undefined) delete process.env.PI_CODING_AGENT_DIR;
      else process.env.PI_CODING_AGENT_DIR = previous;
      delete (globalThis as Record<string, unknown>).__gptqueueOwnedSentinel;
      await rm(root, { recursive: true, force: true });
    }
  });
});
